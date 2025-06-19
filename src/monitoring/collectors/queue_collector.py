"""
RabbitMQ queue monitoring collector.

Monitors queue depths, connection status, and message flow rates.
"""

import time
from datetime import datetime
from typing import Dict, Any

# Optional imports with fallbacks
try:
    import aio_pika  # type: ignore
    AIO_PIKA_AVAILABLE = True
except ImportError:
    AIO_PIKA_AVAILABLE = False
    
try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

from loguru import logger

from pdf_parser.settings import get_settings
from ..prometheus_metrics import (
    rabbitmq_connections, rabbitmq_queue_messages
)
from .base_collector import BaseCollector, CollectorMetrics


class QueueCollector(BaseCollector):
    """Collector for RabbitMQ queue metrics."""
    
    def __init__(self, collection_interval: float = 30.0):
        super().__init__(collection_interval)
        self.settings = get_settings()
        self._connection = None
        self._channel = None
    
    def get_component_name(self) -> str:
        return "rabbitmq_queues"
    
    async def _get_connection(self):
        """Get or create RabbitMQ connection."""
        if self._connection is None or self._connection.is_closed:
            try:
                self._connection = await aio_pika.connect_robust(
                    self.settings.CELERY_BROKER_URL,
                    timeout=15  # Reduced timeout to prevent slow collection
                )
                self._channel = await self._connection.channel()
                logger.info("Established RabbitMQ connection for monitoring")
            except Exception as e:
                logger.error(f"Failed to connect to RabbitMQ: {e}")
                raise
        
        return self._connection, self._channel
    
    async def _get_queue_info(self, queue_name: str) -> Dict[str, Any]:
        """Get information about a specific queue - non-blocking approach."""
        try:
            _, channel = await self._get_connection()
            
            # Try to get queue info without declaring (passive=False creates if needed)
            # For monitoring, we'll create the queue if it doesn't exist
            queue = await channel.declare_queue(queue_name, durable=True, passive=False)
            
            # Get queue declaration result which contains the metrics
            message_count = queue.declaration_result.message_count
            consumer_count = queue.declaration_result.consumer_count
            
            return {
                "message_count": message_count,
                "consumer_count": consumer_count,
                "exists": True,
                "queue_name": queue_name
            }
            
        except Exception as e:
            # Only log queue errors, not routine "not found" cases
            if "NOT_FOUND" not in str(e) and "Channel closed" not in str(e):
                logger.warning(f"Queue {queue_name} monitoring error: {e}")
            return {
                "message_count": 0,
                "consumer_count": 0,
                "exists": False,
                "queue_name": queue_name,
                "error": str(e)
            }
    
    async def _get_management_api_metrics(self) -> Dict[str, Any]:
        """Get metrics from RabbitMQ Management API if available."""
        try:
            if not AIOHTTP_AVAILABLE:
                return {"management_api_available": False, "reason": "aiohttp not available"}
            
            # Extract host and port from broker URL
            broker_url = self.settings.CELERY_BROKER_URL
            if "amqp://" in broker_url:
                # Parse amqp://user:pass@host:port/vhost
                parts = broker_url.replace("amqp://", "").split("@")
                if len(parts) == 2:
                    host_part = parts[1].split("/")[0]
                    if ":" in host_part:
                        host, port = host_part.split(":")
                    else:
                        host, port = host_part, "5672"
                    
                    # Try management API on default port with timeout
                    management_url = f"http://{host}:15672/api/overview"
                    
                    timeout = aiohttp.ClientTimeout(total=3)  # Short timeout
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        async with session.get(management_url) as response:
                            if response.status == 200:
                                data = await response.json()
                                return {
                                    "management_api_available": True,
                                    "total_messages": data.get("queue_totals", {}).get("messages", 0),
                                    "total_connections": data.get("object_totals", {}).get("connections", 0),
                                    "erlang_version": data.get("erlang_version", "unknown"),
                                    "rabbitmq_version": data.get("rabbitmq_version", "unknown")
                                }
            
        except Exception as e:
            # Only log on first failure, then suppress routine failures
            pass  # Management API is optional, don't spam logs
        
        return {"management_api_available": False}
    
    async def collect_metrics(self) -> CollectorMetrics:
        """Collect RabbitMQ queue metrics."""
        start_time = time.time()
        
        try:
            # Define core queues to monitor (only create these)
            core_queues = ["gpu", "cpu"]  # Simplified list - main processing queues
            
            queue_metrics = {}
            total_messages = 0
            total_consumers = 0
            
            # Collect queue information for core queues only
            for queue_name in core_queues:
                queue_info = await self._get_queue_info(queue_name)
                queue_metrics[queue_name] = queue_info
                
                if queue_info["exists"]:
                    # Update Prometheus metrics
                    rabbitmq_queue_messages.labels(
                        queue=queue_name, 
                        state="ready"
                    ).set(queue_info["message_count"])
                    
                    total_messages += queue_info["message_count"]
                    total_consumers += queue_info["consumer_count"]
            
            # Get management API metrics (with timeout)
            management_metrics = await self._get_management_api_metrics()
            
            # Update connection metrics
            connection_healthy = self._connection and not self._connection.is_closed
            rabbitmq_connections.labels(type="monitoring").set(1 if connection_healthy else 0)
            
            collection_time = (time.time() - start_time) * 1000
            
            metrics = {
                "queues": queue_metrics,
                "summary": {
                    "total_messages": total_messages,
                    "total_consumers": total_consumers,
                    "connection_status": "connected" if connection_healthy else "disconnected",
                    "monitored_queues": len(core_queues),
                    "existing_queues": len([q for q in queue_metrics.values() if q["exists"]])
                },
                **management_metrics
            }
            
            # Determine health status
            healthy = connection_healthy
            
            errors = None
            if not healthy:
                errors = {
                    "connection": "No active RabbitMQ connection"
                }
            
            # Add warnings for queues with no consumers (but don't mark as unhealthy)
            warnings = {}
            for queue_name, queue_info in queue_metrics.items():
                if queue_info["exists"] and queue_info["consumer_count"] == 0 and queue_info["message_count"] > 0:
                    warnings[f"{queue_name}_no_consumers"] = f"Queue {queue_name} has {queue_info['message_count']} messages but no consumers"
            
            if warnings:
                if errors is None:
                    errors = {}
                errors.update(warnings)
            
            return CollectorMetrics(
                component=self.get_component_name(),
                healthy=healthy,
                last_collection=datetime.now(),
                collection_duration_ms=round(collection_time, 2),
                metrics=metrics,
                errors=errors
            )
            
        except Exception as e:
            collection_time = (time.time() - start_time) * 1000
            logger.error(f"Error collecting queue metrics: {e}")
            
            return CollectorMetrics(
                component=self.get_component_name(),
                healthy=False,
                last_collection=datetime.now(),
                collection_duration_ms=round(collection_time, 2),
                metrics={
                    "queues": {},
                    "summary": {
                        "total_messages": 0,
                        "total_consumers": 0,
                        "connection_status": "error",
                        "monitored_queues": 0,
                        "existing_queues": 0
                    }
                },
                errors={"collection_error": str(e)}
            )
    
    async def stop_collection(self):
        """Stop collection and cleanup connections."""
        await super().stop_collection()
        
        # Cleanup connections
        try:
            if self._channel and not self._channel.is_closed:
                await self._channel.close()
            
            if self._connection and not self._connection.is_closed:
                await self._connection.close()
            
            # Only log connection cleanup on info level, not debug
            logger.info("RabbitMQ monitoring connections closed")
        except Exception as e:
            logger.warning(f"Error closing RabbitMQ connections: {e}") 
