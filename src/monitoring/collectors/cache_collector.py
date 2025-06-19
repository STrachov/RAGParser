"""
Redis cache monitoring collector.

Monitors Redis performance, memory usage, and connection health.
"""

import time
from datetime import datetime
from typing import Dict, Any

# Use existing redis module
import redis.asyncio as aioredis
from loguru import logger

from pdf_parser.settings import get_settings
from ..prometheus_metrics import (
    redis_connection_pool_size, redis_memory_usage, record_redis_operation
)
from .base_collector import BaseCollector, CollectorMetrics


class CacheCollector(BaseCollector):
    """Collector for Redis cache metrics."""
    
    def __init__(self, collection_interval: float = 30.0):
        super().__init__(collection_interval)
        self.settings = get_settings()
        self._redis_client = None
    
    def get_component_name(self) -> str:
        return "redis_cache"
    
    async def _get_redis_client(self) -> aioredis.Redis:
        """Get or create Redis client."""
        if self._redis_client is None:
            self._redis_client = aioredis.from_url(
                self.settings.REDIS_URL,
                encoding="utf-8",
                decode_responses=True
            )
        return self._redis_client
    
    async def collect_metrics(self) -> CollectorMetrics:
        """Collect Redis cache metrics."""
        start_time = time.time()
        
        try:
            redis_client = await self._get_redis_client()
            
            # Test basic connectivity
            ping_start = time.time()
            await redis_client.ping()
            ping_time = (time.time() - ping_start) * 1000
            
            # Get Redis info
            info = await redis_client.info()
            memory_info = await redis_client.info("memory")
            stats_info = await redis_client.info("stats")
            replication_info = await redis_client.info("replication")
            
            # Test performance with actual operations
            perf_start = time.time()
            test_key = f"health_check_{int(time.time())}"
            await redis_client.set(test_key, "test", ex=60)
            test_value = await redis_client.get(test_key)
            await redis_client.delete(test_key)
            perf_time = (time.time() - perf_start) * 1000
            
            # Record operation metrics
            record_redis_operation("ping", ping_time / 1000)
            record_redis_operation("set_get_delete", perf_time / 1000)
            
            # Calculate metrics
            memory_used = memory_info.get("used_memory", 0)
            memory_peak = memory_info.get("used_memory_peak", 0)
            memory_rss = memory_info.get("used_memory_rss", 0)
            
            # Update Prometheus metrics
            redis_memory_usage.labels(type="used").set(memory_used)
            redis_memory_usage.labels(type="peak").set(memory_peak)
            redis_memory_usage.labels(type="rss").set(memory_rss)
            
            # Connection pool info
            connected_clients = info.get("connected_clients", 0)
            redis_connection_pool_size.labels(pool_type="clients").set(connected_clients)
            
            # Calculate hit ratio
            keyspace_hits = stats_info.get("keyspace_hits", 0)
            keyspace_misses = stats_info.get("keyspace_misses", 0)
            total_requests = keyspace_hits + keyspace_misses
            hit_ratio = (keyspace_hits / total_requests * 100) if total_requests > 0 else 0
            
            collection_time = (time.time() - start_time) * 1000
            
            metrics = {
                "connectivity": {
                    "ping_time_ms": round(ping_time, 2),
                    "operation_time_ms": round(perf_time, 2),
                    "test_success": test_value == "test"
                },
                "memory": {
                    "used_bytes": memory_used,
                    "used_mb": round(memory_used / 1024 / 1024, 2),
                    "peak_bytes": memory_peak,
                    "peak_mb": round(memory_peak / 1024 / 1024, 2),
                    "rss_bytes": memory_rss,
                    "rss_mb": round(memory_rss / 1024 / 1024, 2),
                    "fragmentation_ratio": memory_info.get("mem_fragmentation_ratio", 0)
                },
                "performance": {
                    "hit_ratio_percent": round(hit_ratio, 2),
                    "keyspace_hits": keyspace_hits,
                    "keyspace_misses": keyspace_misses,
                    "total_commands_processed": stats_info.get("total_commands_processed", 0),
                    "instantaneous_ops_per_sec": stats_info.get("instantaneous_ops_per_sec", 0)
                },
                "connections": {
                    "connected_clients": connected_clients,
                    "client_longest_output_list": info.get("client_longest_output_list", 0),
                    "client_biggest_input_buf": info.get("client_biggest_input_buf", 0),
                    "blocked_clients": info.get("blocked_clients", 0)
                },
                "server": {
                    "redis_version": info.get("redis_version", "unknown"),
                    "uptime_in_seconds": info.get("uptime_in_seconds", 0),
                    "role": replication_info.get("role", "unknown"),
                    "tcp_port": info.get("tcp_port", 6379)
                }
            }
            
            # Determine health status
            healthy = (
                test_value == "test" and  # Basic operations work
                ping_time < 1000 and     # Ping under 1 second
                hit_ratio > 10           # Reasonable hit ratio (or no traffic yet)
            )
            
            errors = None
            if not healthy:
                errors = {}
                if test_value != "test":
                    errors["operations"] = "Basic operations failed"
                if ping_time >= 1000:
                    errors["latency"] = f"High ping latency: {ping_time:.2f}ms"
                if hit_ratio <= 10 and total_requests > 100:
                    errors["performance"] = f"Low hit ratio: {hit_ratio:.2f}%"
            
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
            logger.error(f"Error collecting cache metrics: {e}")
            
            return CollectorMetrics(
                component=self.get_component_name(),
                healthy=False,
                last_collection=datetime.now(),
                collection_duration_ms=round(collection_time, 2),
                metrics={},
                errors={"collection_error": str(e)}
            )
    
    async def stop_collection(self):
        """Stop collection and cleanup connections."""
        await super().stop_collection()
        
        if self._redis_client:
            await self._redis_client.close()
            logger.debug("Closed Redis monitoring connection") 