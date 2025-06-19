"""
Base collector interface for monitoring metrics.

Provides common functionality and interface for all specialized collectors.
"""

import asyncio
import time
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime

from loguru import logger


@dataclass
class CollectorMetrics:
    """Standard metrics returned by collectors."""
    component: str
    healthy: bool
    last_collection: datetime
    collection_duration_ms: float
    metrics: Dict[str, Any]
    errors: Dict[str, str] = None


class BaseCollector(ABC):
    """Base class for all metric collectors."""
    
    def __init__(self, collection_interval: float = 30.0):
        self.collection_interval = collection_interval
        self.last_collection: Optional[datetime] = None
        self.is_running = False
        self._task: Optional[asyncio.Task] = None
        self.errors = {}
    
    @abstractmethod
    async def collect_metrics(self) -> CollectorMetrics:
        """Collect metrics for this component."""
        pass
    
    @abstractmethod
    def get_component_name(self) -> str:
        """Get the name of the component this collector monitors."""
        pass
    
    async def start_collection(self):
        """Start periodic metric collection."""
        if self.is_running:
            logger.warning(f"{self.get_component_name()} collector already running")
            return
        
        self.is_running = True
        self._task = asyncio.create_task(self._collection_loop())
        logger.info(f"Started {self.get_component_name()} collector (interval: {self.collection_interval}s)")
    
    async def stop_collection(self):
        """Stop periodic metric collection."""
        if not self.is_running:
            return
        
        self.is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        
        logger.info(f"Stopped {self.get_component_name()} collector")
    
    async def _collection_loop(self):
        """Main collection loop."""
        while self.is_running:
            try:
                start_time = time.time()
                await self.collect_metrics()
                collection_time = time.time() - start_time
                
                self.last_collection = datetime.now()
                
                # Log slow collections
                if collection_time > 5.0:  # 5 second threshold
                    logger.warning(
                        f"Slow metric collection for {self.get_component_name()}: {collection_time:.2f}s"
                    )
                
            except Exception as e:
                logger.error(f"Error in {self.get_component_name()} collector: {e}")
                self.errors[datetime.now().isoformat()] = str(e)
                
                # Keep only last 10 errors
                if len(self.errors) > 10:
                    oldest_key = min(self.errors.keys())
                    del self.errors[oldest_key]
            
            # Wait for next collection
            await asyncio.sleep(self.collection_interval)
    
    def get_status(self) -> Dict[str, Any]:
        """Get collector status information."""
        return {
            "component": self.get_component_name(),
            "running": self.is_running,
            "last_collection": self.last_collection.isoformat() if self.last_collection else None,
            "collection_interval": self.collection_interval,
            "error_count": len(self.errors),
            "recent_errors": list(self.errors.values())[-3:] if self.errors else []
        }
    
    async def health_check(self) -> bool:
        """Perform a health check for this collector."""
        try:
            metrics = await self.collect_metrics()
            return metrics.healthy
        except Exception as e:
            logger.error(f"Health check failed for {self.get_component_name()}: {e}")
            return False


class AsyncCollectorManager:
    """Manager for multiple collectors with coordinated lifecycle."""
    
    def __init__(self):
        self.collectors: Dict[str, BaseCollector] = {}
    
    def add_collector(self, collector: BaseCollector):
        """Add a collector to the manager."""
        component_name = collector.get_component_name()
        self.collectors[component_name] = collector
        logger.info(f"Added {component_name} collector to manager")
    
    async def start_all(self):
        """Start all collectors."""
        for collector in self.collectors.values():
            await collector.start_collection()
        
        logger.info(f"Started {len(self.collectors)} collectors")
    
    async def stop_all(self):
        """Stop all collectors."""
        for collector in self.collectors.values():
            await collector.stop_collection()
        
        logger.info(f"Stopped {len(self.collectors)} collectors")
    
    async def collect_all_metrics(self) -> Dict[str, CollectorMetrics]:
        """Collect metrics from all collectors."""
        results = {}
        
        # Collect metrics in parallel
        tasks = [
            collector.collect_metrics() 
            for collector in self.collectors.values()
        ]
        
        metrics_list = await asyncio.gather(*tasks, return_exceptions=True)
        
        for collector, metrics in zip(self.collectors.values(), metrics_list):
            component_name = collector.get_component_name()
            
            if isinstance(metrics, Exception):
                logger.error(f"Failed to collect metrics for {component_name}: {metrics}")
                results[component_name] = CollectorMetrics(
                    component=component_name,
                    healthy=False,
                    last_collection=datetime.now(),
                    collection_duration_ms=0,
                    metrics={},
                    errors={"collection_error": str(metrics)}
                )
            else:
                results[component_name] = metrics
        
        return results
    
    def get_all_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all collectors."""
        return {
            name: collector.get_status() 
            for name, collector in self.collectors.items()
        }
    
    async def health_check_all(self) -> Dict[str, bool]:
        """Perform health checks on all collectors."""
        tasks = [
            collector.health_check() 
            for collector in self.collectors.values()
        ]
        
        health_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        return {
            collector.get_component_name(): (
                result if isinstance(result, bool) else False
            )
            for collector, result in zip(self.collectors.values(), health_results)
        } 