"""
Celery worker monitoring collector.

Monitors worker health, task processing rates, and resource usage.
"""

import time
from datetime import datetime
from typing import Dict, Any, List

from celery import Celery
from loguru import logger

from pdf_parser.settings import get_settings
from ..prometheus_metrics import (
    worker_active, worker_tasks_active, worker_memory_usage
)
from .base_collector import BaseCollector, CollectorMetrics


class WorkerCollector(BaseCollector):
    """Collector for Celery worker metrics."""
    
    def __init__(self, collection_interval: float = 30.0):
        super().__init__(collection_interval)
        self.settings = get_settings()
        self._celery_app = None
    
    def get_component_name(self) -> str:
        return "celery_workers"
    
    def _get_celery_app(self) -> Celery:
        """Get or create Celery app for monitoring."""
        if self._celery_app is None:
            self._celery_app = Celery("worker_monitor")
            self._celery_app.conf.update(
                broker_url=self.settings.CELERY_BROKER_URL,
                result_backend=self.settings.CELERY_RESULT_BACKEND,
                # Must match worker serialization settings to avoid msgpack deserialization errors
                task_serializer="msgpack",
                accept_content=["msgpack", "json"],
                result_serializer="msgpack",
            )
        return self._celery_app
    
    def _parse_worker_name(self, worker_name: str) -> Dict[str, str]:
        """Parse worker name to extract type and hostname."""
        # Example: celery@hostname or worker_gpu@hostname
        parts = worker_name.split('@')
        if len(parts) == 2:
            worker_type = parts[0]
            hostname = parts[1]
        else:
            worker_type = "unknown"
            hostname = worker_name
        
        return {
            "worker_type": worker_type,
            "hostname": hostname
        }
    
    async def collect_metrics(self) -> CollectorMetrics:
        """Collect Celery worker metrics."""
        start_time = time.time()
        
        try:
            celery_app = self._get_celery_app()
            inspect = celery_app.control.inspect()
            
            # Get worker information
            active_workers = inspect.active() or {}
            registered_workers = inspect.registered() or {}
            reserved_tasks = inspect.reserved() or {}
            stats = inspect.stats() or {}
            
            worker_metrics = {}
            total_workers = 0
            total_active_tasks = 0
            worker_types = {}
            
            # Process each worker
            for worker_name, worker_tasks in active_workers.items():
                parsed = self._parse_worker_name(worker_name)
                worker_type = parsed["worker_type"]
                hostname = parsed["hostname"]
                
                # Count workers by type
                worker_types[worker_type] = worker_types.get(worker_type, 0) + 1
                
                # Active tasks count
                active_task_count = len(worker_tasks)
                total_active_tasks += active_task_count
                
                # Reserved tasks count
                reserved_task_count = len(reserved_tasks.get(worker_name, []))
                
                # Worker stats
                worker_stat = stats.get(worker_name, {})
                pool_info = worker_stat.get('pool', {})
                
                worker_info = {
                    "worker_type": worker_type,
                    "hostname": hostname,
                    "active_tasks": active_task_count,
                    "reserved_tasks": reserved_task_count,
                    "max_concurrency": pool_info.get('max-concurrency', 0),
                    "processes": pool_info.get('processes', []),
                    "total_tasks": worker_stat.get('total', {}),
                    "registered_tasks": registered_workers.get(worker_name, [])
                }
                
                worker_metrics[worker_name] = worker_info
                total_workers += 1
                
                # Update Prometheus metrics
                worker_active.labels(
                    worker_type=worker_type,
                    hostname=hostname
                ).set(1)
                
                worker_tasks_active.labels(
                    worker_id=worker_name,
                    worker_type=worker_type
                ).set(active_task_count)
            
            # Set inactive workers to 0
            for worker_name in registered_workers.keys():
                if worker_name not in active_workers:
                    parsed = self._parse_worker_name(worker_name)
                    worker_active.labels(
                        worker_type=parsed["worker_type"],
                        hostname=parsed["hostname"]
                    ).set(0)
            
            collection_time = (time.time() - start_time) * 1000
            
            # Calculate additional metrics
            avg_tasks_per_worker = total_active_tasks / total_workers if total_workers > 0 else 0
            
            metrics = {
                "total_workers": total_workers,
                "total_active_tasks": total_active_tasks,
                "worker_types": worker_types,
                "workers": worker_metrics,
                "avg_tasks_per_worker": round(avg_tasks_per_worker, 2),
                "registered_task_types": self._get_registered_task_types(registered_workers)
            }
            
            # Determine health status
            healthy = total_workers > 0
            errors = None
            
            if not healthy:
                errors = {"workers": "No active workers found"}
            elif total_workers < 2:  # Warning if too few workers
                errors = {"capacity": f"Only {total_workers} worker(s) active"}
            
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
            logger.error(f"Error collecting worker metrics: {e}")
            
            return CollectorMetrics(
                component=self.get_component_name(),
                healthy=False,
                last_collection=datetime.now(),
                collection_duration_ms=round(collection_time, 2),
                metrics={},
                errors={"collection_error": str(e)}
            )
    
    def _get_registered_task_types(self, registered_workers: Dict[str, List]) -> Dict[str, int]:
        """Get count of registered task types across all workers."""
        task_counts = {}
        
        for worker_name, tasks in registered_workers.items():
            for task in tasks:
                task_counts[task] = task_counts.get(task, 0) + 1
        
        return task_counts 