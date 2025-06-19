import asyncio
import json
import time
import warnings
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from pathlib import Path

import redis.asyncio as aioredis
import httpx
from celery import Celery
from celery.events.state import State
from loguru import logger

from pdf_parser.settings import get_settings
from pdf_parser.utils.storage import get_s3_client

# ============================================================================
# DEPRECATION WARNING: This module is being replaced by the new monitoring system
# Use `from monitoring import health_checker` for new code
# ============================================================================

warnings.warn(
    "SystemMonitor is deprecated. Use the new monitoring system: "
    "`from monitoring import health_checker, setup_monitoring`",
    DeprecationWarning,
    stacklevel=2
)

@dataclass
class ComponentHealth:
    """Health status of a system component."""
    component: str
    healthy: bool
    details: Dict[str, Any]
    last_check: datetime
    error_message: Optional[str] = None


@dataclass
class TaskInfo:
    """Detailed information about a task."""
    task_id: str
    state: str
    progress: int
    created_at: datetime
    updated_at: datetime
    worker: Optional[str] = None
    queue: Optional[str] = None
    runtime: Optional[float] = None
    retries: int = 0
    error_message: Optional[str] = None
    result_key: Optional[str] = None
    table_keys: Optional[List[str]] = None


@dataclass
class QueueStats:
    """Statistics about task queues."""
    queue_name: str
    active_tasks: int
    pending_tasks: int
    failed_tasks: int
    succeeded_tasks: int
    processing_rate: float  # tasks per minute
    avg_processing_time: float  # seconds
    oldest_pending_age: Optional[float] = None  # seconds


@dataclass
class WorkerInfo:
    """Information about Celery workers."""
    worker_id: str
    status: str
    active_tasks: int
    processed_tasks: int
    load_average: List[float]
    memory_usage: Dict[str, Any]
    gpu_info: Optional[Dict[str, Any]] = None


class SystemMonitor:
    """Comprehensive monitoring for the PDF parsing system."""
    
    def __init__(self):
        self.settings = get_settings()
        self._redis_client: Optional[aioredis.Redis] = None
        self._celery_app: Optional[Celery] = None
        self._s3_client = None
        
    async def _get_redis_client(self) -> aioredis.Redis:
        """Get or create Redis client."""
        if self._redis_client is None:
            self._redis_client = aioredis.from_url(self.settings.REDIS_URL)
        return self._redis_client
    
    def _get_celery_app(self) -> Celery:
        """Get or create Celery app for monitoring."""
        if self._celery_app is None:
            self._celery_app = Celery("pdf_parser")
            self._celery_app.conf.update(
                broker_url=self.settings.CELERY_BROKER_URL,
                result_backend=self.settings.CELERY_RESULT_BACKEND,
                task_serializer="msgpack",
                accept_content=["msgpack", "json"],
                result_serializer="msgpack",
                task_compression="gzip",
            )
        return self._celery_app
    
    def _get_s3_client(self):
        """Get or create S3 client."""
        if self._s3_client is None:
            self._s3_client = get_s3_client()
        return self._s3_client
    
    async def check_redis_health(self) -> ComponentHealth:
        """Check Redis connection and performance."""
        start_time = time.time()
        try:
            redis_client = await self._get_redis_client()
            
            # Test basic operations
            await redis_client.ping()
            
            # Check memory usage
            memory_info = await redis_client.info("memory")
            
            # Test set/get performance
            test_key = f"health_check_{int(time.time())}"
            await redis_client.set(test_key, "test", ex=60)
            await redis_client.get(test_key)
            await redis_client.delete(test_key)
            
            response_time = time.time() - start_time
            
            details = {
                "response_time_ms": round(response_time * 1000, 2),
                "memory_used_mb": round(memory_info["used_memory"] / 1024 / 1024, 2),
                "memory_peak_mb": round(memory_info["used_memory_peak"] / 1024 / 1024, 2),
                "connected_clients": memory_info.get("connected_clients", 0),
                "keyspace_hits": memory_info.get("keyspace_hits", 0),
                "keyspace_misses": memory_info.get("keyspace_misses", 0),
            }
            
            # Calculate hit ratio
            hits = details["keyspace_hits"]
            misses = details["keyspace_misses"]
            if hits + misses > 0:
                details["hit_ratio"] = round(hits / (hits + misses) * 100, 2)
            
            return ComponentHealth(
                component="redis",
                healthy=True,
                details=details,
                last_check=datetime.now()
            )
            
        except Exception as e:
            return ComponentHealth(
                component="redis",
                healthy=False,
                details={"response_time_ms": round((time.time() - start_time) * 1000, 2)},
                last_check=datetime.now(),
                error_message=str(e)
            )
    
    async def check_s3_health(self) -> ComponentHealth:
        """Check S3 connection and performance."""
        start_time = time.time()
        try:
            s3_client = self._get_s3_client()
            
            # Test bucket access
            bucket_exists = s3_client.bucket_exists()
            
            # Test upload/download performance
            test_key = f"health_check_{int(time.time())}.txt"
            test_content = "health check test"
            
            upload_start = time.time()
            s3_client.upload_text(test_content, test_key)
            upload_time = time.time() - upload_start
            
            download_start = time.time()
            downloaded_content = s3_client.download_text(test_key)
            download_time = time.time() - download_start
            
            # Clean up test file
            s3_client.delete_file(test_key)
            
            total_time = time.time() - start_time
            
            details = {
                "response_time_ms": round(total_time * 1000, 2),
                "upload_time_ms": round(upload_time * 1000, 2),
                "download_time_ms": round(download_time * 1000, 2),
                "bucket_exists": bucket_exists,
                "test_data_integrity": downloaded_content == test_content,
            }
            
            return ComponentHealth(
                component="s3",
                healthy=True,
                details=details,
                last_check=datetime.now()
            )
            
        except Exception as e:
            return ComponentHealth(
                component="s3",
                healthy=False,
                details={"response_time_ms": round((time.time() - start_time) * 1000, 2)},
                last_check=datetime.now(),
                error_message=str(e)
            )
    
    async def check_celery_health(self) -> ComponentHealth:
        """Check Celery broker and workers."""
        start_time = time.time()
        try:
            celery_app = self._get_celery_app()
            
            # Get worker statistics
            inspect = celery_app.control.inspect()
            
            # Check active workers
            active_workers = inspect.active() or {}
            registered_workers = inspect.registered() or {}
            
            # Get queue lengths (if possible)
            try:
                # This might not work with all brokers
                reserved_tasks = inspect.reserved() or {}
                active_tasks = inspect.active() or {}
            except Exception:
                reserved_tasks = {}
                active_tasks = {}
            
            total_workers = len(active_workers)
            total_active_tasks = sum(len(tasks) for tasks in active_tasks.values())
            total_reserved_tasks = sum(len(tasks) for tasks in reserved_tasks.values())
            
            details = {
                "response_time_ms": round((time.time() - start_time) * 1000, 2),
                "active_workers": total_workers,
                "active_tasks": total_active_tasks,
                "reserved_tasks": total_reserved_tasks,
                "workers": list(active_workers.keys()),
            }
            
            # Add per-worker details
            for worker_name, tasks in active_tasks.items():
                details[f"worker_{worker_name}_active_tasks"] = len(tasks)
            
            healthy = total_workers > 0
            error_message = None if healthy else "No active workers found"
            
            return ComponentHealth(
                component="celery",
                healthy=healthy,
                details=details,
                last_check=datetime.now(),
                error_message=error_message
            )
            
        except Exception as e:
            return ComponentHealth(
                component="celery",
                healthy=False,
                details={"response_time_ms": round((time.time() - start_time) * 1000, 2)},
                last_check=datetime.now(),
                error_message=str(e)
            )
    
    async def get_task_details(self, task_id: str) -> Optional[TaskInfo]:
        """Get detailed information about a specific task."""
        try:
            redis_client = await self._get_redis_client()
            
            # Get task data from Redis
            task_data = await redis_client.get(f"task:{task_id}")
            if not task_data:
                # If no custom task data, try to get from Celery directly
                return await self._get_celery_task_info(task_id)
            
            task_info = json.loads(task_data)
            
            # Get additional metadata if available
            metadata_key = f"task_metadata:{task_id}"
            metadata = await redis_client.get(metadata_key)
            if metadata:
                metadata_info = json.loads(metadata)
                task_info.update(metadata_info)
            
            # Always try to get fresh Celery task info and merge it
            celery_app = self._get_celery_app()
            try:
                celery_result = celery_app.AsyncResult(task_id)
                celery_state = celery_result.state
                celery_info = celery_result.info if celery_result.info else {}
                
                # Update state from Celery if it's more recent or different
                if celery_state and celery_state != "PENDING":
                    # Map Celery states to our states
                    state_mapping = {
                        "PENDING": "pending",
                        "STARTED": "running", 
                        "RETRY": "running",
                        "SUCCESS": "succeeded",
                        "FAILURE": "failed",
                        "REVOKED": "failed"
                    }
                    mapped_state = state_mapping.get(celery_state, celery_state.lower())
                    
                    # If Celery shows a different state, use it and update our tracking
                    if mapped_state != task_info.get("state"):
                        logger.info(f"Syncing task {task_id} state from Celery: {task_info.get('state')} -> {mapped_state}")
                        task_info["state"] = mapped_state
                        task_info["celery_synced"] = True
                        
                        # Update our Redis tracking
                        task_info["updated_at"] = datetime.now().isoformat()
                        await redis_client.set(
                            f"task:{task_id}",
                            json.dumps(task_info),
                            ex=self.settings.CACHE_TTL
                        )
                
                task_info["celery_state"] = celery_state
                task_info["celery_info"] = celery_info
                
            except Exception as e:
                logger.warning(f"Could not get Celery info for task {task_id}: {e}")
            
            return TaskInfo(
                task_id=task_id,
                state=task_info.get("state", "unknown"),
                progress=task_info.get("progress", 0),
                created_at=datetime.fromisoformat(task_info.get("created_at", datetime.now().isoformat())),
                updated_at=datetime.fromisoformat(task_info.get("updated_at", datetime.now().isoformat())),
                worker=task_info.get("worker") or task_info.get("worker_name"),
                queue=task_info.get("queue"),
                runtime=task_info.get("runtime") or task_info.get("total_runtime_seconds"),
                retries=task_info.get("retries", 0),
                error_message=task_info.get("error") or task_info.get("error_message"),
                result_key=task_info.get("result_key"),
                table_keys=task_info.get("table_keys"),
            )
            
        except Exception as e:
            logger.error(f"Error getting task details for {task_id}: {e}")
            return None

    async def _get_celery_task_info(self, task_id: str) -> Optional[TaskInfo]:
        """Get task info directly from Celery when our tracking is missing."""
        try:
            celery_app = self._get_celery_app()
            celery_result = celery_app.AsyncResult(task_id)
            
            if not celery_result.state or celery_result.state == "PENDING":
                return None
            
            # Map Celery states to our states
            state_mapping = {
                "PENDING": "pending",
                "STARTED": "running",
                "RETRY": "running", 
                "SUCCESS": "succeeded",
                "FAILURE": "failed",
                "REVOKED": "failed"
            }
            
            state = state_mapping.get(celery_result.state, celery_result.state.lower())
            
            # Try to extract progress from result info
            progress = 0
            if celery_result.info and isinstance(celery_result.info, dict):
                progress = celery_result.info.get("progress", 0)
            
            # Create minimal task info from Celery data
            now = datetime.now()
            return TaskInfo(
                task_id=task_id,
                state=state,
                progress=progress,
                created_at=now,  # We don't know the real creation time
                updated_at=now,
                worker=None,  # Celery doesn't easily provide this
                queue=None,
                runtime=None,
                retries=0,
                error_message=str(celery_result.info) if state == "failed" and celery_result.info else None,
                result_key=None,
                table_keys=None,
            )
            
        except Exception as e:
            logger.warning(f"Could not get Celery task info for {task_id}: {e}")
            return None
    
    async def get_all_tasks(self, limit: int = 100) -> List[TaskInfo]:
        """Get information about all tasks."""
        try:
            redis_client = await self._get_redis_client()
            
            # Get all task keys from our tracking
            task_keys = await redis_client.keys("task:*")
            
            # Also discover tasks from Celery that we might not be tracking
            celery_task_ids = await self._discover_celery_tasks()
            
            # Combine task IDs from both sources
            tracked_task_ids = {task_key.decode().split(":", 1)[1] for task_key in task_keys}
            all_task_ids = tracked_task_ids.union(celery_task_ids)
            
            # Limit results
            all_task_ids = list(all_task_ids)[:limit]
            
            tasks = []
            for task_id in all_task_ids:
                task_info = await self.get_task_details(task_id)
                if task_info:
                    tasks.append(task_info)
            
            # Sort by creation time, newest first
            tasks.sort(key=lambda t: t.created_at, reverse=True)
            
            return tasks
            
        except Exception as e:
            logger.error(f"Error getting all tasks: {e}")
            return []

    async def _discover_celery_tasks(self) -> set:
        """Discover task IDs from Celery's active and reserved queues."""
        task_ids = set()
        
        try:
            celery_app = self._get_celery_app()
            inspect = celery_app.control.inspect()
            
            # Get active tasks
            active_tasks = inspect.active() or {}
            for worker_tasks in active_tasks.values():
                for task in worker_tasks:
                    task_ids.add(task.get('id'))
            
            # Get reserved tasks
            reserved_tasks = inspect.reserved() or {}
            for worker_tasks in reserved_tasks.values():
                for task in worker_tasks:
                    task_ids.add(task.get('id'))
            
            # Remove None values
            task_ids.discard(None)
            
            logger.debug(f"Discovered {len(task_ids)} tasks from Celery queues")
            
        except Exception as e:
            logger.warning(f"Could not discover Celery tasks: {e}")
        
        return task_ids
    
    async def get_stuck_tasks(self, stuck_threshold_minutes: int = 30) -> List[TaskInfo]:
        """Get tasks that appear to be stuck."""
        all_tasks = await self.get_all_tasks()
        stuck_tasks = []
        
        now = datetime.now()
        stuck_threshold = timedelta(minutes=stuck_threshold_minutes)
        
        for task in all_tasks:
            if task.state in ["pending", "running"]:
                age = now - task.updated_at
                if age > stuck_threshold:
                    stuck_tasks.append(task)
        
        return stuck_tasks
    
    async def analyze_pending_tasks(self) -> Dict[str, Any]:
        """Analyze why tasks might be stuck on pending."""
        all_tasks = await self.get_all_tasks()
        pending_tasks = [t for t in all_tasks if t.state == "pending"]
        
        if not pending_tasks:
            return {"status": "no_pending_tasks", "count": 0}
        
        # Analyze pending tasks
        now = datetime.now()
        pending_ages = [(now - task.created_at).total_seconds() for task in pending_tasks]
        
        oldest_pending = max(pending_ages) if pending_ages else 0
        avg_pending_age = sum(pending_ages) / len(pending_ages) if pending_ages else 0
        
        # Check system health
        redis_health = await self.check_redis_health()
        celery_health = await self.check_celery_health()
        
        analysis = {
            "status": "analysis_complete",
            "pending_count": len(pending_tasks),
            "oldest_pending_seconds": oldest_pending,
            "average_pending_age_seconds": avg_pending_age,
            "redis_healthy": redis_health.healthy,
            "celery_healthy": celery_health.healthy,
            "active_workers": celery_health.details.get("active_workers", 0),
            "possible_causes": []
        }
        
        # Identify possible causes
        if not celery_health.healthy:
            analysis["possible_causes"].append("celery_broker_unavailable")
        
        if celery_health.details.get("active_workers", 0) == 0:
            analysis["possible_causes"].append("no_active_workers")
        
        if not redis_health.healthy:
            analysis["possible_causes"].append("redis_connection_issues")
        
        if oldest_pending > 1800:  # 30 minutes
            analysis["possible_causes"].append("worker_overload_or_crash")
        
        return analysis
    
    async def get_system_overview(self) -> Dict[str, Any]:
        """Get a comprehensive overview of the system."""
        start_time = time.time()
        
        # Run all health checks in parallel
        redis_health, s3_health, celery_health = await asyncio.gather(
            self.check_redis_health(),
            self.check_s3_health(),
            self.check_celery_health(),
            return_exceptions=True
        )
        
        # Get task statistics
        all_tasks = await self.get_all_tasks()
        stuck_tasks = await self.get_stuck_tasks()
        pending_analysis = await self.analyze_pending_tasks()
        
        # Calculate task statistics
        task_states = {}
        for task in all_tasks:
            task_states[task.state] = task_states.get(task.state, 0) + 1
        
        overview = {
            "timestamp": datetime.now().isoformat(),
            "collection_time_ms": round((time.time() - start_time) * 1000, 2),
            "system_health": {
                "redis": asdict(redis_health) if isinstance(redis_health, ComponentHealth) else {"error": str(redis_health)},
                "s3": asdict(s3_health) if isinstance(s3_health, ComponentHealth) else {"error": str(s3_health)},
                "celery": asdict(celery_health) if isinstance(celery_health, ComponentHealth) else {"error": str(celery_health)},
            },
            "task_statistics": {
                "total_tasks": len(all_tasks),
                "by_state": task_states,
                "stuck_tasks": len(stuck_tasks),
                "oldest_stuck_age_seconds": max(
                    [(datetime.now() - task.updated_at).total_seconds() for task in stuck_tasks],
                    default=0
                ),
            },
            "pending_analysis": pending_analysis,
        }
        
        # Overall health status
        all_healthy = all(
            isinstance(health, ComponentHealth) and health.healthy
            for health in [redis_health, s3_health, celery_health]
        )
        
        overview["overall_healthy"] = all_healthy
        overview["critical_issues"] = []
        
        if not all_healthy:
            overview["critical_issues"].append("component_unhealthy")
        
        if len(stuck_tasks) > 5:
            overview["critical_issues"].append("many_stuck_tasks")
        
        if pending_analysis.get("pending_count", 0) > 10:
            overview["critical_issues"].append("high_pending_count")
        
        return overview
    
    async def cleanup(self):
        """Clean up connections."""
        if self._redis_client:
            await self._redis_client.close()


# Monitoring utilities
async def diagnose_stuck_task(task_id: str) -> Dict[str, Any]:
    """Diagnose why a specific task might be stuck."""
    monitor = SystemMonitor()
    try:
        task_info = await monitor.get_task_details(task_id)
        if not task_info:
            return {"error": "task_not_found"}
        
        # Get system health
        system_overview = await monitor.get_system_overview()
        
        diagnosis = {
            "task_id": task_id,
            "task_info": asdict(task_info),
            "system_health": system_overview["system_health"],
            "recommendations": []
        }
        
        # Analyze task state
        if task_info.state == "pending":
            age_minutes = (datetime.now() - task_info.created_at).total_seconds() / 60
            
            if age_minutes > 5:
                diagnosis["recommendations"].append("Task has been pending for more than 5 minutes")
            
            if not system_overview["system_health"]["celery"]["healthy"]:
                diagnosis["recommendations"].append("Celery broker is unhealthy - check RabbitMQ/Redis")
            
            if system_overview["system_health"]["celery"]["details"]["active_workers"] == 0:
                diagnosis["recommendations"].append("No active workers - start Celery workers")
            
            if not system_overview["system_health"]["redis"]["healthy"]:
                diagnosis["recommendations"].append("Redis is unhealthy - check Redis connection")
        
        elif task_info.state == "running":
            age_minutes = (datetime.now() - task_info.updated_at).total_seconds() / 60
            
            if age_minutes > 15:
                diagnosis["recommendations"].append("Task has been running for more than 15 minutes - may be stuck")
            
            if task_info.progress == 0:
                diagnosis["recommendations"].append("Task shows no progress - worker may be unresponsive")
        
        return diagnosis
        
    finally:
        await monitor.cleanup()


async def get_monitoring_report() -> str:
    """Generate a human-readable monitoring report."""
    monitor = SystemMonitor()
    try:
        overview = await monitor.get_system_overview()
        
        report = []
        report.append("=== PDF Parser System Monitoring Report ===")
        report.append(f"Generated at: {overview['timestamp']}")
        report.append(f"Collection time: {overview['collection_time_ms']}ms")
        report.append("")
        
        # Overall health
        status_emoji = "✅" if overview["overall_healthy"] else "❌"
        report.append(f"Overall Health: {status_emoji} {'HEALTHY' if overview['overall_healthy'] else 'UNHEALTHY'}")
        
        if overview["critical_issues"]:
            report.append(f"Critical Issues: {', '.join(overview['critical_issues'])}")
        
        report.append("")
        
        # Component health
        report.append("=== Component Health ===")
        for component, health in overview["system_health"].items():
            if "error" in health:
                report.append(f"❌ {component.upper()}: ERROR - {health['error']}")
            elif health["healthy"]:
                response_time = health["details"].get("response_time_ms", 0)
                report.append(f"✅ {component.upper()}: OK ({response_time}ms)")
            else:
                error_msg = health.get("error_message", "Unknown error")
                report.append(f"❌ {component.upper()}: FAILED - {error_msg}")
        
        report.append("")
        
        # Task statistics
        report.append("=== Task Statistics ===")
        stats = overview["task_statistics"]
        report.append(f"Total tasks: {stats['total_tasks']}")
        
        for state, count in stats["by_state"].items():
            report.append(f"  {state}: {count}")
        
        if stats["stuck_tasks"] > 0:
            report.append(f"⚠️ Stuck tasks: {stats['stuck_tasks']} (oldest: {stats['oldest_stuck_age_seconds']:.0f}s)")
        
        report.append("")
        
        # Pending analysis
        pending = overview["pending_analysis"]
        if pending["status"] == "no_pending_tasks":
            report.append("✅ No pending tasks")
        else:
            report.append(f"⏳ Pending tasks: {pending['pending_count']}")
            report.append(f"  Oldest pending: {pending['oldest_pending_seconds']:.0f}s")
            report.append(f"  Average age: {pending['average_pending_age_seconds']:.0f}s")
            
            if pending["possible_causes"]:
                report.append("  Possible causes:")
                for cause in pending["possible_causes"]:
                    report.append(f"    - {cause.replace('_', ' ').title()}")
        
        return "\n".join(report)
        
    finally:
        await monitor.cleanup() 
