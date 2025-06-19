"""
Health check endpoints for RAGParser monitoring.

Provides comprehensive health checks for all system components with FastAPI integration.
"""

import time
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict

# Use existing redis module
import redis.asyncio as aioredis

# Optional imports with fallbacks
try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False
    
try:
    import aio_pika
    AIO_PIKA_AVAILABLE = True
except ImportError:
    AIO_PIKA_AVAILABLE = False

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from celery import Celery
from loguru import logger

from pdf_parser.settings import get_settings
from pdf_parser.utils.storage import get_s3_client

# Health check models
class ComponentHealth(BaseModel):
    """Health status of a system component."""
    component: str
    healthy: bool
    response_time_ms: float
    details: Dict[str, Any] = {}
    error_message: Optional[str] = None
    last_check: datetime

class SystemHealthResponse(BaseModel):
    """Overall system health response."""
    overall_healthy: bool
    checked_at: datetime
    components: Dict[str, ComponentHealth]
    critical_issues: List[str] = []

# Create router
router = APIRouter(prefix="/health", tags=["health"])

class HealthChecker:
    """Centralized health checking for all system components."""
    
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
            self._celery_app = Celery("health_checker")
            self._celery_app.conf.update(
                broker_url=self.settings.CELERY_BROKER_URL,
                result_backend=self.settings.CELERY_RESULT_BACKEND,
                # Must match worker serialization settings to avoid msgpack deserialization errors
                task_serializer="msgpack",
                accept_content=["msgpack", "json"],
                result_serializer="msgpack",
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
            
            # Get memory info
            memory_info = await redis_client.info("memory")
            
            # Test set/get performance
            test_key = f"health_check_{int(time.time())}"
            await redis_client.set(test_key, "test", ex=60)
            result = await redis_client.get(test_key)
            await redis_client.delete(test_key)
            
            response_time = (time.time() - start_time) * 1000
            
            # Calculate hit ratio
            hits = memory_info.get("keyspace_hits", 0)
            misses = memory_info.get("keyspace_misses", 0)
            hit_ratio = None
            if hits + misses > 0:
                hit_ratio = round(hits / (hits + misses) * 100, 2)
            
            details = {
                "memory_used_mb": round(memory_info["used_memory"] / 1024 / 1024, 2),
                "memory_peak_mb": round(memory_info["used_memory_peak"] / 1024 / 1024, 2),
                "connected_clients": memory_info.get("connected_clients", 0),
                "keyspace_hits": hits,
                "keyspace_misses": misses,
                "hit_ratio_percent": hit_ratio,
                "test_operation_success": result == b"test"
            }
            
            return ComponentHealth(
                component="redis",
                healthy=True,
                response_time_ms=round(response_time, 2),
                details=details,
                last_check=datetime.now()
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return ComponentHealth(
                component="redis",
                healthy=False,
                response_time_ms=round(response_time, 2),
                details={},
                error_message=str(e),
                last_check=datetime.now()
            )

    async def check_rabbitmq_health(self) -> ComponentHealth:
        """Check RabbitMQ broker health via Celery."""
        start_time = time.time()
        
        try:
            celery_app = self._get_celery_app()
            
            # Get broker connection info
            inspect = celery_app.control.inspect()
            
            # Check active workers
            active_workers = inspect.active() or {}
            registered_workers = inspect.registered() or {}
            reserved_tasks = inspect.reserved() or {}
            active_tasks = inspect.active() or {}
            
            total_workers = len(active_workers)
            total_active_tasks = sum(len(tasks) for tasks in active_tasks.values())
            total_reserved_tasks = sum(len(tasks) for tasks in reserved_tasks.values())
            
            response_time = (time.time() - start_time) * 1000
            
            details = {
                "active_workers": total_workers,
                "active_tasks": total_active_tasks,
                "reserved_tasks": total_reserved_tasks,
                "worker_names": list(active_workers.keys()),
                "broker_url": self.settings.CELERY_BROKER_URL.split('@')[-1] if '@' in self.settings.CELERY_BROKER_URL else "masked"
            }
            
            # Determine health status
            healthy = total_workers > 0
            error_message = None if healthy else "No active workers found"
            
            return ComponentHealth(
                component="rabbitmq",
                healthy=healthy,
                response_time_ms=round(response_time, 2),
                details=details,
                error_message=error_message,
                last_check=datetime.now()
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return ComponentHealth(
                component="rabbitmq",
                healthy=False,
                response_time_ms=round(response_time, 2),
                details={},
                error_message=str(e),
                last_check=datetime.now()
            )

    async def check_s3_health(self) -> ComponentHealth:
        """Check S3/R2 storage health."""
        start_time = time.time()
        
        try:
            s3_client = self._get_s3_client()
            
            # Check bucket exists
            bucket_name = self.settings.S3_BUCKET_NAME
            await asyncio.get_event_loop().run_in_executor(
                None, s3_client.head_bucket, {"Bucket": bucket_name}
            )
            
            # Test upload/download operations
            test_key = f"health_check_{int(time.time())}.txt"
            test_content = b"health check test"
            
            # Upload test
            upload_start = time.time()
            await asyncio.get_event_loop().run_in_executor(
                None, 
                s3_client.put_object,
                {
                    "Bucket": bucket_name,
                    "Key": test_key,
                    "Body": test_content,
                    "ContentType": "text/plain"
                }
            )
            upload_time = (time.time() - upload_start) * 1000
            
            # Download test
            download_start = time.time()
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                s3_client.get_object,
                {"Bucket": bucket_name, "Key": test_key}
            )
            download_time = (time.time() - download_start) * 1000
            
            # Cleanup
            await asyncio.get_event_loop().run_in_executor(
                None,
                s3_client.delete_object,
                {"Bucket": bucket_name, "Key": test_key}
            )
            
            total_response_time = (time.time() - start_time) * 1000
            
            details = {
                "bucket_name": bucket_name,
                "bucket_exists": True,
                "upload_time_ms": round(upload_time, 2),
                "download_time_ms": round(download_time, 2),
                "test_operation_success": True,
                "endpoint_url": getattr(self.settings, 'S3_ENDPOINT_URL', 'aws')
            }
            
            return ComponentHealth(
                component="s3",
                healthy=True,
                response_time_ms=round(total_response_time, 2),
                details=details,
                last_check=datetime.now()
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return ComponentHealth(
                component="s3",
                healthy=False,
                response_time_ms=round(response_time, 2),
                details={"bucket_name": getattr(self.settings, 'S3_BUCKET_NAME', 'unknown')},
                error_message=str(e),
                last_check=datetime.now()
            )

    async def check_worker_health(self) -> ComponentHealth:
        """Check Celery worker health and capacity."""
        start_time = time.time()
        
        try:
            celery_app = self._get_celery_app()
            inspect = celery_app.control.inspect()
            
            # Get worker stats
            stats = inspect.stats() or {}
            active = inspect.active() or {}
            
            total_workers = len(stats)
            worker_details = {}
            
            for worker_name, worker_stats in stats.items():
                pool_info = worker_stats.get('pool', {})
                worker_details[worker_name] = {
                    "processes": pool_info.get('processes', 'unknown'),
                    "max_concurrency": pool_info.get('max-concurrency', 'unknown'),
                    "active_tasks": len(active.get(worker_name, [])),
                    "total_tasks": worker_stats.get('total', {})
                }
            
            response_time = (time.time() - start_time) * 1000
            
            details = {
                "total_workers": total_workers,
                "workers": worker_details,
                "queues_declared": list(set(
                    queue for worker_active in active.values() 
                    for task in worker_active 
                    for queue in [task.get('delivery_info', {}).get('routing_key', 'unknown')]
                ))
            }
            
            healthy = total_workers > 0
            error_message = None if healthy else "No workers available"
            
            return ComponentHealth(
                component="workers",
                healthy=healthy,
                response_time_ms=round(response_time, 2),
                details=details,
                error_message=error_message,
                last_check=datetime.now()
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return ComponentHealth(
                component="workers",
                healthy=False,
                response_time_ms=round(response_time, 2),
                details={},
                error_message=str(e),
                last_check=datetime.now()
            )

    async def get_system_health(self) -> SystemHealthResponse:
        """Get comprehensive system health."""
        # Run all health checks in parallel
        health_checks = await asyncio.gather(
            self.check_redis_health(),
            self.check_rabbitmq_health(), 
            self.check_s3_health(),
            self.check_worker_health(),
            return_exceptions=True
        )
        
        components = {}
        critical_issues = []
        
        # Process health check results
        for health_check in health_checks:
            if isinstance(health_check, ComponentHealth):
                components[health_check.component] = health_check
                
                # Check for critical issues
                if not health_check.healthy:
                    critical_issues.append(f"{health_check.component}_unhealthy")
                elif health_check.response_time_ms > 5000:  # 5 second threshold
                    critical_issues.append(f"{health_check.component}_slow_response")
            else:
                # Exception occurred
                logger.error(f"Health check failed with exception: {health_check}")
                critical_issues.append("health_check_exception")
        
        # Determine overall health
        overall_healthy = all(
            component.healthy for component in components.values()
        )
        
        return SystemHealthResponse(
            overall_healthy=overall_healthy,
            checked_at=datetime.now(),
            components=components,
            critical_issues=critical_issues
        )

    async def cleanup(self):
        """Clean up connections."""
        if self._redis_client:
            await self._redis_client.close()


# Global health checker instance
health_checker = HealthChecker()


# ==================== FASTAPI ENDPOINTS ====================

@router.get("/", response_model=SystemHealthResponse)
async def get_overall_health():
    """Get overall system health status."""
    return await health_checker.get_system_health()


@router.get("/detailed", response_model=SystemHealthResponse)
async def get_detailed_health():
    """Get detailed system health with component breakdown."""
    return await health_checker.get_system_health()


@router.get("/redis", response_model=ComponentHealth)
async def get_redis_health():
    """Get Redis-specific health status."""
    return await health_checker.check_redis_health()


@router.get("/rabbitmq", response_model=ComponentHealth)
async def get_rabbitmq_health():
    """Get RabbitMQ broker health status."""
    return await health_checker.check_rabbitmq_health()


@router.get("/s3", response_model=ComponentHealth)
async def get_s3_health():
    """Get S3/R2 storage health status."""
    return await health_checker.check_s3_health()


@router.get("/workers", response_model=ComponentHealth)
async def get_worker_health():
    """Get Celery worker health status."""
    return await health_checker.check_worker_health()


@router.get("/quick")
async def quick_health_check():
    """Quick health check for load balancers."""
    try:
        # Just check Redis connection for speed
        redis_health = await health_checker.check_redis_health()
        
        if redis_health.healthy:
            return {"status": "healthy"}
        else:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Service unavailable"
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Health check failed: {str(e)}"
        ) 