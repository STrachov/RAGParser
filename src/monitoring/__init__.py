"""
RAGParser Monitoring System

Comprehensive monitoring for all system components including:
- Prometheus metrics collection
- Health checks for all services  
- Request correlation and tracing
- Specialized collectors for each component
- Production-ready observability

Usage:
    from monitoring import setup_monitoring, health_checker
    
    # In FastAPI app
    setup_monitoring(app)
    
    # Manual health check
    health = await health_checker.get_system_health()
"""

from .prometheus_metrics import (
    metrics_endpoint, setup_fastapi_instrumentation,
    record_task_start, record_task_completion, 
    record_s3_operation, record_redis_operation
)
from .health_checks import health_checker, router as health_router
from .middleware import add_monitoring_middleware
from .collectors import (
    QueueCollector, WorkerCollector, CacheCollector, 
    StorageCollector, SystemCollector
)
from .collectors.base_collector import AsyncCollectorManager

# Global collector manager instance
collector_manager = AsyncCollectorManager()

def setup_monitoring(app, config: dict = None):
    """
    Set up comprehensive monitoring for a FastAPI application.
    
    Args:
        app: FastAPI application instance
        config: Optional monitoring configuration
    """
    if config is None:
        config = {}
    
    # Add health check routes
    app.include_router(health_router)
    
    # Note: /metrics endpoint is provided automatically by prometheus-fastapi-instrumentator
    # Don't add it manually to avoid conflicts
    
    # Add monitoring middleware
    add_monitoring_middleware(app, config)
    
    # Set up Prometheus instrumentation (this includes the /metrics endpoint)
    setup_fastapi_instrumentation(app)
    
    print("✅ RAGParser monitoring system initialized")


def setup_collectors(enable_background: bool = True):
    """
    Set up all metric collectors with optimized intervals.
    
    Args:
        enable_background: Whether to start background collection
    """
    # Add all collectors with optimized intervals to prevent slow collection warnings
    # System metrics are lightweight and updated frequently
    collector_manager.add_collector(SystemCollector(collection_interval=20.0))  # Reduced from 15s
    
    # Cache metrics are medium complexity
    collector_manager.add_collector(CacheCollector(collection_interval=45.0))   # Increased from 30s
    
    # Queue metrics have connection overhead
    collector_manager.add_collector(QueueCollector(collection_interval=45.0))   # Increased from 30s
    
    # Worker metrics involve Celery inspection which can be slow
    collector_manager.add_collector(WorkerCollector(collection_interval=60.0))  # Increased from 30s
    
    # Storage metrics involve S3 operations and are the slowest
    collector_manager.add_collector(StorageCollector(collection_interval=90.0)) # Increased from 60s
    
    if enable_background:
        import asyncio
        asyncio.create_task(collector_manager.start_all())
    
    print("✅ Monitoring collectors initialized")


async def get_comprehensive_status():
    """Get comprehensive system status from all collectors."""
    return {
        "collectors": collector_manager.get_all_status(),
        "health": await health_checker.get_system_health(),
        "metrics_available": True
    }


# Export commonly used items
__all__ = [
    # Main setup functions
    "setup_monitoring",
    "setup_collectors", 
    "get_comprehensive_status",
    
    # Core components
    "health_checker",
    "collector_manager",
    
    # Metrics functions
    "record_task_start",
    "record_task_completion", 
    "record_s3_operation",
    "record_redis_operation",
    
    # Collectors
    "QueueCollector",
    "WorkerCollector", 
    "CacheCollector",
    "StorageCollector",
    "SystemCollector",
    "AsyncCollectorManager"
] 