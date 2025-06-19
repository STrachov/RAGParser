"""
Prometheus metrics definitions and collectors for RAGParser.

This module centralizes all metrics collection for FastAPI, Celery, RabbitMQ, Redis, and S3.
"""

import time
import asyncio
from typing import Dict, Any, Optional

import psutil
from fastapi import Response
from loguru import logger

from pdf_parser.settings import get_settings

# Try to import prometheus packages, fall back to mock implementations
try:
    from prometheus_client import (
        Counter, Histogram, Gauge, Info, CollectorRegistry, 
        generate_latest, CONTENT_TYPE_LATEST
    )
    from prometheus_fastapi_instrumentator import Instrumentator  # type: ignore
    PROMETHEUS_AVAILABLE = True
except ImportError:
    logger.warning("Prometheus packages not installed. Using mock implementations.")
    PROMETHEUS_AVAILABLE = False
    
    # Mock implementations for development
    class MockMetric:
        def __init__(self, *args, **kwargs):
            self.name = args[0] if args else "mock_metric"
            
        def labels(self, **kwargs):
            return self
            
        def inc(self, value=1):
            pass
            
        def set(self, value):
            pass
            
        def observe(self, value):
            pass
    
    Counter = Histogram = Gauge = Info = MockMetric
    
    class MockRegistry:
        def __init__(self):
            pass
    
    CollectorRegistry = MockRegistry
    
    def generate_latest(registry=None):
        return b"# Prometheus metrics not available\n"
    
    CONTENT_TYPE_LATEST = "text/plain"
    
    class MockInstrumentator:
        def instrument(self, app):
            return self
        def expose(self, app, **kwargs):
            return self
    
    Instrumentator = MockInstrumentator

# Global metrics registry
REGISTRY = CollectorRegistry()

# ==================== API METRICS ====================
# Request metrics
api_requests_total = Counter(
    'api_requests_total',
    'Total number of API requests',
    ['method', 'endpoint', 'status_code'],
    registry=REGISTRY if PROMETHEUS_AVAILABLE else None
)

api_request_duration = Histogram(
    'api_request_duration_seconds',
    'API request duration in seconds',
    ['method', 'endpoint'],
    registry=REGISTRY if PROMETHEUS_AVAILABLE else None
)

api_active_requests = Gauge(
    'api_active_requests',
    'Number of active API requests',
    registry=REGISTRY if PROMETHEUS_AVAILABLE else None
)

# ==================== TASK METRICS ====================
task_duration = Histogram(
    'task_duration_seconds',
    'Task processing duration in seconds',
    ['task_name', 'parser_type', 'queue', 'status'],
    registry=REGISTRY if PROMETHEUS_AVAILABLE else None
)

task_total = Counter(
    'task_total',
    'Total number of tasks processed',
    ['task_name', 'parser_type', 'queue', 'status'],
    registry=REGISTRY if PROMETHEUS_AVAILABLE else None
)

task_queue_depth = Gauge(
    'task_queue_depth',
    'Number of tasks in queue by status',
    ['queue', 'status'],
    registry=REGISTRY if PROMETHEUS_AVAILABLE else None
)

task_retries_total = Counter(
    'task_retries_total',
    'Total number of task retries',
    ['task_name', 'parser_type', 'queue'],
    registry=REGISTRY if PROMETHEUS_AVAILABLE else None
)

# ==================== SYSTEM METRICS ====================
system_cpu_percent = Gauge(
    'system_cpu_percent',
    'CPU usage percentage',
    ['cpu'],
    registry=REGISTRY if PROMETHEUS_AVAILABLE else None
)

system_memory_bytes = Gauge(
    'system_memory_bytes',
    'Memory usage in bytes',
    ['type'],
    registry=REGISTRY if PROMETHEUS_AVAILABLE else None
)

system_disk_usage = Gauge(
    'system_disk_usage_percent',
    'Disk usage percentage',
    ['mount_point'],
    registry=REGISTRY if PROMETHEUS_AVAILABLE else None
)

# ==================== GPU METRICS ====================
gpu_utilization = Gauge(
    'gpu_utilization_percent',
    'GPU utilization percentage',
    ['gpu_id', 'gpu_name'],
    registry=REGISTRY if PROMETHEUS_AVAILABLE else None
)

gpu_memory_bytes = Gauge(
    'gpu_memory_bytes',
    'GPU memory usage in bytes',
    ['gpu_id', 'type'],
    registry=REGISTRY if PROMETHEUS_AVAILABLE else None
)

# ==================== REDIS METRICS ====================
redis_operation_duration = Histogram(
    'redis_operation_duration_seconds',
    'Redis operation duration in seconds',
    ['operation'],
    registry=REGISTRY if PROMETHEUS_AVAILABLE else None
)

redis_connection_pool_size = Gauge(
    'redis_connection_pool_size',
    'Redis connection pool size',
    ['pool_type'],
    registry=REGISTRY if PROMETHEUS_AVAILABLE else None
)

redis_memory_usage = Gauge(
    'redis_memory_usage_bytes',
    'Redis memory usage in bytes',
    ['type'],
    registry=REGISTRY if PROMETHEUS_AVAILABLE else None
)

# ==================== RABBITMQ METRICS ====================
rabbitmq_connections = Gauge(
    'rabbitmq_connections',
    'Number of RabbitMQ connections',
    ['type'],
    registry=REGISTRY if PROMETHEUS_AVAILABLE else None
)

rabbitmq_queue_messages = Gauge(
    'rabbitmq_queue_messages',
    'Number of messages in RabbitMQ queues',
    ['queue', 'state'],
    registry=REGISTRY if PROMETHEUS_AVAILABLE else None
)

# ==================== S3 METRICS ====================
s3_operation_duration = Histogram(
    's3_operation_duration_seconds',
    'S3 operation duration in seconds',
    ['operation', 'bucket'],
    registry=REGISTRY if PROMETHEUS_AVAILABLE else None
)

s3_operation_total = Counter(
    's3_operation_total',
    'Total number of S3 operations',
    ['operation', 'bucket', 'status'],
    registry=REGISTRY if PROMETHEUS_AVAILABLE else None
)

# ==================== WORKER METRICS ====================
worker_active = Gauge(
    'worker_active',
    'Number of active workers',
    ['worker_type', 'hostname'],
    registry=REGISTRY if PROMETHEUS_AVAILABLE else None
)

worker_tasks_active = Gauge(
    'worker_tasks_active',
    'Number of active tasks per worker',
    ['worker_id', 'worker_type'],
    registry=REGISTRY if PROMETHEUS_AVAILABLE else None
)

worker_memory_usage = Gauge(
    'worker_memory_usage_bytes',
    'Worker memory usage in bytes',
    ['worker_id'],
    registry=REGISTRY if PROMETHEUS_AVAILABLE else None
)


# ==================== HELPER FUNCTIONS ====================
def record_task_start(task_name: str, parser_type: str, queue: str):
    """Record task start metrics."""
    try:
        task_queue_depth.labels(queue=queue, status="active").inc()
        # Only log actual task starts, not monitoring operations
        logger.info(f"Task started: {task_name} on {queue}")
    except Exception as e:
        logger.error(f"Failed to record task start metrics: {e}")


def record_task_completion(task_name: str, parser_type: str, queue: str, 
                         duration: float, status: str):
    """Record task completion metrics."""
    try:
        task_duration.labels(
            task_name=task_name, 
            parser_type=parser_type, 
            queue=queue, 
            status=status
        ).observe(duration)
        
        task_total.labels(
            task_name=task_name, 
            parser_type=parser_type, 
            queue=queue, 
            status=status
        ).inc()
        
        task_queue_depth.labels(queue=queue, status="active").dec()
        
        # Log task completions as they are important
        logger.info(f"Task completed: {task_name} ({status}) in {duration:.2f}s")
    except Exception as e:
        logger.error(f"Failed to record task completion metrics: {e}")


def record_s3_operation(operation: str, bucket: str, duration: float, 
                       status: str, size_bytes: Optional[int] = None):
    """Record S3 operation metrics."""
    try:
        s3_operation_duration.labels(operation=operation, bucket=bucket).observe(duration)
        s3_operation_total.labels(operation=operation, bucket=bucket, status=status).inc()
        
        # Only log errors or very slow operations to reduce noise
        if status == "error":
            logger.error(f"S3 operation failed: {operation} on {bucket} in {duration:.2f}s")
        elif duration > 5.0:  # Log slow operations (over 5 seconds)
            logger.warning(f"Slow S3 operation: {operation} on {bucket} took {duration:.2f}s")
        # Remove debug logging for successful fast operations
        
    except Exception as e:
        logger.error(f"Failed to record S3 operation metrics: {e}")


def record_redis_operation(operation: str, duration: float):
    """Record Redis operation metrics."""
    try:
        redis_operation_duration.labels(operation=operation).observe(duration)
        
        # Only log slow operations or errors to reduce noise
        if duration > 1.0:  # Log operations over 1 second
            logger.warning(f"Slow Redis operation: {operation} took {duration:.2f}s")
        # Remove debug logging for fast operations
        
    except Exception as e:
        logger.error(f"Failed to record Redis operation metrics: {e}")


# ==================== FASTAPI INTEGRATION ====================
def metrics_endpoint():
    """Prometheus metrics endpoint."""
    if not PROMETHEUS_AVAILABLE:
        return Response(
            content="# Prometheus packages not installed\n# Install with: poetry add prometheus-fastapi-instrumentator\n",
            media_type="text/plain"
        )
    
    try:
        metrics_data = generate_latest(REGISTRY)
        return Response(content=metrics_data, media_type=CONTENT_TYPE_LATEST)
    except Exception as e:
        logger.error(f"Error generating metrics: {e}")
        return Response(
            content=f"# Error generating metrics: {e}\n",
            media_type="text/plain",
            status_code=500
        )


def setup_fastapi_instrumentation(app):
    """Set up FastAPI instrumentation."""
    if not PROMETHEUS_AVAILABLE:
        logger.warning("Prometheus packages not available. Skipping FastAPI instrumentation.")
        return
    
    try:
        instrumentator = Instrumentator()
        instrumentator.instrument(app).expose(app, endpoint="/metrics")
        logger.info("✅ FastAPI Prometheus instrumentation enabled")
    except Exception as e:
        logger.error(f"Failed to set up FastAPI instrumentation: {e}")


# ==================== SYSTEM INFO ====================
def get_monitoring_info() -> Dict[str, Any]:
    """Get monitoring system information."""
    return {
        "prometheus_available": PROMETHEUS_AVAILABLE,
        "metrics_endpoint": "/metrics",
        "health_endpoints": [
            "/health",
            "/health/detailed", 
            "/health/redis",
            "/health/rabbitmq",
            "/health/s3",
            "/health/workers"
        ],
        "collectors_available": [
            "SystemCollector",
            "CacheCollector", 
            "QueueCollector",
            "WorkerCollector",
            "StorageCollector"
        ]
    } 
