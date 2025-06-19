# RAGParser Monitoring System

## Overview
Comprehensive monitoring system for RAGParser with minimal logging noise and efficient collection intervals.

## Recent Improvements

### Fixed Issues
1. **S3 Storage Collector**: Fixed `head_bucket` attribute error by using proper S3Client wrapper methods
2. **RabbitMQ Queue Collector**: Fixed missing queue and channel timeout issues by creating queues and reducing timeouts
3. **Prometheus Integration**: Properly installed `prometheus-fastapi-instrumentator` to eliminate mock implementations
4. **Collection Intervals**: Optimized intervals to prevent slow collection warnings:
   - System: 20s (lightweight metrics)
   - Cache: 45s (medium complexity)
   - Queue: 45s (connection overhead)
   - Worker: 60s (Celery inspection overhead)
   - Storage: 90s (S3 operations are slowest)

### Reduced Log Noise
- **S3 Operations**: Only log errors or operations >5 seconds
- **Redis Operations**: Only log operations >1 second
- **Queue Monitoring**: Suppress routine "NOT_FOUND" and "Channel closed" messages
- **GPU Detection**: Only log once when GPU monitoring is enabled/disabled
- **Connection Management**: Reduced connection cleanup verbosity

## Monitoring Components

### Collectors
- **SystemCollector**: CPU, memory, disk, GPU metrics
- **CacheCollector**: Redis performance and memory usage  
- **QueueCollector**: RabbitMQ queue depths and connections
- **WorkerCollector**: Celery worker health and task metrics
- **StorageCollector**: S3/R2 storage operations and performance

### Health Checks
- `/health` - Quick health check
- `/health/detailed` - Comprehensive system health
- `/health/redis` - Redis connectivity
- `/health/rabbitmq` - RabbitMQ broker health
- `/health/s3` - S3/R2 storage health
- `/health/workers` - Celery worker status

### Metrics Endpoint
- `/metrics` - Prometheus metrics for Grafana/alerting

## Configuration
The monitoring system starts automatically with the FastAPI app and uses optimized collection intervals to balance monitoring accuracy with system performance.

### Log Levels
- **INFO**: Important events (task starts/completions, connection establishment)
- **WARNING**: Performance issues (slow operations, queue warnings)
- **ERROR**: Actual failures requiring attention
- **DEBUG**: Minimal usage to reduce noise 