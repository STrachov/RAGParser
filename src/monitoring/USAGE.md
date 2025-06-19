# RAGParser Monitoring System - Usage Guide

## Quick Start

The monitoring system starts automatically with the RAGParser API and provides real-time insights into system health and performance.

### 1. Basic Health Check
```bash
curl http://localhost:8000/health
```
**Returns**: Quick health status (healthy/unhealthy) with Redis connectivity

### 2. Detailed System Health
```bash
curl http://localhost:8000/health/detailed
```
**Returns**: Comprehensive health report for all components (Redis, RabbitMQ, S3, Workers)

### 3. Prometheus Metrics
```bash
curl http://localhost:8000/metrics
```
**Returns**: Prometheus-formatted metrics for Grafana/alerting systems

## Available Endpoints

### Health Check Endpoints

| Endpoint | Purpose | Response Time | Use Case |
|----------|---------|---------------|----------|
| `/health` | Quick liveness check | <100ms | Kubernetes probes |
| `/health/detailed` | Full system status | 1-3s | Dashboard overview |
| `/health/redis` | Redis connectivity | <500ms | Cache troubleshooting |
| `/health/rabbitmq` | Queue broker status | <1s | Message queue issues |
| `/health/s3` | Storage connectivity | 2-5s | File upload/download issues |
| `/health/workers` | Celery worker status | 1-2s | Task processing issues |

### Metrics Endpoint

| Endpoint | Purpose | Format | Use Case |
|----------|---------|--------|----------|
| `/metrics` | System metrics | Prometheus | Grafana dashboards, alerting |

## Understanding Health Responses

### Quick Health Check (`/health`)
```json
{
  "status": "healthy",
  "timestamp": "2025-05-30T10:30:00Z",
  "redis": true
}
```

### Detailed Health Check (`/health/detailed`)
```json
{
  "status": "healthy",
  "timestamp": "2025-05-30T10:30:00Z",
  "components": {
    "redis": {
      "healthy": true,
      "response_time_ms": 2.5,
      "details": {
        "ping_successful": true,
        "memory_usage_mb": 45.2
      }
    },
    "rabbitmq": {
      "healthy": true,
      "response_time_ms": 156.3,
      "details": {
        "active_workers": 2,
        "queue_depths": {"gpu": 0, "cpu": 1}
      }
    },
    "s3": {
      "healthy": true,
      "response_time_ms": 1247.8,
      "details": {
        "bucket_accessible": true,
        "upload_test_ms": 543.2
      }
    }
  }
}
```

## Production Monitoring Setup

### 1. Prometheus + Grafana Stack

#### Docker Compose Addition
```yaml
services:
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - grafana-storage:/var/lib/grafana
```

#### Prometheus Configuration (`monitoring/prometheus.yml`)
```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'ragparser-api'
    static_configs:
      - targets: ['localhost:8000']
    metrics_path: '/metrics'
    scrape_interval: 10s
```

### 2. Health Check Monitoring

#### Kubernetes Health Probes
```yaml
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: ragparser-api
    livenessProbe:
      httpGet:
        path: /health
        port: 8000
      initialDelaySeconds: 30
      periodSeconds: 10
    readinessProbe:
      httpGet:
        path: /health
        port: 8000
      initialDelaySeconds: 5
      periodSeconds: 5
```

#### Uptime Monitoring
```bash
# Simple uptime check script
#!/bin/bash
while true; do
  if curl -f http://localhost:8000/health > /dev/null 2>&1; then
    echo "$(date): RAGParser API is healthy"
  else
    echo "$(date): RAGParser API is down!" | mail -s "ALERT" admin@yourcompany.com
  fi
  sleep 60
done
```

## Key Metrics to Monitor

### 1. System Health Indicators
- **API Response Time**: `api_request_duration_seconds`
- **Queue Depth**: `rabbitmq_queue_messages`
- **Worker Availability**: `worker_active`
- **Memory Usage**: `system_memory_bytes`
- **Storage Latency**: `s3_operation_duration_seconds`

### 2. Business Metrics
- **Task Success Rate**: `task_total{status="success"}` / `task_total`
- **Average Processing Time**: `task_duration_seconds`
- **Queue Wait Times**: Derived from queue depth and processing rate
- **Error Rate**: `task_total{status="failed"}` / `task_total`

### 3. Performance Thresholds

| Metric | Warning | Critical | Action |
|--------|---------|----------|---------|
| API Response Time | >2s | >5s | Scale API instances |
| Queue Depth | >10 | >50 | Scale workers |
| Memory Usage | >80% | >95% | Add memory/restart |
| Storage Latency | >5s | >15s | Check S3 connectivity |
| Error Rate | >5% | >20% | Investigate logs |

## Alerting Setup

### Prometheus Alerting Rules (`alerts.yml`)
```yaml
groups:
- name: ragparser
  rules:
  - alert: HighErrorRate
    expr: rate(task_total{status="failed"}[5m]) / rate(task_total[5m]) > 0.1
    for: 2m
    labels:
      severity: warning
    annotations:
      summary: "High task failure rate"
      description: "Task failure rate is {{ $value | humanizePercentage }}"

  - alert: QueueBacklog
    expr: rabbitmq_queue_messages > 20
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "Queue backlog detected"
      description: "Queue has {{ $value }} pending messages"

  - alert: APIDown
    expr: up{job="ragparser-api"} == 0
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "RAGParser API is down"
```

## Troubleshooting Guide

### Common Issues

#### 1. High Memory Usage
```bash
# Check detailed memory breakdown
curl http://localhost:8000/health/detailed | jq '.components.system.memory'

# Actions:
# - Restart workers if memory leak detected
# - Reduce worker concurrency
# - Scale horizontally
```

#### 2. S3 Connection Issues
```bash
# Test S3 connectivity
curl http://localhost:8000/health/s3

# Check metrics for slow operations
curl http://localhost:8000/metrics | grep s3_operation_duration

# Actions:
# - Verify S3 credentials
# - Check network connectivity
# - Review S3 bucket permissions
```

#### 3. Queue Backlog
```bash
# Check queue status
curl http://localhost:8000/health/rabbitmq

# Check worker availability
curl http://localhost:8000/health/workers

# Actions:
# - Scale worker instances
# - Check worker health
# - Verify queue routing
```

#### 4. High Error Rates
```bash
# Check recent task failures
curl http://localhost:8000/metrics | grep 'task_total.*failed'

# Review application logs
tail -f logs/pdf_parser.log | grep ERROR

# Actions:
# - Review failed task patterns
# - Check input validation
# - Verify external dependencies
```

## Dashboard Examples

### Grafana Dashboard Panels

#### 1. System Overview
- **API Request Rate**: `rate(api_requests_total[5m])`
- **Response Time P95**: `histogram_quantile(0.95, api_request_duration_seconds_bucket)`
- **Active Workers**: `sum(worker_active)`
- **Queue Depth**: `sum(rabbitmq_queue_messages)`

#### 2. Task Processing
- **Tasks per Second**: `rate(task_total[5m])`
- **Success Rate**: `rate(task_total{status="success"}[5m]) / rate(task_total[5m])`
- **Average Duration**: `rate(task_duration_seconds_sum[5m]) / rate(task_duration_seconds_count[5m])`

#### 3. Resource Usage
- **CPU Usage**: `system_cpu_percent`
- **Memory Usage**: `system_memory_bytes{type="used"} / system_memory_bytes{type="total"}`
- **GPU Utilization**: `gpu_utilization_percent`

## Best Practices

### 1. Monitoring Strategy
- **Use health endpoints for simple up/down checks**
- **Use metrics for detailed performance analysis**
- **Set up alerts for business-critical thresholds**
- **Review metrics regularly to understand normal patterns**

### 2. Performance Optimization
- **Monitor collection intervals vs system load**
- **Adjust alert thresholds based on actual usage patterns**
- **Use recording rules for complex metric calculations**
- **Archive old metrics to manage storage costs**

### 3. Security Considerations
- **Secure metrics endpoints in production**
- **Don't expose sensitive data in metrics labels**
- **Use authentication for Grafana dashboards**
- **Monitor access to health check endpoints**

## Integration Examples

### 1. Slack Notifications
```python
import requests
import json

def send_alert(message):
    webhook_url = "YOUR_SLACK_WEBHOOK"
    payload = {"text": f"🚨 RAGParser Alert: {message}"}
    requests.post(webhook_url, json=payload)

# Check health and alert if unhealthy
response = requests.get("http://localhost:8000/health")
if response.json()["status"] != "healthy":
    send_alert("System unhealthy - check dashboard")
```

### 2. Custom Metrics Export
```python
import requests
import csv
from datetime import datetime

def export_metrics():
    metrics = requests.get("http://localhost:8000/metrics").text
    
    with open(f"metrics_{datetime.now().strftime('%Y%m%d_%H%M')}.txt", "w") as f:
        f.write(metrics)

# Run daily
export_metrics()
```

### 3. Automated Scaling
```python
import requests

def check_and_scale():
    health = requests.get("http://localhost:8000/health/detailed").json()
    
    queue_depth = health["components"]["rabbitmq"]["details"]["total_messages"]
    active_workers = health["components"]["rabbitmq"]["details"]["active_workers"]
    
    if queue_depth > 20 and active_workers < 5:
        # Trigger scaling (implement your scaling logic)
        print("Scaling workers up...")
    elif queue_depth < 5 and active_workers > 2:
        print("Scaling workers down...")
```

## Support

For issues with the monitoring system:
1. Check the application logs: `logs/pdf_parser.log`
2. Verify all services are running: `docker-compose ps`
3. Test individual health endpoints to isolate issues
4. Review metrics for patterns before failures
5. Check resource availability (memory, disk, network) 