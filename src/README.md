# PDF Parser Service

A microservice for parsing PDF files into structured JSON/Markdown using GPU-accelerated ML models.

## Features

- **FastAPI** application with async endpoints
- **Celery** worker with GPU and CPU queues
- **Multiple parser backends**:
  - Docling - for layout-sensitive documents
  - Marker - for scientific papers
  - Unstructured - for OCR and image-heavy documents
- **Intelligent chunking** for optimized processing
- **S3/MinIO integration** for storage
- **Docker and Kubernetes** support with GPU acceleration
- **Prometheus metrics** for monitoring

## Architecture

The service consists of two main components:

1. **API Service**: Handles requests, provides status updates, and manages the task queue
2. **Worker Service**: Processes PDFs on GPU/CPU using specialized ML models

### API Endpoints

- `POST /upload` - Submit a PDF for processing (accepts S3 URL)
- `GET /status/{task_id}` - Check processing status
- `GET /health` - Health check for Kubernetes probes

## Development Setup

### Prerequisites

- Python 3.10+
- Poetry for dependency management
- Docker and Docker Compose (optional)
- NVIDIA GPU with CUDA support (optional but recommended)

### Poetry Configuration

Configure Poetry to use your existing virtual environment:

```bash
# Configure Poetry to use in-project virtual environment
poetry config virtualenvs.in-project true

# Use current Python interpreter
poetry env use $(which python)
```

### Environment Variables

Create a `.env` file in the project root with the following variables:

```
# API Settings
APP_LOG_LEVEL=info
MAX_QUEUE_SIZE=100

# Parser Settings
PDF_CHUNK_SIZE=300
PDF_CHUNK_OVERLAP=50
OCR_ENABLED=true
OCR_FALLBACK_THRESHOLD=0.2

# Storage Settings
S3_ENDPOINT_URL=http://localhost:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
S3_REGION=us-east-1
S3_BUCKET_NAME=pdf-parser
S3_SECURE=false

# Queue Settings
CELERY_BROKER_URL=amqp://guest:guest@localhost:5672//
CELERY_RESULT_BACKEND=redis://localhost:6379/0
REDIS_URL=redis://localhost:6379/0

# GPU Settings
USE_CUDA=true
```

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/pdf-parser.git
cd pdf-parser

# Install dependencies
poetry install

# If ML dependencies need to be installed separately
# Note: These packages can be marked as optional in pyproject.toml
poetry install -E ml

# Install torch if using GPU acceleration
poetry add torch torchvision torchaudio --platform linux --python 3.10

# Run the API server
./scripts/dev.sh

# Run the worker (will auto-detect GPU)
./scripts/worker.sh
```

## Running the Service

### Start Required Services

```bash
# Start Redis and RabbitMQ
docker-compose up -d redis rabbitmq
```

### FastAPI Application

```bash
# Run the FastAPI application
cd src
poetry run python -m app.main
```

### Celery Workers

```bash
# Run CPU worker
cd src
poetry run celery -A worker.tasks worker --loglevel=info --concurrency=1 --queues=cpu

# Run GPU worker (if GPU available)
cd src
poetry run celery -A worker.tasks worker --loglevel=info --concurrency=1 --queues=gpu
```

### Celery Monitoring with Flower

```bash
# Run Flower dashboard
cd src
poetry run celery -A worker.tasks flower --port=5555
#or just
celery -A worker.tasks flower --port=5555
```

Access Flower dashboard at http://localhost:5555

### Testing Celery Tasks

```bash
# Run test script to send a task to Celery
cd src
poetry run python ../test_celery.py
```

## Docker Build

Build the Docker image with:

```bash
./scripts/build-docker.sh
```

To build and push to a registry:

```bash
REGISTRY=ghcr.io/yourusername PUSH=true ./scripts/build-docker.sh
```

## Kubernetes Deployment

Prerequisites:
- Kubernetes cluster with NVIDIA GPU support
- KEDA installed for autoscaling
- RabbitMQ and Redis instances

Deployment steps:

```bash
# Create ConfigMap and Secret (customize as needed)
kubectl apply -f k8s/configmap-env.yaml
kubectl apply -f k8s/secret.yaml

# Deploy API and Worker
kubectl apply -f k8s/deployment-api.yaml
kubectl apply -f k8s/deployment-parser.yaml

# Configure autoscaling
kubectl apply -f k8s/keda-scaler.yaml
```

## Example Usage

### API Requests

```bash
# Upload a PDF for processing
curl -X 'POST' \
  'http://localhost:8000/upload' \
  -H 'Content-Type: application/json' \
  -d '{
    "url": "s3://pdf-parser/document.pdf",
    "parser_hint": "docling"
  }'

# Check task status
curl -X 'GET' \
  'http://localhost:8000/status/5f8c7ae6-1d1e-4b9f-b7a2-9a7b54e3f1c2'
```

### Python Client

```python
import requests
import time

# API URL
API_URL = "http://localhost:8000"

# Upload a PDF (assume it's already in S3)
pdf_url = "s3://your-bucket/path/to/document.pdf"
response = requests.post(
    f"{API_URL}/upload",
    json={"url": pdf_url, "parser_hint": "docling"}
)

# Get the task ID
task_id = response.json()["task_id"]
print(f"Task ID: {task_id}")

# Check status periodically
while True:
    status_response = requests.get(f"{API_URL}/status/{task_id}")
    
    if status_response.status_code == 200:
        status_data = status_response.json()
        print(f"Status: {status_data['state']} - Progress: {status_data.get('progress', 'N/A')}%")
        
        if status_data["state"] == "succeeded":
            result_key = status_data["result_key"]
            print(f"Result available at: {result_key}")
            break
        elif status_data["state"] == "failed":
            print(f"Processing failed: {status_data.get('error', 'Unknown error')}")
            break
    
    time.sleep(5)
```

## Troubleshooting

### Common Issues

1. **Prometheus metric registration errors**
   - Fixed by adding error handling in `app/main.py`
   - During hot reload, unregisters existing metrics

2. **Missing ML dependencies**
   - Mark ML packages as optional in `pyproject.toml`
   - Install with `poetry install -E ml`
   - Separately install torch: `poetry add torch torchvision torchaudio`

3. **Celery worker not connecting**
   - Verify RabbitMQ is running: `docker-compose ps`
   - Check connection string in `.env`

4. **Parser failures**
   - Check if S3 bucket is accessible
   - Ensure PDF exists at the specified URL
   - Verify all required dependencies are installed

## Running Tests

```bash
# Run all tests
poetry run pytest

# Run specific test file
poetry run pytest tests/utils/test_splitter.py

# Run with coverage report
poetry run pytest --cov=pdf_parser
```

## License

MIT 