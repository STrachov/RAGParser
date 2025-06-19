import time
import uuid
from typing import Dict, List, Optional, Literal, Annotated, Any
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException, Request, Depends, BackgroundTasks, Header, status, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel, Field, HttpUrl
from prometheus_client import Counter, Histogram, start_http_server, REGISTRY

from loguru import logger
import redis.asyncio as redis

# Internal imports
from pdf_parser.settings import get_settings
from pdf_parser.utils.storage import S3Client, get_s3_client
from pdf_parser.utils.queue import get_task_count

# New monitoring system
from monitoring import setup_monitoring, setup_collectors, health_checker, record_task_start

# Prometheus metrics
try:
    # Clear existing metrics to prevent duplicates during reload
    REGISTRY._collector_to_names.clear()
    REGISTRY._names_to_collectors.clear()
    
    REQUEST_COUNT = Counter("pdf_parser_request_count", "Total number of requests", ["method", "endpoint", "status"])
    REQUEST_LATENCY = Histogram("pdf_parser_request_latency_seconds", "Request latency in seconds", ["method", "endpoint"])
    PARSE_LATENCY = Histogram("pdf_parser_parse_latency_seconds", "PDF parsing latency in seconds", ["parser_backend"])
    ERROR_COUNT = Counter("pdf_parser_error_count", "Total number of errors", ["error_type"])
    QUEUE_SIZE = Histogram("pdf_parser_queue_size", "Size of the task queue", ["queue_name"])
    GPU_MEMORY = Histogram("pdf_parser_gpu_memory_bytes", "GPU memory usage in bytes", ["device"])
except ValueError as e:
    # If metrics already exist (e.g., during hot reload), log and reuse existing metrics
    logger.warning(f"Prometheus metrics already registered: {e}")
    # Get existing metrics instead of creating new ones
    REQUEST_COUNT = REGISTRY._names_to_collectors.get('pdf_parser_request_count')
    REQUEST_LATENCY = REGISTRY._names_to_collectors.get('pdf_parser_request_latency_seconds')
    PARSE_LATENCY = REGISTRY._names_to_collectors.get('pdf_parser_parse_latency_seconds')
    ERROR_COUNT = REGISTRY._names_to_collectors.get('pdf_parser_error_count')
    QUEUE_SIZE = REGISTRY._names_to_collectors.get('pdf_parser_queue_size')
    GPU_MEMORY = REGISTRY._names_to_collectors.get('pdf_parser_gpu_memory_bytes')


# Lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Set up redis connection pool
    settings = get_settings()
    app.state.redis = redis.from_url(settings.REDIS_URL)
    
    # Initialize monitoring collectors
    try:
        setup_collectors(enable_background=True)
        logger.info("✅ Monitoring collectors started")
    except Exception as e:
        logger.error(f"❌ Failed to start monitoring collectors: {e}")
    
    yield
    
    # Clean up resources
    await app.state.redis.close()


app = FastAPI(
    title="PDF Parser API",
    description="Service for parsing PDFs into structured JSON/Markdown using GPU-accelerated ML models",
    version="0.1.0",
    lifespan=lifespan,
)

# Set up comprehensive monitoring (must be early in app setup)
monitoring_config = {
    "slow_request_threshold": 2.0,
    "enable_detailed_metrics": True,
    "enable_body_logging": False  # Set to True for debugging
}
setup_monitoring(app, monitoring_config)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=get_settings().API_ALLOWED_HOSTS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add GZip compression
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Legacy monitoring routes (kept for backward compatibility)
try:
    from .monitoring_routes import router as legacy_monitoring_router
    app.include_router(legacy_monitoring_router, prefix="/legacy")
    logger.info("Legacy monitoring routes loaded at /legacy prefix")
except ImportError as e:
    logger.warning(f"Legacy monitoring routes not available: {e}")


# Middleware for metrics and logging
@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start_time = time.time()
    method = request.method
    path = request.url.path
    
    try:
        response = await call_next(request)
        status_code = response.status_code
        
        # Record metrics
        REQUEST_COUNT.labels(method=method, endpoint=path, status=status_code).inc()
        REQUEST_LATENCY.labels(method=method, endpoint=path).observe(time.time() - start_time)
        
        return response
    except Exception as e:
        # Record error metrics
        ERROR_COUNT.labels(error_type=type(e).__name__).inc()
        logger.exception(f"Request failed: {e}")
        raise


# Models
class UploadRequest(BaseModel):
    """Request model for uploading PDF documents."""
    url: HttpUrl = Field(..., description="URL to the PDF file in RAGPilot's storage (presigned URL recommended)")
    parser_hint: Optional[Literal["docling", "marker", "unstructured"]] = Field(
        "docling", description="Parser selection hint"
    )
    parse_config: Optional[Dict[str, Any]] = Field(
        None, description="Additional parsing configuration from RAGPilot"
    )
    callback_url: Optional[HttpUrl] = Field(
        None, description="Optional callback URL (for future use)"
    )

class TaskStatus(BaseModel):
    """Response model for task status."""
    state: Literal["waiting", "running", "completed", "failed"]
    progress: Optional[int] = Field(None, description="Progress percentage (0-100)", ge=0, le=100)
    result_key: Optional[str] = Field(None, description="S3 key where results are stored")
    table_keys: Optional[List[str]] = Field(None, description="S3 keys for extracted tables")
    error: Optional[str] = Field(None, description="Error message if task failed")
    # New fields for RAGPilot integration:
    started_at: Optional[str] = Field(None, description="ISO datetime string when task started")
    parser_used: Optional[str] = Field(None, description="Parser backend that was used")
    pages_processed: Optional[int] = Field(None, description="Number of pages processed")


class UploadResponse(BaseModel):
    """Response model for PDF upload requests."""
    task_id: str = Field(..., description="Unique identifier for the processing task")
    queue_position: int = Field(..., description="Current position in the processing queue")
    estimated_wait_time: Optional[int] = Field(None, description="Estimated wait time in seconds")


# Endpoints
@app.get("/health")
async def health_check():
    """
    Health check endpoint for Kubernetes probes.
    
    This is a quick health check. For detailed health information, use /health/detailed
    """
    try:
        # Use the new health checker for a quick check
        redis_health = await health_checker.check_redis_health()
        return JSONResponse(
            content={
                "status": "healthy" if redis_health.healthy else "unhealthy",
                "timestamp": datetime.now().isoformat(),
                "redis": redis_health.healthy
            },
            status_code=200 if redis_health.healthy else 503
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            content={
                "status": "unhealthy", 
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            },
            status_code=503
        )


async def validate_queue_size(settings: Annotated[get_settings, Depends()]) -> None:
    """Validate if the queue can accept more tasks."""
    try:
        queue_size = await get_task_count()
        QUEUE_SIZE.labels(queue_name="celery").observe(queue_size)
        
        if queue_size >= settings.MAX_QUEUE_SIZE:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Queue is full ({queue_size} tasks). Please try again later."
            )
    except Exception as e:
        # Log the error but don't fail the request - allow processing to continue
        logger.warning(f"Could not check queue size, proceeding anyway: {e}")
        # Still observe the metric as 0 if we can't get the real count
        QUEUE_SIZE.labels(queue_name="celery").observe(0)


@app.post("/upload", response_model=UploadResponse)
async def upload_pdf(
    request: UploadRequest,
    request_obj: Request,
    settings: Annotated[get_settings, Depends()],
    _: Annotated[None, Depends(validate_queue_size)],
    x_request_id: Optional[str] = Header(None),
) -> UploadResponse:
    """Accept a PDF URL from RAGPilot for processing.
    
    This endpoint receives a URL pointing to a PDF file (e.g. in RAGPilot's storage),
    downloads it, processes it with ML models, and stores results in RAGParser's S3 bucket.
    Processing happens asynchronously, and status can be checked at GET /status/{task_id}.
    
    Args:
        request: The upload request containing the PDF URL and optional parameters
        request_obj: FastAPI request object
        settings: Application settings
        x_request_id: Optional request ID header for tracking
        
    Returns:
        UploadResponse containing task ID and queue information
        
    Raises:
        HTTPException: If the URL is invalid, download fails, or queue is full
    """
    try:
        # Generate task ID or use request ID header if provided
        task_id = x_request_id if x_request_id else str(uuid.uuid4())
        
        # Get current queue size for response
        try:
            queue_size = await get_task_count()
        except Exception as e:
            logger.warning(f"Could not get queue size: {e}")
            queue_size = 0  # Default to 0 if we can't get the real size

        # Store task in Redis with proper error handling
        try:
            await request_obj.app.state.redis.set(
                f"task:{task_id}",
                '{"state": "waiting", "progress": 0}',
                ex=settings.CACHE_TTL
            )
        except redis.RedisError as e:
            logger.error(f"Failed to store task {task_id} in Redis: {e}")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Failed to store task information. Please try again."
            )
        
        # Queue task with Celery using shared client
        try:
            from .celery_client import get_celery_client
            celery_client = get_celery_client()
            
            # Send task using the shared client with consistent configuration
            celery_client.send_task(
                'worker.tasks.parse_pdf',
                args=[task_id, str(request.url), request.parser_hint],
                kwargs={
                    "callback_url": str(request.callback_url) if request.callback_url else None,
                    "parse_config": request.parse_config
                }
            )
        except Exception as e:
            logger.error(f"Failed to queue task {task_id}: {e}")
            # Clean up Redis entry if task queuing fails
            await request_obj.app.state.redis.delete(f"task:{task_id}")
            
            # Show more detailed error in development
            error_detail = f"Failed to queue task for processing: {str(e)}"
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=error_detail
            )
        
        # Log successful task creation
        logger.info(f"Created task {task_id} for PDF at {request.url}")
        
        return UploadResponse(
            task_id=task_id,
            queue_position=queue_size + 1,
            estimated_wait_time=queue_size * settings.AVG_PROCESSING_TIME if hasattr(settings, 'AVG_PROCESSING_TIME') and settings.AVG_PROCESSING_TIME else None
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Unexpected error in upload_pdf: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred. Please try again."
        )


@app.get("/status/{task_id}", response_model=TaskStatus)
async def get_task_status(task_id: str, request: Request):
    """Get the status of a PDF processing task."""
    # Check if task exists in Redis
    task_data = await request.app.state.redis.get(f"task:{task_id}")
    
    if not task_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task ID {task_id} not found or expired."
        )
    
    # Parse task data
    import json
    task_info = json.loads(task_data)
    
    return TaskStatus(**task_info)


if __name__ == "__main__":
    import uvicorn
    settings = get_settings()
    uvicorn.run(
        "app.main:app",
        host=settings.APP_HOST,
        port=settings.APP_PORT,
        workers=settings.APP_WORKERS,
        reload=True,
        log_level=settings.APP_LOG_LEVEL.lower(),
    ) 
