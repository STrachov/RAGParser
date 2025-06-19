"""
Shared Celery client configuration for the API.
This ensures the API uses the same Celery configuration as the worker.
"""
import torch
from celery import Celery
from pdf_parser.settings import get_settings
import logging # <--- ADD THIS

logger = logging.getLogger(__name__) # <--- ADD THIS

def get_celery_app() -> Celery:
    """Get a Celery app instance configured for sending tasks."""
    settings = get_settings()
    
    # Determine queue
    cuda_available = torch.cuda.is_available() # <--- ADD THIS
    use_cuda_setting = settings.USE_CUDA # <--- ADD THIS
    
    target_queue = 'gpu' if cuda_available and use_cuda_setting else 'cpu' # <--- ADD THIS
    #target_queue = 'cpu'  # FORCE CPU QUEUE FOR TESTING
    
    logger.info(f"Celery Client: CUDA available: {cuda_available}, USE_CUDA setting: {use_cuda_setting}, Target queue: {target_queue}") # <--- ADD THIS
    
    app = Celery('pdf_parser')
    
    # Configure broker and backend
    app.conf.update(
        broker_url=settings.CELERY_BROKER_URL,
        result_backend=settings.CELERY_RESULT_BACKEND,
        
        # Task routing - exactly same as worker
        task_routes={
            'worker.tasks.parse_pdf': {'queue': target_queue} # <--- USE target_queue
        },
        
        # Serialization settings - exactly same as worker
        task_serializer='msgpack',
        accept_content=['msgpack', 'json'],
        result_serializer='msgpack',
        task_compression='gzip',
        
        # Other settings
        timezone='UTC',
        enable_utc=True,
        task_track_started=True,
        task_time_limit=30 * 60,  # 30 minutes
        task_soft_time_limit=25 * 60,  # 25 minutes
        worker_prefetch_multiplier=1,
    )
    
    return app

# Create a singleton instance
# _celery_app = None # Commented out for debugging

def get_celery_client() -> Celery:
    """Get the shared Celery client instance.
    FOR DEBUGGING: Always returns a new instance.
    """
    # global _celery_app # Commented out for debugging
    # if _celery_app is None: # Commented out for debugging
    #     _celery_app = get_celery_app() # Commented out for debugging
    # return _celery_app # Commented out for debugging
    return get_celery_app() # Always return a new instance for debugging
