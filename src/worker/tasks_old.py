import os
import json
import hashlib
import time
import tempfile
from pathlib import Path
from typing import Optional, Literal, Dict, List, Any, Union
from urllib.parse import urlparse

import torch
import httpx
import celery
from celery import Celery, Task, signals
from celery.utils.log import get_task_logger
from rich.progress import Progress, TextColumn, BarColumn, SpinnerColumn, TimeElapsedColumn 
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type
from prometheus_client import Counter, Histogram, Gauge

from pdf_parser.settings import get_settings
from pdf_parser.utils.storage import S3Client, get_s3_client
from pdf_parser.parsers.factory import ParserFactory
from pdf_parser.utils.splitter import TextSplitter
from pdf_parser.utils.logging_config import get_logger

# Setup logger with enhanced capabilities
logger = get_logger(__name__)

# Initialize Celery
settings = get_settings()
app = Celery("pdf_parser")
app.conf.update(
    broker_url=settings.CELERY_BROKER_URL,
    result_backend=settings.CELERY_RESULT_BACKEND,
    task_serializer="msgpack",
    accept_content=["msgpack", "json"],
    result_serializer="msgpack",
    task_compression="gzip",
    worker_prefetch_multiplier=1,  # Better for long-running tasks
    worker_max_tasks_per_child=10,  # Prevent memory leaks
    broker_connection_retry_on_startup=True,
    task_routes={
        "worker.tasks.parse_pdf": {"queue": "gpu" if torch.cuda.is_available() and settings.USE_CUDA else "cpu"}
    },
    # Reduce logging noise
    worker_disable_rate_limits=True,
    worker_log_format='[%(asctime)s: %(levelname)s/%(processName)s] %(message)s',
    worker_task_log_format='[%(asctime)s: %(levelname)s/%(processName)s][%(task_name)s(%(task_id)s)] %(message)s',
)

# Prometheus metrics
TASK_COUNT = Counter("pdf_parser_worker_task_count", "Total number of worker tasks", ["state"])
PARSE_LATENCY = Histogram("pdf_parser_worker_parse_latency_seconds", "PDF parsing latency in seconds (worker)", ["parser_backend"])
ERROR_COUNT = Counter("pdf_parser_worker_error_count", "Total number of worker errors", ["error_type"])
GPU_MEMORY = Gauge("pdf_parser_worker_gpu_memory_bytes", "GPU memory usage in bytes (worker)", ["device"])


# Removed problematic queue setup that was causing Flower issues


@signals.task_prerun.connect
def task_prerun_handler(sender, task_id, task, args, kwargs, **_):
    """Handler called before task execution."""
    TASK_COUNT.labels(state="started").inc()
    
    # Update GPU metrics if available
    if torch.cuda.is_available() and settings.USE_CUDA:
        current_device = torch.cuda.current_device()
        memory_allocated = torch.cuda.memory_allocated(current_device)
        memory_reserved = torch.cuda.memory_reserved(current_device)
        GPU_MEMORY.labels(device=f"cuda:{current_device}").set(memory_allocated)


@signals.task_failure.connect
def task_failure_handler(sender, task_id, exception, args, kwargs, traceback, einfo, **_):
    """Handler called when task execution fails."""
    TASK_COUNT.labels(state="failed").inc()
    ERROR_COUNT.labels(error_type=type(exception).__name__).inc()


@signals.task_success.connect
def task_success_handler(sender, result, **kwargs):
    """Handler called when task executes successfully."""
    TASK_COUNT.labels(state="success").inc()


class BaseTask(Task):
    """Base class for PDF parser tasks."""
    
    _redis_client = None
    _s3_client = None
    
    @property
    def redis(self):
        """Lazy-loaded Redis client."""
        import redis
        
        if self._redis_client is None:
            self._redis_client = redis.Redis.from_url(settings.REDIS_URL)
        return self._redis_client
    
    @property
    def s3(self):
        """Lazy-loaded S3 client."""
        if self._s3_client is None:
            self._s3_client = get_s3_client()
        return self._s3_client
    
    def update_task_status(
        self,
        task_id: str,
        state: Literal["pending", "running", "succeeded", "failed"],
        progress: Optional[int] = None,
        result_key: Optional[str] = None,
        table_keys: Optional[List[str]] = None,
        error: Optional[str] = None,
    ):
        """Update the task status in Redis."""
        status_data = {"state": state}
        
        if progress is not None:
            status_data["progress"] = progress
        
        if result_key is not None:
            status_data["result_key"] = result_key
        
        if table_keys is not None:
            status_data["table_keys"] = table_keys
        
        if error is not None:
            status_data["error"] = error
        
        self.redis.set(
            f"task:{task_id}",
            json.dumps(status_data),
            ex=settings.CACHE_TTL
        )

    def _download_pdf_from_url(self, pdf_url: str, local_path: Path) -> None:
        """Download PDF from any URL (HTTP/HTTPS or S3) to local file.
        
        Args:
            pdf_url: URL to download from
            local_path: Local path to save the file
        """
        parsed_url = urlparse(pdf_url)
        
        if parsed_url.scheme == "s3":
            # Handle S3 URLs
            bucket = parsed_url.netloc
            s3_key = parsed_url.path.lstrip('/')
            logger.info(f"Downloading from S3: s3://{bucket}/{s3_key}")
            
            # Create a temporary S3 client for the source bucket (RAGPilot)
            # Note: This requires proper cross-bucket access or the URL should be presigned
            source_s3 = S3Client(
                endpoint_url=settings.S3_ENDPOINT_URL,
                access_key=settings.S3_ACCESS_KEY,
                secret_key=settings.S3_SECRET_KEY,
                region=settings.S3_REGION,
                bucket_name=bucket,
                secure=settings.S3_SECURE,
            )
            source_s3.download_file(s3_key, str(local_path))
            
        elif parsed_url.scheme in ("http", "https"):
            # Handle HTTP/HTTPS URLs (including presigned URLs)
            logger.info(f"Downloading from HTTP: {pdf_url}")
            with httpx.Client(timeout=300.0, follow_redirects=True) as client:  # 5 minutes timeout for large files
            #with httpx.Client(timeout=300.0) as client:  # 5 minutes timeout for large files
                with client.stream("GET", pdf_url) as response:
                    response.raise_for_status()
                    
                    # Check if it's actually a PDF
                    content_type = response.headers.get("content-type", "")
                    if not content_type.startswith("application/pdf"):
                        logger.warning(f"Content-Type is {content_type}, expected application/pdf")
                    
                    # Download in chunks to handle large files
                    with open(local_path, "wb") as f:
                        for chunk in response.iter_bytes(chunk_size=8192):
                            f.write(chunk)
        else:
            raise ValueError(f"Unsupported URL scheme: {parsed_url.scheme}")
        
        # Verify the file was downloaded and is not empty
        if not local_path.exists() or local_path.stat().st_size == 0:
            raise ValueError(f"Failed to download PDF or file is empty")
        
        logger.info(f"Successfully downloaded PDF ({local_path.stat().st_size} bytes)")

    def _get_file_hash_from_url(self, pdf_url: str) -> str:
        """Generate a hash for the file content from URL.
        
        Args:
            pdf_url: URL of the PDF file
            
        Returns:
            SHA256 hash of the file content
        """
        try:
            # For caching, we'll use the URL itself as the hash source
            # In a production system, we might want to download a small chunk
            # to create a content-based hash, but for simplicity we'll use URL
            hash_obj = hashlib.sha256()
            hash_obj.update(pdf_url.encode())
            return hash_obj.hexdigest()
            
        except Exception as e:
            logger.error(f"Error generating file hash: {str(e)}")
            # Fallback to timestamp-based hash
            hash_obj = hashlib.sha256()
            hash_obj.update(f"{pdf_url}_{time.time()}".encode())
            return hash_obj.hexdigest()

    def _get_file_hash(self, s3_key: str) -> str:
        """Generate a hash for the file content."""
        try:
            hash_obj = hashlib.sha256()
            file_data = self.s3.get_object_bytes(s3_key)
            hash_obj.update(file_data)
            return hash_obj.hexdigest()
        except Exception as e:
            logger.error(f"Error generating file hash: {str(e)}")
            # Fallback to using the key as a hash
            hash_obj = hashlib.sha256()
            hash_obj.update(s3_key.encode())
            return hash_obj.hexdigest()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_random_exponential(multiplier=1, max=10),
        retry=retry_if_exception_type(httpx.HTTPError)
    )
    def _send_callback(
        self, callback_url: str, task_id: str, success: bool, error: Optional[str]
    ):
        """Send a callback to the provided URL."""
        try:
            payload = {
                "task_id": task_id,
                "success": success,
            }
            if not success and error:
                payload["error"] = error
                
            with httpx.Client(timeout=30.0) as client:
                response = client.post(
                    callback_url,
                    json=payload,
                    headers={"Content-Type": "application/json"}
                )
                response.raise_for_status()
                logger.info(f"Callback sent successfully to {callback_url}")
        except Exception as e:
            logger.error(f"Error sending callback: {str(e)}")


@app.task(base=BaseTask, bind=True, acks_late=True, reject_on_worker_lost=True)
def parse_pdf(
    self,
    task_id: str,
    pdf_url: str,
    parser_backend: str = "docling",
    callback_url: Optional[str] = None,
):
    """Parse a PDF file from RAGPilot and store the results in RAGParser's S3.
    
    Args:
        task_id: Unique ID for this processing task
        pdf_url: URL of the PDF to process (can be HTTP/HTTPS presigned URL or S3 URL)
        parser_backend: Parser to use for PDF processing
        callback_url: Optional URL to call when processing is complete
    
    Returns:
        Dict containing keys for the processed results in S3
    """
    start_time = time.time()
    pdf_hash = None
    temp_dir = None
    
    # Log task start with full context
    logger.info(f"🚀 Task started: {task_id} - parse_pdf({pdf_url}, {parser_backend}) callback_url={callback_url}")
    
    try:
        # Check dependencies first
        logger.info(f"🔍 Checking parser dependencies for backend: {parser_backend}")
        
        # Test parser availability
        try:
            from pdf_parser.parsers.factory import ParserFactory
            # This will raise an error if dependencies are missing
            #parser_class = ParserFactory._get_parser_class(parser_backend)
            #logger.info(f"✅ Parser {parser_backend} is available: {parser_class.__name__}")
            parsers = ParserFactory._get_parsers()
            if parser_backend not in parsers:
                available = ", ".join(parsers.keys()) if parsers else "none"
                raise ValueError(f"Parser {parser_backend} not available. Available: {available}")
            logger.info(f"✅ Parser {parser_backend} is available")
        except ImportError as e:
            logger.error(f"❌ Dependency error for Parser {parser_backend}: {e}")
            raise ValueError(f"Parser {parser_backend} dependencies not installed: {str(e)}")
        except Exception as e:
            logger.error(f"❌ Dependency error for Parser {parser_backend}: {e}")
            raise
        
        # Update task status to running
        self.update_task_status(task_id, "running", progress=0)
        logger.info(f"📊 Task progress: {task_id} - 0% - Task started, checking cache")
        
        # Parse the URL to determine how to download
        parsed_url = urlparse(pdf_url)
        logger.info(f"📄 Processing PDF from URL: {pdf_url} (scheme: {parsed_url.scheme})")
        
        # Create a content hash for the PDF (for caching)
        logger.info(f"📊 Task progress: {task_id} - 5% - Generating content hash")
        pdf_hash = self._get_file_hash_from_url(pdf_url)
        cache_key = f"pdf_cache:{pdf_hash}"
        logger.info(f"🔑 PDF hash: {pdf_hash}")
        
        # Check if we already processed this file
        cached_result = self.redis.get(cache_key)
        if cached_result:
            logger.info(f"💾 Found cached result for hash {pdf_hash}")
            cached_data = json.loads(cached_result)
            self.update_task_status(
                task_id,
                "succeeded",
                progress=100,
                result_key=cached_data["result_key"],
                table_keys=cached_data.get("table_keys"),
            )
            
            # Call the callback URL if provided
            if callback_url:
                self._send_callback(callback_url, task_id, True, None)
            
            duration = time.time() - start_time
            logger.info(f"✅ Task completed successfully: {task_id} - Duration: {duration:.2f}s - Result: {cached_data}")
            return cached_data
        
        # Create temporary directory for processing
        temp_dir = tempfile.TemporaryDirectory()
        local_pdf_path = Path(temp_dir.name) / "document.pdf"
        logger.info(f"📁 Created temp directory: {temp_dir.name}")
        
        # Download PDF from the provided URL
        logger.info(f"📊 Task progress: {task_id} - 10% - Downloading PDF from {parsed_url.scheme} source")
        try:
            self._download_pdf_from_url(pdf_url, local_pdf_path)
            file_size = local_pdf_path.stat().st_size
            logger.info(f"📥 Successfully downloaded PDF: {file_size:,} bytes")
        except Exception as e:
            logger.error(f"❌ Failed to download PDF: {str(e)}", exc_info=True)
            raise
        
        # Select appropriate parser
        logger.info(f"📊 Task progress: {task_id} - 20% - Initializing {parser_backend} parser")
        try:
            parser = ParserFactory.create_parser(parser_backend, local_pdf_path)
            logger.info(f"🔧 Created parser: {type(parser).__name__}")
        except Exception as e:
            logger.error(f"❌ Failed to create parser: {str(e)}", exc_info=True)
            raise
        
        # Parse PDF and get structured output
        logger.info(f"📊 Task progress: {task_id} - 30% - Starting PDF parsing")
        try:
            parsed_data, tables_data = parser.parse()
            logger.info(f"📖 Parsing completed: {len(parsed_data.get('content', ''))} chars, {len(tables_data) if tables_data else 0} tables")
        except Exception as e:
            logger.error(f"❌ PDF parsing failed: {str(e)}", exc_info=True)
            raise
        
        # Split the document into chunks
        logger.info(f"📊 Task progress: {task_id} - 60% - Splitting document into chunks")
        try:
            splitter = TextSplitter(
                chunk_size=settings.PDF_CHUNK_SIZE,
                chunk_overlap=settings.PDF_CHUNK_OVERLAP
            )
            chunked_data = splitter.split(parsed_data)
            logger.info(f"✂️ Document split into {len(chunked_data.get('chunks', []))} chunks")
        except Exception as e:
            logger.error(f"❌ Document splitting failed: {str(e)}", exc_info=True)
            raise
        
        # Prepare output paths in RAGParser's S3 bucket
        pdf_filename = Path(parsed_url.path).name or "document.pdf"
        result_key = f"results/{pdf_hash}/{pdf_filename}.json"
        logger.info(f"🎯 Target S3 key: {result_key}")
        
        # Upload results to S3
        logger.info(f"📊 Task progress: {task_id} - 80% - Uploading results to S3")
        try:
            self.s3.upload_json(chunked_data, result_key)
            logger.info(f"☁️ Successfully uploaded results to S3: {result_key}")
        except Exception as e:
            logger.error(f"❌ S3 upload failed: {str(e)}", exc_info=True)
            raise
        
        # Process and upload tables to RAGParser's S3 bucket
        table_keys = []
        if tables_data:
            logger.info(f"📊 Task progress: {task_id} - 90% - Uploading {len(tables_data)} tables to S3")
            try:
                for i, table in enumerate(tables_data):
                    table_key = f"tables/{pdf_hash}/table_{i}.json"
                    self.s3.upload_json(table, table_key)
                    
                    # If the table has markdown representation, save it too
                    if "markdown" in table:
                        markdown_key = f"tables/{pdf_hash}/table_{i}.md"
                        self.s3.upload_text(table["markdown"], markdown_key)
                    table_keys.append(table_key)
                logger.info(f"📊 Successfully uploaded {len(table_keys)} tables")
            except Exception as e:
                logger.error(f"❌ Table upload failed: {str(e)}", exc_info=True)
                raise
        
        # Cache the result for future requests
        cache_data = {
            "result_key": result_key,
            "table_keys": table_keys
        }
        self.redis.set(cache_key, json.dumps(cache_data), ex=settings.CACHE_TTL)
        logger.info(f"💾 Cached result with key: {cache_key}")
        
        # Update task status to succeeded
        self.update_task_status(
            task_id,
            "succeeded",
            progress=100,
            result_key=result_key,
            table_keys=table_keys,
        )
        
        # Log parsing latency metric
        duration = time.time() - start_time
        PARSE_LATENCY.labels(parser_backend=parser_backend).observe(duration)
        
        # Call the callback URL if provided
        if callback_url:
            self._send_callback(callback_url, task_id, True, None)
        
        logger.info(f"✅ Task completed successfully: {task_id} - Duration: {duration:.2f}s - Result: {cache_data}")
        return cache_data
        
    except Exception as e:
        # Update task status to failed
        error_message = f"{type(e).__name__}: {str(e)}"
        self.update_task_status(task_id, "failed", error=error_message)
        
        # Enhanced error logging
        duration = time.time() - start_time
        logger.error(f"❌ Task failed: {task_id} - Duration: {duration:.2f}s - Error: {e}", exc_info=True)
        ERROR_COUNT.labels(error_type=type(e).__name__).inc()
        
        # Call the callback URL if provided
        if callback_url:
            self._send_callback(callback_url, task_id, False, error_message)
        
        raise
    
    finally:
        # Clean up temporary directory
        if temp_dir:
            temp_dir.cleanup()
            logger.info(f"🧹 Cleaned up temp directory")