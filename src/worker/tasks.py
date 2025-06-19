import os
import json
import hashlib
import time
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional, Literal, List, Dict, Any
from urllib.parse import urlparse

import torch
import httpx
from celery import Celery, Task, signals
from prometheus_client import Counter, Histogram, Gauge
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception_type,
)

from pdf_parser.settings import get_settings
from pdf_parser.utils.storage import S3Client, get_s3_client
from pdf_parser.parsers.factory import ParserFactory
#from pdf_parser.utils.splitter import TextSplitter
from pdf_parser.utils.logging_config import configure_logging, get_logger

configure_logging(
    level=os.getenv("LOG_LEVEL", "INFO"),
    log_to_file=os.getenv("LOG_TO_FILE", "1") not in {"0", "false", "False"},
    log_dir=os.getenv("LOG_DIR"),
)
logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Celery configuration
# ---------------------------------------------------------------------------
settings = get_settings()
app = Celery("pdf_parser")
app.conf.update(
    broker_url=settings.CELERY_BROKER_URL,
    result_backend=settings.CELERY_RESULT_BACKEND,
    task_serializer="msgpack",
    accept_content=["msgpack", "json"],
    result_serializer="msgpack",
    task_compression="gzip",
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=10,
    
    # Broker connection settings for stability  
    broker_connection_retry_on_startup=True,
    broker_connection_retry=True,
    broker_connection_max_retries=10,
    broker_heartbeat=30,
    broker_pool_limit=10,
    worker_cancel_long_running_tasks_on_connection_loss=False,
    
    # Task timeout settings for PDF processing
    task_soft_time_limit=600,  # 10 minutes
    task_time_limit=900,       # 15 minutes
    task_acks_late=True,
    
    task_routes={
        "worker.tasks.parse_pdf": {
            "queue": "gpu" if torch.cuda.is_available() and settings.USE_CUDA else "cpu"
        }
    },
    worker_disable_rate_limits=True,
    worker_log_format='[%(asctime)s: %(levelname)s/%(processName)s] %(message)s',
    worker_task_log_format='[%(asctime)s: %(levelname)s/%(processName)s][%(task_name)s(%(task_id)s)] %(message)s',
)
# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------
TASK_COUNT = Counter("pdf_parser_worker_task_count", "Total number of worker tasks", ["state"])
PARSE_LATENCY = Histogram(
    "pdf_parser_worker_parse_latency_seconds", "PDF parsing latency in seconds (worker)", ["parser_backend"]
)
ERROR_COUNT = Counter("pdf_parser_worker_error_count", "Total number of worker errors", ["error_type"])
GPU_MEMORY = Gauge("pdf_parser_worker_gpu_memory_bytes", "GPU memory usage in bytes (worker)", ["device"])


# ---------------------------------------------------------------------------
# Celery signal handlers for metrics
# ---------------------------------------------------------------------------
@signals.task_prerun.connect
def task_prerun_handler(sender, task_id, task, args, kwargs, **_):
    """Increment counters and snapshot GPU memory at task start."""
    TASK_COUNT.labels(state="started").inc()

    if torch.cuda.is_available() and settings.USE_CUDA:
        current_device = torch.cuda.current_device()
        memory_allocated = torch.cuda.memory_allocated(current_device)
        GPU_MEMORY.labels(device=f"cuda:{current_device}").set(memory_allocated)


@signals.task_postrun.connect
def task_postrun_handler(sender, task_id, task, args, kwargs, retval, state, **_):
    """Reset GPU memory gauge so dashboards show current usage, not high‑water mark."""
    if torch.cuda.is_available() and settings.USE_CUDA:
        current_device = torch.cuda.current_device()
        GPU_MEMORY.labels(device=f"cuda:{current_device}").set(0)


@signals.task_failure.connect
def task_failure_handler(sender, task_id, exception, args, kwargs, traceback, einfo, **_):
    TASK_COUNT.labels(state="failed").inc()
    ERROR_COUNT.labels(error_type=type(exception).__name__).inc()


@signals.task_success.connect
def task_success_handler(sender, result, **kwargs):
    TASK_COUNT.labels(state="success").inc()


# ---------------------------------------------------------------------------
# Base task with helpers
# ---------------------------------------------------------------------------
class BaseTask(Task):
    _redis_client = None
    _s3_client = None

    # -------------------------- Lazy clients ------------------------------
    @property
    def redis(self):
        import redis

        if self._redis_client is None:
            self._redis_client = redis.Redis.from_url(settings.REDIS_URL)
        return self._redis_client

    @property
    def s3(self):
        if self._s3_client is None:
            self._s3_client = get_s3_client()
        return self._s3_client

    # ------------------------ Status / cache ------------------------------
    def update_task_status(
        self,
        task_id: str,
        state: Literal["waiting", "running", "completed", "failed"],
        progress: Optional[int] = None,
        result_key: Optional[str] = None,
        table_keys: Optional[List[str]] = None,
        error: Optional[str] = None,
        started_at: Optional[str] = None,
        parser_used: Optional[str] = None,
        pages_processed: Optional[int] = None,
    ) -> None:
        payload = {"state": state}
        if progress is not None:
            payload["progress"] = progress
        if result_key is not None:
            payload["result_key"] = result_key
        if table_keys is not None:
            payload["table_keys"] = table_keys
        if error is not None:
            payload["error"] = error
        if started_at is not None:
            payload["started_at"] = started_at
        if parser_used is not None:
            payload["parser_used"] = parser_used
        if pages_processed is not None:
            payload["pages_processed"] = pages_processed

        self.redis.set(f"task:{task_id}", json.dumps(payload), ex=settings.CACHE_TTL)

    # ------------------------- Helpers ------------------------------------
    def _download_pdf_from_url(self, pdf_url: str, local_path: Path) -> None:
        """Download PDF from HTTP/HTTPS or S3 and write it to *local_path*.

        Adds extra validation: ensures "%PDF" header and removes partial file on failure.
        """
        parsed = urlparse(pdf_url)

        try:
            if parsed.scheme == "s3":
                bucket = parsed.netloc
                key = parsed.path.lstrip("/")
                logger.info(f"Downloading from S3: s3://{bucket}/{key}")
                source_s3 = S3Client(
                    endpoint_url=settings.S3_ENDPOINT_URL,
                    access_key=settings.S3_ACCESS_KEY,
                    secret_key=settings.S3_SECRET_KEY,
                    region=settings.S3_REGION,
                    bucket_name=bucket,
                    secure=settings.S3_SECURE,
                )
                source_s3.download_file(key, str(local_path))

            elif parsed.scheme in {"http", "https"}:
                logger.info(f"Downloading via HTTP: {pdf_url}")
                with httpx.Client(timeout=300.0, follow_redirects=True) as client:
                    with client.stream("GET", pdf_url) as resp:
                        resp.raise_for_status()

                        ctype = resp.headers.get("content-type", "")
                        if not ctype.startswith("application/pdf") and ctype != "application/octet-stream":
                            logger.warning(
                                "Unexpected Content‑Type %s for %s – proceeding but may not be a PDF",
                                ctype,
                                pdf_url,
                            )

                        with open(local_path, "wb") as fh:
                            for chunk in resp.iter_bytes(chunk_size=65536):
                                fh.write(chunk)
            else:
                raise ValueError(f"Unsupported URL scheme: {parsed.scheme}")

            # --------------------- Post‑download validation ----------------
            if not local_path.exists() or local_path.stat().st_size == 0:
                raise ValueError("Downloaded file is empty")

            with open(local_path, "rb") as fh:
                if fh.read(4) != b"%PDF":
                    raise ValueError("File header is not PDF (%PDF)")

            logger.info(
                "Downloaded %s bytes to %s", f"{local_path.stat().st_size:,}", local_path
            )
        except Exception:
            # remove partial file to avoid confusing downstream code
            if local_path.exists():
                try:
                    local_path.unlink()
                except Exception:
                    pass
            logger.exception("Failed to download %s", pdf_url)
            raise

    def _get_file_hash_from_url(self, pdf_url: str) -> str:
        """Hash of URL *including* query component to capture presigned URL versions."""
        try:
            parsed = urlparse(pdf_url)
            url_without_fragment = parsed._replace(fragment="").geturl()
            return hashlib.sha256(url_without_fragment.encode()).hexdigest()
        except Exception:
            logger.exception("Error generating hash from URL; falling back to timestamp salt")
            return hashlib.sha256(f"{pdf_url}_{time.time()}".encode()).hexdigest()

    # ------------------------- Callback helper ---------------------------
    def _send_callback(self, callback_url: str, task_id: str, success: bool, error: Optional[str]):
        """Retryable POST callback to external service."""
        @retry(retry=retry_if_exception_type(httpx.HTTPError), wait=wait_random_exponential(multiplier=1, max=10), stop=stop_after_attempt(3))
        def _do_post():
            payload = {"task_id": task_id, "success": success}
            if not success and error:
                payload["error"] = error
            with httpx.Client(timeout=30.0) as client:
                client.post(callback_url, json=payload, headers={"Content-Type": "application/json"}).raise_for_status()
            logger.info("Callback sent to %s", callback_url)

        _do_post()


# ---------------------------------------------------------------------------
# Main task
# ---------------------------------------------------------------------------
@app.task(base=BaseTask, bind=True, acks_late=True, reject_on_worker_lost=True)
def parse_pdf(self, task_id: str, pdf_url: str, parser_backend: str = "docling", callback_url: Optional[str] = None, parse_config: Optional[Dict[str, Any]] = None):
    """Parse a PDF (via *parser_backend*), chunk it and upload results to S3."""

    start = time.time()
    started_at = datetime.utcnow().isoformat() + "Z"
    pdf_hash: Optional[str] = None
    temp_dir: Optional[tempfile.TemporaryDirectory] = None
    pages_processed = 0

    # Extract key parsing options from parse_config
    if parse_config is None:
        parse_config = {}
    
    do_ocr = parse_config.get("do_ocr", True)
    do_table_structure = parse_config.get("do_table_structure", True)
    ocr_language = parse_config.get("ocr_language", "en")

    logger.info("🚀 Task started: %s – url=%s backend=%s config=%s", task_id, pdf_url, parser_backend, parse_config)

    # -------------------------------------------------------------------
    # Wrap the whole flow so we get a single traceback path and a simple
    # try/finally for cleanup and state setting.
    # -------------------------------------------------------------------
    try:
        # Validate backend early
        available_parsers = ParserFactory._get_parsers()
        if parser_backend not in available_parsers:
            raise ValueError(
                f"Parser '{parser_backend}' not available. Available: {', '.join(available_parsers)}"
            )
        logger.info("Using parser backend: %s", parser_backend)

        # Status: running 0 % with enhanced metadata
        self.update_task_status(task_id, "running", progress=0, started_at=started_at, parser_used=parser_backend)

        # Hash & cache lookup
        pdf_hash = self._get_file_hash_from_url(pdf_url)
        cache_key = f"pdf_cache:{pdf_hash}"
        if (cached := self.redis.get(cache_key)):
            cached_data = json.loads(cached)
            self.update_task_status(
                task_id, "completed", progress=100, 
                result_key=cached_data["result_key"], 
                table_keys=cached_data.get("table_keys"),
                started_at=started_at,
                parser_used=parser_backend,
                pages_processed=cached_data.get("pages_processed", 0)
            )
            if callback_url:
                self._send_callback(callback_url, task_id, True, None)
            logger.info("Cached result for %s, returning", task_id)
            return cached_data

        # ------------------------- Download -----------------------------
        temp_dir = tempfile.TemporaryDirectory()
        local_pdf_path = Path(temp_dir.name) / "document.pdf"

        self.update_task_status(task_id, "running", progress=10, started_at=started_at, parser_used=parser_backend)
        self._download_pdf_from_url(pdf_url, local_pdf_path)

        # ------------------------- Parse --------------------------------
        self.update_task_status(task_id, "running", progress=30, started_at=started_at, parser_used=parser_backend)
        
        # Create parser with configuration
        parser_kwargs = {
            'do_ocr': do_ocr,
            'do_table_structure': do_table_structure,
            'ocr_language': ocr_language
        }
        parser = ParserFactory.create_parser(parser_backend, local_pdf_path, **parser_kwargs)
        parsed_data, tables_data = parser.parse()
        
        # Try to get page count from parsed data
        try:
            pages_processed = parsed_data.get('metadata', {}).get('pages', 0)
            if pages_processed == 0 and parsed_data.get('content'):
                pages_processed = len(parsed_data['content'])
        except (TypeError, AttributeError):
            pages_processed = 0
        
        self.update_task_status(task_id, "running", progress=60, started_at=started_at, parser_used=parser_backend, pages_processed=pages_processed)

        # ------------------------- Split --------------------------------
        #self.update_task_status(task_id, "running", progress=60)
        #splitter = TextSplitter(chunk_size=settings.PDF_CHUNK_SIZE, chunk_overlap=settings.PDF_CHUNK_OVERLAP)
        #chunked_data = splitter.split(parsed_data)

        # ------------------------- Upload -------------------------------
        # Extract original filename for metadata (but don't use in S3 key)
        original_filename = Path(urlparse(pdf_url).path).name or "document.pdf"
        
        # Use safe, predictable S3 key naming instead of original filename
        result_key = f"results/{pdf_hash}/document.json"
        
        # Enhance metadata with original filename and processing info
        if "metadata" not in parsed_data:
            parsed_data["metadata"] = {}
        
        parsed_data["metadata"]["original_filename"] = original_filename
        parsed_data["metadata"]["task_id"] = task_id
        parsed_data["metadata"]["processing_timestamp"] = started_at
        parsed_data["metadata"]["parser_backend"] = parser_backend
        parsed_data["metadata"]["parse_config"] = parse_config
        
        self.update_task_status(task_id, "running", progress=80, started_at=started_at, parser_used=parser_backend, pages_processed=pages_processed)
        self.s3.upload_json(parsed_data, result_key)

        # Tables - also use safe naming
        table_keys: List[str] = []
        if tables_data:
            for i, table in enumerate(tables_data):
                # Safe table naming: results/{hash}/tables/table_N.json
                table_key = f"results/{pdf_hash}/tables/table_{i}.json"
                table_md_key = f"results/{pdf_hash}/tables/table_{i}.md"
                
                # Add metadata to each table
                table["metadata"] = {
                    "original_filename": original_filename,
                    "task_id": task_id,
                    "table_index": i,
                    "processing_timestamp": started_at
                }
                
                self.s3.upload_json(table, table_key)
                if "markdown" in table:
                    self.s3.upload_text(table["markdown"], table_md_key)
                table_keys.append(table_key)

        # ------------------------- Cache & final state ------------------
        cache_payload = {"result_key": result_key, "table_keys": table_keys, "pages_processed": pages_processed}
        self.redis.set(cache_key, json.dumps(cache_payload), ex=settings.CACHE_TTL)

        self.update_task_status(task_id, "completed", progress=100, result_key=result_key, table_keys=table_keys, 
                               started_at=started_at, parser_used=parser_backend, pages_processed=pages_processed)
        PARSE_LATENCY.labels(parser_backend=parser_backend).observe(time.time() - start)

        if callback_url:
            self._send_callback(callback_url, task_id, True, None)

        logger.info("✅ Task %s completed in %.2fs", task_id, time.time() - start)
        return cache_payload

    except Exception as exc:
        error_msg = f"{type(exc).__name__}: {str(exc)}"
        self.update_task_status(task_id, "failed", error=error_msg, started_at=started_at, parser_used=parser_backend)
        logger.exception("Task %s failed", task_id)
        if callback_url:
            self._send_callback(callback_url, task_id, False, error_msg)
        raise

    finally:
        if temp_dir:
            temp_dir.cleanup()
            logger.debug("Cleaned up temp dir for task %s", task_id)
