import os
import json
import time
from datetime import datetime
from typing import Optional, Literal, Dict, List, Any
from pathlib import Path

import redis
from celery import Task, signals
from celery.utils.log import get_task_logger
from loguru import logger

from pdf_parser.settings import get_settings


class EnhancedBaseTask(Task):
    """Enhanced base class for PDF parser tasks with comprehensive monitoring."""
    
    _redis_client = None
    
    @property
    def redis(self):
        """Lazy-loaded Redis client."""
        if self._redis_client is None:
            settings = get_settings()
            self._redis_client = redis.Redis.from_url(settings.REDIS_URL)
        return self._redis_client
    
    def apply_async(self, args=None, kwargs=None, task_id=None, **options):
        """Override apply_async to ensure task tracking is initialized."""
        # Call the parent apply_async first
        result = super().apply_async(args, kwargs, task_id, **options)
        
        # Initialize task tracking immediately
        try:
            self.update_task_status(
                result.id,
                "pending",
                progress=0,
                step="Task submitted to queue"
            )
            logger.info(f"Initialized tracking for task {result.id}")
        except Exception as e:
            logger.warning(f"Failed to initialize tracking for task {result.id}: {e}")
        
        return result
    
    def delay(self, *args, **kwargs):
        """Override delay to ensure task tracking is initialized."""
        return self.apply_async(args, kwargs)
    
    def update_task_status(
        self,
        task_id: str,
        state: Literal["pending", "running", "succeeded", "failed"],
        progress: Optional[int] = None,
        result_key: Optional[str] = None,
        table_keys: Optional[List[str]] = None,
        error: Optional[str] = None,
        step: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        """Update the task status in Redis with enhanced monitoring."""
        now = datetime.now()
        
        # Get existing task data to preserve creation time
        existing_data = {}
        try:
            existing_raw = self.redis.get(f"task:{task_id}")
            if existing_raw:
                existing_data = json.loads(existing_raw)
        except Exception as e:
            logger.warning(f"Could not retrieve existing task data: {e}")
        
        # Prepare status data
        status_data = {
            "state": state,
            "updated_at": now.isoformat(),
            "created_at": existing_data.get("created_at", now.isoformat()),
            "task_name": getattr(self, 'name', 'unknown'),
        }
        
        if progress is not None:
            status_data["progress"] = progress
        
        if result_key is not None:
            status_data["result_key"] = result_key
        
        if table_keys is not None:
            status_data["table_keys"] = table_keys
        
        if error is not None:
            status_data["error"] = error
        
        if step is not None:
            status_data["current_step"] = step
        
        if details is not None:
            status_data["details"] = details
        
        # Calculate runtime if transitioning from pending to running
        if state == "running" and existing_data.get("state") == "pending":
            created_at = datetime.fromisoformat(existing_data.get("created_at", now.isoformat()))
            queue_time = (now - created_at).total_seconds()
            status_data["queue_time_seconds"] = queue_time
            logger.info(f"Task {task_id} started processing after {queue_time:.2f}s in queue")
        
        # Calculate total runtime for completed tasks
        if state in ["succeeded", "failed"]:
            created_at = datetime.fromisoformat(existing_data.get("created_at", now.isoformat()))
            total_runtime = (now - created_at).total_seconds()
            status_data["total_runtime_seconds"] = total_runtime
            
            # Also track processing time (excluding queue time)
            if "queue_time_seconds" in existing_data:
                processing_time = total_runtime - existing_data["queue_time_seconds"]
                status_data["processing_time_seconds"] = processing_time
            
            logger.info(f"Task {task_id} completed with state '{state}' after {total_runtime:.2f}s total")
        
        # Store in Redis with TTL
        settings = get_settings()
        try:
            self.redis.set(
                f"task:{task_id}",
                json.dumps(status_data),
                ex=settings.CACHE_TTL
            )
            
            # Also store metadata for monitoring
            self._store_task_metadata(task_id, status_data, step, details)
            
            # Log status change
            log_message = f"Task {task_id}: {state}"
            if progress is not None:
                log_message += f" ({progress}%)"
            if step:
                log_message += f" - {step}"
            
            logger.info(log_message)
            
        except Exception as e:
            logger.error(f"Failed to update task status for {task_id}: {e}")
            raise
    
    def _store_task_metadata(
        self,
        task_id: str,
        status_data: Dict[str, Any],
        step: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """Store additional metadata for monitoring and analysis."""
        try:
            metadata = {
                "worker_name": self.request.hostname if hasattr(self, 'request') else None,
                "queue": getattr(self.request, 'delivery_info', {}).get('routing_key') if hasattr(self, 'request') else None,
                "retries": getattr(self.request, 'retries', 0) if hasattr(self, 'request') else 0,
                "last_step": step,
                "step_details": details,
                "updated_at": datetime.now().isoformat(),
            }
            
            settings = get_settings()
            self.redis.set(
                f"task_metadata:{task_id}",
                json.dumps(metadata),
                ex=settings.CACHE_TTL
            )
            
        except Exception as e:
            logger.warning(f"Failed to store task metadata for {task_id}: {e}")
    
    def log_task_progress(
        self,
        task_id: str,
        step: str,
        details: Optional[Dict[str, Any]] = None,
        progress: Optional[int] = None
    ):
        """Log detailed progress information."""
        timestamp = datetime.now().isoformat()
        
        # Store progress log entry
        log_entry = {
            "timestamp": timestamp,
            "step": step,
            "details": details or {},
            "progress": progress,
        }
        
        try:
            # Store in a list for this task
            progress_key = f"task_progress:{task_id}"
            self.redis.lpush(progress_key, json.dumps(log_entry))
            
            # Keep only the last 20 progress entries
            self.redis.ltrim(progress_key, 0, 19)
            
            # Set TTL on the progress log
            settings = get_settings()
            self.redis.expire(progress_key, settings.CACHE_TTL)
            
            # Also log to the main logger
            log_message = f"Task {task_id} - {step}"
            if progress is not None:
                log_message += f" ({progress}%)"
            if details:
                log_message += f" - {details}"
            
            logger.debug(log_message)
            
        except Exception as e:
            logger.warning(f"Failed to log progress for {task_id}: {e}")
    
    def handle_task_failure(
        self,
        task_id: str,
        exception: Exception,
        step: Optional[str] = None
    ):
        """Handle task failure with detailed error information."""
        error_details = {
            "exception_type": type(exception).__name__,
            "exception_message": str(exception),
            "failed_step": step,
            "timestamp": datetime.now().isoformat(),
        }
        
        # Store detailed error information
        try:
            error_key = f"task_error:{task_id}"
            self.redis.set(
                error_key,
                json.dumps(error_details),
                ex=get_settings().CACHE_TTL
            )
        except Exception as e:
            logger.warning(f"Failed to store error details for {task_id}: {e}")
        
        # Update task status with error
        self.update_task_status(
            task_id,
            "failed",
            error=f"{type(exception).__name__}: {str(exception)}",
            step=step,
            details=error_details
        )
        
        # Log the error
        logger.error(f"Task {task_id} failed at step '{step}': {exception}")
    
    def get_task_progress_log(self, task_id: str) -> List[Dict[str, Any]]:
        """Get the progress log for a task."""
        try:
            progress_key = f"task_progress:{task_id}"
            log_entries = self.redis.lrange(progress_key, 0, -1)
            
            return [json.loads(entry) for entry in log_entries]
            
        except Exception as e:
            logger.warning(f"Failed to get progress log for {task_id}: {e}")
            return []


# Enhanced Celery signals for monitoring
@signals.task_prerun.connect
def enhanced_task_prerun_handler(sender, task_id, task, args, kwargs, **_):
    """Enhanced handler called before task execution."""
    logger.info(f"Task {task_id} starting: {task.name}")
    
    # Store task start information
    try:
        redis_client = redis.Redis.from_url(get_settings().REDIS_URL)
        
        start_info = {
            "task_name": task.name,
            "started_at": datetime.now().isoformat(),
            "args": str(args),  # Convert to string for JSON serialization
            "kwargs": {k: str(v) for k, v in kwargs.items()},  # Convert values to strings
        }
        
        redis_client.set(
            f"task_start:{task_id}",
            json.dumps(start_info),
            ex=get_settings().CACHE_TTL
        )
        
    except Exception as e:
        logger.warning(f"Failed to store task start info for {task_id}: {e}")


@signals.task_postrun.connect
def enhanced_task_postrun_handler(sender, task_id, task, args, kwargs, retval, state, **_):
    """Enhanced handler called after task execution."""
    logger.info(f"Task {task_id} finished with state: {state}")
    
    # Store task completion information
    try:
        redis_client = redis.Redis.from_url(get_settings().REDIS_URL)
        
        completion_info = {
            "task_name": task.name,
            "completed_at": datetime.now().isoformat(),
            "final_state": state,
            "return_value": str(retval) if retval else None,
        }
        
        redis_client.set(
            f"task_completion:{task_id}",
            json.dumps(completion_info),
            ex=get_settings().CACHE_TTL
        )
        
    except Exception as e:
        logger.warning(f"Failed to store task completion info for {task_id}: {e}")


@signals.task_retry.connect
def enhanced_task_retry_handler(sender, task_id, reason, traceback, einfo, **_):
    """Handler called when task is retried."""
    logger.warning(f"Task {task_id} being retried: {reason}")
    
    # Store retry information
    try:
        redis_client = redis.Redis.from_url(get_settings().REDIS_URL)
        
        retry_info = {
            "retried_at": datetime.now().isoformat(),
            "reason": str(reason),
            "traceback": traceback,
        }
        
        # Store retry history
        retry_key = f"task_retries:{task_id}"
        redis_client.lpush(retry_key, json.dumps(retry_info))
        redis_client.ltrim(retry_key, 0, 9)  # Keep last 10 retries
        redis_client.expire(retry_key, get_settings().CACHE_TTL)
        
    except Exception as e:
        logger.warning(f"Failed to store retry info for {task_id}: {e}")


class TaskMonitoringMixin:
    """Mixin class to add monitoring capabilities to existing tasks."""
    
    def monitor_step(
        self,
        task_id: str,
        step_name: str,
        step_function,
        *args,
        progress_start: int = 0,
        progress_end: int = 100,
        **kwargs
    ):
        """Monitor execution of a task step."""
        start_time = time.time()
        
        try:
            # Log step start
            self.log_task_progress(
                task_id,
                f"Starting {step_name}",
                {"args": str(args), "kwargs": str(kwargs)},
                progress_start
            )
            
            # Execute the step
            result = step_function(*args, **kwargs)
            
            # Log step completion
            duration = time.time() - start_time
            self.log_task_progress(
                task_id,
                f"Completed {step_name}",
                {"duration_seconds": round(duration, 2)},
                progress_end
            )
            
            return result
            
        except Exception as e:
            # Log step failure
            duration = time.time() - start_time
            self.log_task_progress(
                task_id,
                f"Failed {step_name}",
                {
                    "duration_seconds": round(duration, 2),
                    "error": str(e),
                    "error_type": type(e).__name__
                }
            )
            raise
    
    def with_timeout_monitoring(
        self,
        task_id: str,
        timeout_seconds: int,
        step_function,
        *args,
        **kwargs
    ):
        """Execute a function with timeout monitoring."""
        import signal
        
        def timeout_handler(signum, frame):
            raise TimeoutError(f"Step timed out after {timeout_seconds} seconds")
        
        # Set up timeout
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout_seconds)
        
        try:
            result = step_function(*args, **kwargs)
            signal.alarm(0)  # Cancel timeout
            return result
            
        except TimeoutError as e:
            self.log_task_progress(
                task_id,
                "Step timeout",
                {"timeout_seconds": timeout_seconds}
            )
            raise
        
        except Exception as e:
            signal.alarm(0)  # Cancel timeout
            raise


def get_comprehensive_task_info(task_id: str) -> Dict[str, Any]:
    """Get comprehensive information about a task including all monitoring data."""
    redis_client = redis.Redis.from_url(get_settings().REDIS_URL)
    
    info = {}
    
    try:
        # Basic task status
        task_data = redis_client.get(f"task:{task_id}")
        if task_data:
            info["status"] = json.loads(task_data)
        
        # Task metadata
        metadata = redis_client.get(f"task_metadata:{task_id}")
        if metadata:
            info["metadata"] = json.loads(metadata)
        
        # Task start info
        start_info = redis_client.get(f"task_start:{task_id}")
        if start_info:
            info["start_info"] = json.loads(start_info)
        
        # Task completion info
        completion_info = redis_client.get(f"task_completion:{task_id}")
        if completion_info:
            info["completion_info"] = json.loads(completion_info)
        
        # Progress log
        progress_entries = redis_client.lrange(f"task_progress:{task_id}", 0, -1)
        if progress_entries:
            info["progress_log"] = [json.loads(entry) for entry in progress_entries]
        
        # Retry history
        retry_entries = redis_client.lrange(f"task_retries:{task_id}", 0, -1)
        if retry_entries:
            info["retry_history"] = [json.loads(entry) for entry in retry_entries]
        
        # Error details
        error_info = redis_client.get(f"task_error:{task_id}")
        if error_info:
            info["error_details"] = json.loads(error_info)
        
        return info
        
    except Exception as e:
        logger.error(f"Error getting comprehensive task info for {task_id}: {e}")
        return {"error": str(e)} 