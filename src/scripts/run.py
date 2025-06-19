 #!/usr/bin/env python3
"""
Script entry points for the PDF Parser service.
These are used by the poetry scripts defined in pyproject.toml.
"""
import os
import sys
import subprocess
from pathlib import Path
from pdf_parser.utils.logging_config import configure_logging
from dotenv import load_dotenv

# Load environment variables from .env file in parent folder
parent_dir = Path(__file__).parent.parent
env_path = parent_dir / '.env'
load_dotenv(dotenv_path=env_path)

def start_api():
    """Start the FastAPI development server."""
    # Get environment variables or use defaults
    host = os.environ.get("APP_HOST", "0.0.0.0")
    port = int(os.environ.get("APP_PORT", "8000"))
    workers = int(os.environ.get("APP_WORKERS", "1"))
    log_level = os.environ.get("APP_LOG_LEVEL", "debug")
    reload = os.environ.get("RELOAD", "true").lower() in ("true", "1", "yes")
    log_to_file = os.environ.get("LOG_TO_FILE", "true").lower() in ("true", "1", "yes")
    log_dir = os.environ.get("LOG_DIR", os.path.join(os.getcwd(), "logs"))
    
    # Configure logging
    configure_logging(log_level, log_to_file, log_dir)
    
    # Build command
    cmd = [
        "uvicorn", 
        "app.main:app", 
        "--host", host, 
        "--port", str(port), 
        "--workers", str(workers), 
        "--log-level", log_level
    ]
    
    if reload:
        cmd.append("--reload")
    
    # Print info
    print(f"Starting PDF Parser API on {host}:{port}")
    
    # Execute the command
    sys.exit(subprocess.call(cmd))


def start_worker():
    """Start the Celery worker."""
    # Determine queue based on GPU availability
    try:
        import torch
        has_gpu = torch.cuda.is_available()
    except ImportError:
        has_gpu = False
    
    queue = "gpu" if has_gpu else "cpu"
    
    # Get environment variables or use defaults
    concurrency = os.environ.get("CONCURRENCY", "1")
    log_level = os.environ.get("LOG_LEVEL", "info")
    log_to_file = os.environ.get("LOG_TO_FILE", "true").lower() in ("true", "1", "yes")
    log_dir = os.environ.get("LOG_DIR", os.path.join(os.getcwd(), "logs"))
    
    # Configure logging
    configure_logging(log_level, log_to_file, log_dir)
    
    # Print info about GPU detection
    if has_gpu:
        print("GPU detected, using GPU queue")
        os.environ["USE_CUDA"] = "true"
    else:
        print("No GPU detected, using CPU queue")
        os.environ["USE_CUDA"] = "false"
    
    # Change to src directory for correct imports
    src_dir = Path(__file__).parent.parent  # Go up from scripts/ to src/
    print(f"Changing working directory to: {src_dir}")
    os.chdir(src_dir)
    
    cmd = [
        "celery", 
        "-A", "worker.tasks", 
        "worker", 
        "--loglevel", log_level,
        "--concurrency", "1",  # Force single process
        "--pool", "solo",      # Use solo pool (single process, no subprocess)
        "--queues", queue,
        '--events',
        '--prefetch-multiplier=1',
        '--max-tasks-per-child=1'
    ]
    
    # Print info
    print(f"Starting PDF Parser worker on queue {queue}")
    
    # Execute the command
    sys.exit(subprocess.call(cmd))


if __name__ == "__main__":
    # This allows running the script directly for testing
    if len(sys.argv) > 1:
        if sys.argv[1] == "api":
            start_api()
        elif sys.argv[1] == "worker":
            start_worker()
        else:
            print(f"Unknown command: {sys.argv[1]}")
            print("Available commands: api, worker")
            sys.exit(1)
    else:
        print("Usage: python -m scripts.run [api|worker]")
        sys.exit(1)