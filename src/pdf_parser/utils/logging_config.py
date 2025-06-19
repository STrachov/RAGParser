"""Logging configuration for the PDF parser."""

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

def configure_logging(level: str = "INFO", log_to_file: bool = True, log_dir: Optional[str] = None) -> None:
    """Configure logging for the application.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_to_file: Whether to log to a file
        log_dir: Directory to store log files (defaults to logs/ in current directory)
    """
    # Convert level string to logging level
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {level}")
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Clear existing handlers to avoid duplicates
    if root_logger.handlers:
        root_logger.handlers = []
    
    # Create formatter
    formatter = logging.Formatter(
        "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    
    root_logger.addHandler(console_handler)
    
    # Create file handler if enabled
    if log_to_file:
        # Set up log directory
        if log_dir is None:
            log_dir = os.path.join(os.getcwd(), "logs")
        
        # Create log directory if it doesn't exist
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        
        # Set up rotating file handler (10MB per file, max 5 files)
        log_file = os.path.join(log_dir, "pdf_parser.log")
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding="utf-8"
        )
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
        
        # Log the location of the log file
        root_logger.info(f"Logging to file: {os.path.abspath(log_file)}")
    
    # Set level for specific modules
    logging.getLogger("pdf_parser").setLevel(numeric_level)
    logging.getLogger("worker").setLevel(numeric_level)
    
    # Set lower level for boto3/botocore to reduce noise
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("s3transfer").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    
    # Filter out noisy Celery logs
    logging.getLogger("celery").setLevel(logging.INFO)
    logging.getLogger("celery.task").setLevel(numeric_level)  # Keep task-related logs at user-specified level

def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Get a configured logger instance.
    
    Args:
        name: Logger name (optional)
        
    Returns:
        Configured logger instance
    """
    return logging.getLogger(name) 