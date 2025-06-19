from typing import Optional, Literal
from pydantic_settings import BaseSettings
from pydantic import Field, field_validator
from functools import lru_cache
import os
from pathlib import Path


class AppSettings(BaseSettings):
    """Application settings loaded from environment variables"""    
    model_config = {
        "env_file": str(Path(__file__).parent.parent / ".env"),
        "env_file_encoding": "utf-8",
        "case_sensitive": True
    }
    # API settings
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000
    APP_WORKERS: int = 1
    APP_LOG_LEVEL: str = "info"
    
    # Parser settings
    DEFAULT_PARSER_BACKEND: Literal["docling", "marker", "unstructured"] = "docling"
    PDF_CHUNK_SIZE: int = 300  # In tokens
    PDF_CHUNK_OVERLAP: int = 50  # In tokens
    OCR_ENABLED: bool = True
    OCR_FALLBACK_THRESHOLD: float = 0.2  # Percentage of image-based content that triggers OCR
    MAX_PDF_SIZE_MB: int = 100  # Maximum PDF size in MB
    
    # Storage settings
    S3_ENDPOINT_URL: str = Field(..., description="S3 endpoint URL (Minio or AWS)")
    S3_ACCESS_KEY: str = Field(..., description="S3 access key")
    S3_SECRET_KEY: str = Field(..., description="S3 secret key", repr=False)
    S3_REGION: str = "us-east-1"
    S3_BUCKET_NAME: str = "pdf-parser"
    S3_SECURE: bool = True
    
    # Queue settings
    CELERY_BROKER_URL: str = Field(..., description="RabbitMQ connection string")
    CELERY_RESULT_BACKEND: str = Field(..., description="Redis connection string")
    MAX_QUEUE_SIZE: int = 100  # Maximum number of tasks in queue before returning 429
    
    # GPU settings
    USE_CUDA: bool = True
    
    # Cache settings
    CACHE_TTL: int = 172800  # 48 hours in seconds
    REDIS_URL: str = Field(..., description="Redis connection string")

    # Security settings
    API_ALLOWED_HOSTS: list[str] = Field(default_factory=lambda: ["*"])
    
    # Flower settings
    FLOWER_UNAUTHENTICATED_API: bool = True
    
    @field_validator("S3_BUCKET_NAME")
    @classmethod
    def bucket_name_must_be_valid(cls, v: str) -> str:
        if not v.isalnum() and not all(c in "-." for c in v if not c.isalnum()):
            raise ValueError("S3 bucket name must be alphanumeric with optional hyphens and dots")
        return v
 


@lru_cache()
def get_settings() -> AppSettings:
    """Get application settings from environment variables with caching"""
    return AppSettings() 