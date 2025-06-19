"""
Monitoring middleware for FastAPI request/response tracking and correlation.

Provides request correlation IDs, timing, and automatic metrics collection.
"""

import time
import uuid
from typing import Callable, Dict, Any

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from loguru import logger

from .prometheus_metrics import (
    api_requests_total, api_request_duration, api_active_requests,
    record_redis_operation
)


class CorrelationMiddleware(BaseHTTPMiddleware):
    """Middleware to add correlation IDs to requests for distributed tracing."""
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Generate or extract correlation ID
        correlation_id = request.headers.get("X-Correlation-ID")
        if not correlation_id:
            correlation_id = str(uuid.uuid4())
        
        # Store correlation ID in request state
        request.state.correlation_id = correlation_id
        
        # Add correlation ID to response headers
        response = await call_next(request)
        response.headers["X-Correlation-ID"] = correlation_id
        
        return response


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware for structured request logging with correlation tracking."""
    
    def __init__(self, app, enable_body_logging: bool = False):
        super().__init__(app)
        self.enable_body_logging = enable_body_logging
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.time()
        correlation_id = getattr(request.state, 'correlation_id', 'unknown')
        
        # Log request start
        request_info = {
            "correlation_id": correlation_id,
            "method": request.method,
            "url": str(request.url),
            "client_ip": request.client.host if request.client else "unknown",
            "user_agent": request.headers.get("user-agent", "unknown"),
            "content_length": request.headers.get("content-length", 0)
        }
        
        logger.info("Request started", extra=request_info)
        
        # Process request
        try:
            response = await call_next(request)
            processing_time = time.time() - start_time
            
            # Log request completion
            response_info = {
                **request_info,
                "status_code": response.status_code,
                "processing_time_ms": round(processing_time * 1000, 2),
                "response_size": response.headers.get("content-length", 0)
            }
            
            if response.status_code >= 400:
                logger.warning("Request completed with error", extra=response_info)
            else:
                logger.info("Request completed", extra=response_info)
            
            return response
            
        except Exception as e:
            processing_time = time.time() - start_time
            
            # Log request error
            error_info = {
                **request_info,
                "error": str(e),
                "processing_time_ms": round(processing_time * 1000, 2)
            }
            
            logger.error("Request failed", extra=error_info)
            raise


class PrometheusMiddleware(BaseHTTPMiddleware):
    """Middleware for automatic Prometheus metrics collection."""
    
    def __init__(self, app, enable_detailed_metrics: bool = True):
        super().__init__(app)
        self.enable_detailed_metrics = enable_detailed_metrics
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Skip metrics collection for metrics endpoint to avoid recursion
        if request.url.path == "/metrics":
            return await call_next(request)
        
        start_time = time.time()
        method = request.method
        path = request.url.path
        
        # Normalize path for metrics (remove dynamic segments)
        normalized_path = self._normalize_path(path)
        
        # Increment active requests
        api_active_requests.inc()
        
        try:
            response = await call_next(request)
            status_code = response.status_code
            
        except Exception as e:
            # Handle exceptions
            status_code = 500
            response = Response(
                content=f"Internal server error: {str(e)}",
                status_code=500
            )
        
        finally:
            # Decrement active requests
            api_active_requests.dec()
            
            # Record metrics
            processing_time = time.time() - start_time
            
            # Record request metrics
            api_requests_total.labels(
                method=method,
                endpoint=normalized_path,
                status_code=status_code
            ).inc()
            
            api_request_duration.labels(
                method=method,
                endpoint=normalized_path
            ).observe(processing_time)
        
        return response
    
    def _normalize_path(self, path: str) -> str:
        """Normalize API paths for metrics by replacing dynamic segments."""
        # Common patterns to normalize
        normalizations = [
            # Replace UUIDs with placeholder
            (r'/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '/{uuid}'),
            # Replace numeric IDs with placeholder
            (r'/\d+', '/{id}'),
            # Replace task IDs with placeholder
            (r'/task_[a-zA-Z0-9-]+', '/task_{id}'),
        ]
        
        import re
        normalized = path
        for pattern, replacement in normalizations:
            normalized = re.sub(pattern, replacement, normalized)
        
        return normalized


class PerformanceMonitoringMiddleware(BaseHTTPMiddleware):
    """Middleware for performance monitoring and alerting."""
    
    def __init__(self, app, slow_request_threshold: float = 2.0):
        super().__init__(app)
        self.slow_request_threshold = slow_request_threshold
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.time()
        correlation_id = getattr(request.state, 'correlation_id', 'unknown')
        
        response = await call_next(request)
        
        processing_time = time.time() - start_time
        
        # Check for slow requests
        if processing_time > self.slow_request_threshold:
            logger.warning(
                "Slow request detected",
                extra={
                    "correlation_id": correlation_id,
                    "method": request.method,
                    "url": str(request.url),
                    "processing_time_ms": round(processing_time * 1000, 2),
                    "threshold_ms": round(self.slow_request_threshold * 1000, 2),
                    "status_code": response.status_code
                }
            )
        
        return response


class ErrorTrackingMiddleware(BaseHTTPMiddleware):
    """Middleware for error tracking and monitoring."""
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        correlation_id = getattr(request.state, 'correlation_id', 'unknown')
        
        try:
            response = await call_next(request)
            
            # Monitor 4xx and 5xx errors
            if response.status_code >= 400:
                error_context = {
                    "correlation_id": correlation_id,
                    "method": request.method,
                    "url": str(request.url),
                    "status_code": response.status_code,
                    "client_ip": request.client.host if request.client else "unknown"
                }
                
                if response.status_code >= 500:
                    logger.error("Server error", extra=error_context)
                else:
                    logger.warning("Client error", extra=error_context)
            
            return response
            
        except Exception as e:
            # Log unhandled exceptions
            logger.exception(
                "Unhandled exception in request",
                extra={
                    "correlation_id": correlation_id,
                    "method": request.method,
                    "url": str(request.url),
                    "error": str(e),
                    "error_type": type(e).__name__
                }
            )
            
            # Re-raise the exception
            raise


# Convenience function to add all monitoring middleware
def add_monitoring_middleware(app, config: Dict[str, Any] = None):
    """Add all monitoring middleware to a FastAPI app."""
    if config is None:
        config = {}
    
    # Add middleware in reverse order (they are applied in LIFO order)
    
    # Error tracking (innermost)
    app.add_middleware(ErrorTrackingMiddleware)
    
    # Performance monitoring
    slow_threshold = config.get("slow_request_threshold", 2.0)
    app.add_middleware(PerformanceMonitoringMiddleware, slow_request_threshold=slow_threshold)
    
    # Prometheus metrics
    enable_detailed = config.get("enable_detailed_metrics", True)
    app.add_middleware(PrometheusMiddleware, enable_detailed_metrics=enable_detailed)
    
    # Request logging
    enable_body_logging = config.get("enable_body_logging", False)
    app.add_middleware(RequestLoggingMiddleware, enable_body_logging=enable_body_logging)
    
    # Correlation ID (outermost)
    app.add_middleware(CorrelationMiddleware)
    
    logger.info("Monitoring middleware added to FastAPI app") 