"""
S3/R2 storage monitoring collector.

Monitors storage operations, latency, and capacity.
"""

import time
import asyncio
from datetime import datetime
from typing import Dict, Any

from loguru import logger

from pdf_parser.settings import get_settings
from pdf_parser.utils.storage import get_s3_client
from ..prometheus_metrics import record_s3_operation
from .base_collector import BaseCollector, CollectorMetrics


class StorageCollector(BaseCollector):
    """Collector for S3/R2 storage metrics."""
    
    def __init__(self, collection_interval: float = 60.0):  # Longer interval for storage
        super().__init__(collection_interval)
        self.settings = get_settings()
        self._s3_client = None
    
    def get_component_name(self) -> str:
        return "s3_storage"
    
    def _get_s3_client(self):
        """Get or create S3 client."""
        if self._s3_client is None:
            self._s3_client = get_s3_client()
        return self._s3_client
    
    async def collect_metrics(self) -> CollectorMetrics:
        """Collect S3/R2 storage metrics."""
        start_time = time.time()
        
        try:
            s3_client = self._get_s3_client()
            bucket_name = self.settings.S3_BUCKET_NAME
            
            # Test bucket accessibility using the S3Client wrapper method
            bucket_start = time.time()
            bucket_exists = await asyncio.get_event_loop().run_in_executor(
                None, s3_client.bucket_exists
            )
            bucket_time = (time.time() - bucket_start) * 1000
            
            if not bucket_exists:
                # Record failed operation and return early
                record_s3_operation("bucket_check", bucket_name, bucket_time / 1000, "error")
                
                return CollectorMetrics(
                    component=self.get_component_name(),
                    healthy=False,
                    last_collection=datetime.now(),
                    collection_duration_ms=round(bucket_time, 2),
                    metrics={
                        "bucket": {
                            "name": bucket_name,
                            "accessible": False,
                            "head_time_ms": round(bucket_time, 2)
                        }
                    },
                    errors={"bucket_access": f"Bucket {bucket_name} does not exist or is not accessible"}
                )
            
            # Test upload operation using S3Client wrapper
            test_key = f"monitoring/health_check_{int(time.time())}.txt"
            test_content = "Storage health check test content"
            
            upload_start = time.time()
            upload_result = await asyncio.get_event_loop().run_in_executor(
                None, s3_client.upload_text, test_content, test_key
            )
            upload_time = (time.time() - upload_start) * 1000
            
            # Test download operation using S3Client wrapper
            download_start = time.time()
            downloaded_content = await asyncio.get_event_loop().run_in_executor(
                None, s3_client.download_text, test_key
            )
            download_time = (time.time() - download_start) * 1000
            
            # Test delete operation using S3Client wrapper
            delete_start = time.time()
            await asyncio.get_event_loop().run_in_executor(
                None, s3_client.delete_file, test_key
            )
            delete_time = (time.time() - delete_start) * 1000
            
            # Record successful metrics
            record_s3_operation("bucket_check", bucket_name, bucket_time / 1000, "success")
            record_s3_operation("upload_text", bucket_name, upload_time / 1000, "success", len(test_content.encode()))
            record_s3_operation("download_text", bucket_name, download_time / 1000, "success", len(downloaded_content.encode()))
            record_s3_operation("delete_file", bucket_name, delete_time / 1000, "success")
            
            # Get bucket statistics (if possible)
            bucket_stats = await self._get_bucket_statistics(bucket_name)
            
            collection_time = (time.time() - start_time) * 1000
            
            metrics = {
                "bucket": {
                    "name": bucket_name,
                    "accessible": True,
                    "head_time_ms": round(bucket_time, 2),
                    "endpoint_url": self.settings.S3_ENDPOINT_URL
                },
                "operations": {
                    "upload_time_ms": round(upload_time, 2),
                    "download_time_ms": round(download_time, 2),
                    "delete_time_ms": round(delete_time, 2),
                    "test_success": downloaded_content == test_content,
                    "total_operation_time_ms": round(upload_time + download_time + delete_time, 2)
                },
                "performance": {
                    "avg_operation_time_ms": round((upload_time + download_time + delete_time) / 3, 2),
                    "upload_speed_kbps": round((len(test_content.encode()) / 1024) / (upload_time / 1000), 2) if upload_time > 0 else 0,
                    "download_speed_kbps": round((len(downloaded_content.encode()) / 1024) / (download_time / 1000), 2) if download_time > 0 else 0
                },
                **bucket_stats
            }
            
            # Determine health status
            healthy = (
                bucket_exists and                       # Bucket accessible
                downloaded_content == test_content and  # Operations work correctly
                upload_time < 10000 and                 # Upload under 10 seconds
                download_time < 5000 and                # Download under 5 seconds
                delete_time < 3000                      # Delete under 3 seconds
            )
            
            errors = None
            if not healthy:
                errors = {}
                if not bucket_exists:
                    errors["bucket_access"] = "Bucket not accessible"
                if downloaded_content != test_content:
                    errors["data_integrity"] = "Downloaded content doesn't match uploaded"
                if upload_time >= 10000:
                    errors["upload_performance"] = f"Slow upload: {upload_time:.2f}ms"
                if download_time >= 5000:
                    errors["download_performance"] = f"Slow download: {download_time:.2f}ms"
                if delete_time >= 3000:
                    errors["delete_performance"] = f"Slow delete: {delete_time:.2f}ms"
            
            return CollectorMetrics(
                component=self.get_component_name(),
                healthy=healthy,
                last_collection=datetime.now(),
                collection_duration_ms=round(collection_time, 2),
                metrics=metrics,
                errors=errors
            )
            
        except Exception as e:
            collection_time = (time.time() - start_time) * 1000
            logger.error(f"Error collecting storage metrics: {e}")
            
            # Record failed operation
            record_s3_operation("health_check", self.settings.S3_BUCKET_NAME, collection_time / 1000, "error")
            
            return CollectorMetrics(
                component=self.get_component_name(),
                healthy=False,
                last_collection=datetime.now(),
                collection_duration_ms=round(collection_time, 2),
                metrics={
                    "bucket": {
                        "name": self.settings.S3_BUCKET_NAME,
                        "accessible": False,
                        "endpoint_url": self.settings.S3_ENDPOINT_URL
                    }
                },
                errors={"collection_error": str(e)}
            )
    
    async def _get_bucket_statistics(self, bucket_name: str) -> Dict[str, Any]:
        """Get bucket statistics if possible."""
        try:
            s3_client = self._get_s3_client()
            
            # List objects using S3Client wrapper
            objects = await asyncio.get_event_loop().run_in_executor(
                None, s3_client.list_objects, "", 1000  # prefix, max_keys
            )
            
            total_size = sum(obj.get("Size", 0) for obj in objects)
            object_count = len(objects)
            
            return {
                "statistics": {
                    "object_count": object_count,
                    "total_size_bytes": total_size,
                    "total_size_mb": round(total_size / 1024 / 1024, 2),
                    "avg_object_size_bytes": round(total_size / object_count, 2) if object_count > 0 else 0,
                    "last_modified": max((obj.get("LastModified", "") for obj in objects), default="") if objects else ""
                }
            }
            
        except Exception as e:
            logger.debug(f"Could not get bucket statistics: {e}")
            return {
                "statistics": {
                    "object_count": "unknown",
                    "total_size_bytes": "unknown",
                    "error": str(e)
                }
            } 