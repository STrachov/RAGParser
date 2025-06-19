import io
import json
import re
from functools import lru_cache
from typing import Dict, List, Optional, Union, Any, BinaryIO

import boto3
from botocore.exceptions import ClientError
import logging

from pdf_parser.settings import get_settings

logger = logging.getLogger(__name__)


class S3Client:
    """Client for interacting with S3/MinIO storage."""
    
    def __init__(
        self,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        region: str,
        bucket_name: str,
        secure: bool = True,
    ):
        """Initialize the S3 client.
        
        Args:
            endpoint_url: S3 endpoint URL
            access_key: S3 access key
            secret_key: S3 secret key
            region: S3 region
            bucket_name: S3 bucket name
            secure: Whether to use HTTPS
        """
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.bucket_name = bucket_name
        self.secure = secure
        
        # Initialize S3 client
        self.s3 = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
            use_ssl=self.secure,
        )
    
    def bucket_exists(self) -> bool:
        """Check if the bucket exists.
        
        Returns:
            True if the bucket exists, False otherwise
        """
        try:
            self.s3.head_bucket(Bucket=self.bucket_name)
            return True
        except ClientError as e:
            logger.error(f"Error checking bucket: {str(e)}")
            return False
    
    def create_bucket_if_not_exists(self) -> None:
        """Create the bucket if it doesn't exist."""
        try:
            self.s3.head_bucket(Bucket=self.bucket_name)
        except ClientError:
            try:
                # For most regions, you need to specify a LocationConstraint
                if self.region != "us-east-1":
                    self.s3.create_bucket(
                        Bucket=self.bucket_name,
                        CreateBucketConfiguration={"LocationConstraint": self.region},
                    )
                else:
                    # US East (N. Virginia) uses a special case
                    self.s3.create_bucket(Bucket=self.bucket_name)
                logger.info(f"Created bucket: {self.bucket_name}")
            except ClientError as e:
                logger.error(f"Error creating bucket: {str(e)}")
                raise
    
    def upload_file(self, local_path: str, s3_key: str) -> str:
        """Upload a file to S3.
        
        Args:
            local_path: Path to local file
            s3_key: S3 key to upload to
            
        Returns:
            S3 URL of the uploaded file
        """
        try:
            self.s3.upload_file(local_path, self.bucket_name, s3_key)
            return f"s3://{self.bucket_name}/{s3_key}"
        except ClientError as e:
            logger.error(f"Error uploading file: {str(e)}")
            raise
    
    def upload_fileobj(self, fileobj: BinaryIO, s3_key: str) -> str:
        """Upload a file-like object to S3.
        
        Args:
            fileobj: File-like object to upload
            s3_key: S3 key to upload to
            
        Returns:
            S3 URL of the uploaded file
        """
        try:
            self.s3.upload_fileobj(fileobj, self.bucket_name, s3_key)
            return f"s3://{self.bucket_name}/{s3_key}"
        except ClientError as e:
            logger.error(f"Error uploading file object: {str(e)}")
            raise
    
    def upload_bytes(self, data: bytes, s3_key: str) -> str:
        """Upload bytes to S3.
        
        Args:
            data: Bytes to upload
            s3_key: S3 key to upload to
            
        Returns:
            S3 URL of the uploaded file
        """
        try:
            self.s3.put_object(Body=data, Bucket=self.bucket_name, Key=s3_key)
            return f"s3://{self.bucket_name}/{s3_key}"
        except ClientError as e:
            logger.error(f"Error uploading bytes: {str(e)}")
            raise
    
    def upload_text(self, text: str, s3_key: str) -> str:
        """Upload text to S3.
        
        Args:
            text: Text to upload
            s3_key: S3 key to upload to
            
        Returns:
            S3 URL of the uploaded file
        """
        return self.upload_bytes(text.encode("utf-8"), s3_key)
    
    def upload_json(self, data: Union[Dict, List], s3_key: str) -> str:
        """Upload JSON data to S3.
        
        Args:
            data: JSON-serializable data to upload
            s3_key: S3 key to upload to
            
        Returns:
            S3 URL of the uploaded file
        """
        json_str = json.dumps(data, ensure_ascii=False)
        return self.upload_text(json_str, s3_key)
    
    def download_file(self, s3_key: str, local_path: str) -> None:
        """Download a file from S3.
        
        Args:
            s3_key: S3 key to download
            local_path: Path to save the file to
        """
        try:
            self.s3.download_file(self.bucket_name, s3_key, local_path)
        except ClientError as e:
            logger.error(f"Error downloading file: {str(e)}")
            raise
    
    def download_fileobj(self, s3_key: str) -> BinaryIO:
        """Download a file from S3 as a file-like object.
        
        Args:
            s3_key: S3 key to download
            
        Returns:
            File-like object containing the file data
        """
        try:
            buffer = io.BytesIO()
            self.s3.download_fileobj(self.bucket_name, s3_key, buffer)
            buffer.seek(0)
            return buffer
        except ClientError as e:
            logger.error(f"Error downloading file object: {str(e)}")
            raise
    
    def get_object(self, s3_key: str) -> dict:
        """Get an S3 object.
        
        Args:
            s3_key: S3 key to get
            
        Returns:
            S3 object response
        """
        try:
            return self.s3.get_object(Bucket=self.bucket_name, Key=s3_key)
        except ClientError as e:
            logger.error(f"Error getting object: {str(e)}")
            raise
    
    def get_object_bytes(self, s3_key: str) -> bytes:
        """Get an S3 object as bytes.
        
        Args:
            s3_key: S3 key to get
            
        Returns:
            Object data as bytes
        """
        try:
            response = self.get_object(s3_key)
            return response["Body"].read()
        except ClientError as e:
            logger.error(f"Error getting object bytes: {str(e)}")
            raise
    
    def get_object_text(self, s3_key: str) -> str:
        """Get an S3 object as text.
        
        Args:
            s3_key: S3 key to get
            
        Returns:
            Object data as text
        """
        return self.get_object_bytes(s3_key).decode("utf-8")
    
    def get_object_json(self, s3_key: str) -> Union[Dict, List]:
        """Get an S3 object as JSON.
        
        Args:
            s3_key: S3 key to get
            
        Returns:
            Parsed JSON data
        """
        return json.loads(self.get_object_text(s3_key))
    
    def list_objects(self, prefix: str = "", max_keys: int = 1000) -> List[Dict]:
        """List objects in the bucket.
        
        Args:
            prefix: Prefix to filter by
            max_keys: Maximum number of keys to return
            
        Returns:
            List of object metadata
        """
        try:
            response = self.s3.list_objects_v2(
                Bucket=self.bucket_name, Prefix=prefix, MaxKeys=max_keys
            )
            return response.get("Contents", [])
        except ClientError as e:
            logger.error(f"Error listing objects: {str(e)}")
            raise
    
    def delete_object(self, s3_key: str) -> None:
        """Delete an object from S3.
        
        Args:
            s3_key: S3 key to delete
        """
        try:
            self.s3.delete_object(Bucket=self.bucket_name, Key=s3_key)
        except ClientError as e:
            logger.error(f"Error deleting object: {str(e)}")
            raise
    
    def download_text(self, s3_key: str) -> str:
        """Download text from S3 (alias for get_object_text).
        
        Args:
            s3_key: S3 key to download
            
        Returns:
            Object data as text
        """
        return self.get_object_text(s3_key)
    
    def delete_file(self, s3_key: str) -> None:
        """Delete a file from S3 (alias for delete_object).
        
        Args:
            s3_key: S3 key to delete
        """
        return self.delete_object(s3_key)


@lru_cache()
def get_s3_client() -> S3Client:
    """Get a cached S3 client instance.
    
    Returns:
        S3Client instance
    """
    settings = get_settings()
    
    # For HTTPS endpoints, always use secure=True regardless of settings
    secure = settings.S3_ENDPOINT_URL.startswith('https://') or settings.S3_SECURE
    
    client = S3Client(
        endpoint_url=settings.S3_ENDPOINT_URL,
        access_key=settings.S3_ACCESS_KEY,
        secret_key=settings.S3_SECRET_KEY,
        region=settings.S3_REGION,
        bucket_name=settings.S3_BUCKET_NAME,
        secure=secure,
    )
    
    # Don't auto-create bucket to avoid permission issues
    # Let the bucket be created manually or by the application when needed
    
    return client 


def sanitize_filename(filename: str, max_length: int = 100) -> str:
    """
    Create a URL-safe, sanitized version of a filename.
    
    Args:
        filename: Original filename
        max_length: Maximum length for the sanitized filename
        
    Returns:
        Safe filename suitable for URLs and file systems
    """
    # Remove file extension temporarily
    name, ext = filename.rsplit('.', 1) if '.' in filename else (filename, '')
    
    # Replace problematic characters with underscores
    safe_name = re.sub(r'[^\w\-_]', '_', name)
    
    # Remove multiple consecutive underscores
    safe_name = re.sub(r'_+', '_', safe_name)
    
    # Remove leading/trailing underscores
    safe_name = safe_name.strip('_')
    
    # Ensure it's not empty
    if not safe_name:
        safe_name = 'document'
    
    # Truncate if too long (leaving room for extension)
    if ext:
        max_name_length = max_length - len(ext) - 1  # -1 for the dot
        safe_name = safe_name[:max_name_length]
        return f"{safe_name}.{ext}"
    else:
        return safe_name[:max_length]


def generate_safe_s3_key(content_hash: str, content_type: str = "document", index: Optional[int] = None) -> str:
    """
    Generate a safe S3 key based on content hash.
    
    Args:
        content_hash: Hash of the content (e.g., PDF hash)
        content_type: Type of content (document, table, etc.)
        index: Optional index for multiple items
        
    Returns:
        Safe S3 key
    """
    if index is not None:
        return f"results/{content_hash}/{content_type}_{index}.json"
    else:
        return f"results/{content_hash}/{content_type}.json" 