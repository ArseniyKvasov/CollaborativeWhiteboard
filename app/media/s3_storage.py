"""
S3 storage module for media files.

This module provides S3-compatible storage for media files, supporting
yandex object storage and other S3-compatible services.
"""

import io
import uuid
import logging
from pathlib import Path
from typing import Optional, Tuple, IO

from PIL import Image

logger = logging.getLogger("whiteboard")

# === S3 CLIENT ===
try:
    import boto3
    from botocore.exceptions import ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    logger.warning("boto3 not available, S3 storage will not work")

# S3 settings are read straight from the environment so this module stays
# self-contained (Yandex Object Storage and any other S3-compatible endpoint).
S3_BUCKET: Optional[str] = os.getenv("MEDIA_S3_BUCKET")
S3_LOCATION: str = os.getenv("MEDIA_S3_LOCATION", "media")
S3_ENDPOINT_URL: Optional[str] = os.getenv("MEDIA_S3_ENDPOINT_URL")
S3_REGION_NAME: str = os.getenv("MEDIA_S3_REGION_NAME", "ru-central1")
S3_ACCESS_KEY_ID: Optional[str] = os.getenv("MEDIA_S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY: Optional[str] = os.getenv("MEDIA_S3_SECRET_ACCESS_KEY")
S3_UPLOAD_DIR: str = os.getenv("MEDIA_S3_UPLOAD_DIR", "uploads")

# === S3 STORAGE MANAGER ===
class S3StorageManager:
    """Manages S3 storage for media files with backup and validation."""
    
    def __init__(self):
        self.client = None
        self.bucket = None
        self.location = None
        self.upload_dir = None
        
        if BOTO3_AVAILABLE and self._is_s3_configured():
            self._initialize_client()
    
    def _is_s3_configured(self) -> bool:
        """Check if S3 is properly configured."""
        
        if not all([S3_BUCKET, S3_ENDPOINT_URL, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY]):
            return False
        
        return True
    
    def _initialize_client(self) -> None:
        """Initialize S3 client with configured settings."""
        
        try:
            endpoint_url = S3_ENDPOINT_URL
            if endpoint_url and "://" not in endpoint_url:
                endpoint_url = f"https://{endpoint_url}"
            
            self.client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                region_name=S3_REGION_NAME,
                aws_access_key_id=S3_ACCESS_KEY_ID,
                aws_secret_access_key=S3_SECRET_ACCESS_KEY,
            )
            
            self.bucket = S3_BUCKET
            self.location = S3_LOCATION
            self.upload_dir = S3_UPLOAD_DIR
            
            # Verify bucket exists and is accessible
            self.client.head_bucket(Bucket=self.bucket)
            logger.info(f"S3 storage initialized: bucket={self.bucket}, location={self.location}")
            
        except Exception as exc:
            logger.error(f"Failed to initialize S3 client: {exc}")
            self.client = None
    
    def is_available(self) -> bool:
        """Check if S3 storage is available and configured."""
        return self.client is not None
    
    def upload_file(
        self,
        file_data: IO[bytes],
        board_id: str,
        filename: Optional[str] = None,
        content_type: str = "application/octet-stream",
        metadata: Optional[dict] = None,
    ) -> Tuple[bool, Optional[str]]:
        """
        Upload file to S3 storage.
        
        Args:
            file_data: File data to upload
            board_id: Board ID for organization
            filename: Optional filename, auto-generated if not provided
            content_type: MIME type of the file
            metadata: Optional metadata to store with the file
            
        Returns:
            Tuple[bool, Optional[str]]: (success, s3_object_key or None)
        """
        if not self.is_available():
            return False, None
        
        try:
            # Generate filename if not provided
            if not filename:
                filename = f"{uuid.uuid4().hex}.bin"
            
            # Construct S3 object key
            object_key = f"{self.upload_dir}/{board_id}/{filename}"
            
            # Upload file to S3
            self.client.upload_fileobj(
                file_data,
                self.bucket,
                object_key,
                ExtraArgs={
                    "ContentType": content_type,
                    "Metadata": metadata or {},
                }
            )
            
            logger.info(f"Uploaded file to S3: bucket={self.bucket}, key={object_key}")
            return True, object_key
            
        except ClientError as exc:
            logger.error(f"S3 client error during upload: {exc}")
            return False, None
        except Exception as exc:
            logger.error(f"Unexpected error during S3 upload: {exc}")
            return False, None
    
    def download_file(
        self,
        object_key: str,
        expected_size: Optional[int] = None,
    ) -> Tuple[bool, Optional[IO[bytes]]]:
        """
        Download file from S3 storage.
        
        Args:
            object_key: S3 object key
            expected_size: Optional expected file size for validation
            
        Returns:
            Tuple[bool, Optional[IO[bytes]]]: (success, file-like object or None)
        """
        if not self.is_available():
            return False, None
        
        try:
            # Download file from S3
            response = self.client.get_object(Bucket=self.bucket, Key=object_key)
            file_data = response["Body"]
            
            # Validate size if expected size is provided
            if expected_size is not None:
                actual_size = response["ContentLength"]
                if actual_size != expected_size:
                    logger.warning(
                        f"File size mismatch: expected={expected_size}, actual={actual_size}, key={object_key}"
                    )
            
            logger.debug(f"Downloaded file from S3: bucket={self.bucket}, key={object_key}")
            return True, file_data
            
        except ClientError as exc:
            if exc.response['Error']['Code'] == 'NoSuchKey':
                logger.debug(f"File not found in S3: key={object_key}")
                return False, None
            logger.error(f"S3 client error during download: {exc}")
            return False, None
        except Exception as exc:
            logger.error(f"Unexpected error during S3 download: {exc}")
            return False, None
    
    def delete_file(self, object_key: str) -> bool:
        """
        Delete file from S3 storage.
        
        Args:
            object_key: S3 object key to delete
            
        Returns:
            bool: True if deletion succeeded
        """
        if not self.is_available():
            return False
        
        try:
            self.client.delete_object(Bucket=self.bucket, Key=object_key)
            logger.info(f"Deleted file from S3: key={object_key}")
            return True
        except ClientError as exc:
            logger.error(f"S3 client error during delete: {exc}")
            return False
        except Exception as exc:
            logger.error(f"Unexpected error during S3 delete: {exc}")
            return False
    
    def file_exists(self, object_key: str) -> bool:
        """
        Check if file exists in S3 storage.
        
        Args:
            object_key: S3 object key to check
            
        Returns:
            bool: True if file exists
        """
        if not self.is_available():
            return False
        
        try:
            self.client.head_object(Bucket=self.bucket, Key=object_key)
            return True
        except ClientError as exc:
            if exc.response['Error']['Code'] == 'NoSuchKey':
                return False
            logger.error(f"S3 client error during exists check: {exc}")
            return False
        except Exception as exc:
            logger.error(f"Unexpected error during S3 exists check: {exc}")
            return False

# === GLOBAL INSTANCE ===
_s3_storage: Optional[S3StorageManager] = None

def get_s3_storage() -> S3StorageManager:
    """Get or create S3 storage manager instance."""
    global _s3_storage
    if _s3_storage is None:
        _s3_storage = S3StorageManager()
    return _s3_storage

# === HELPER FUNCTIONS ===
def upload_to_s3(
    file_data: IO[bytes],
    board_id: str,
    filename: Optional[str] = None,
    content_type: str = "application/octet-stream",
) -> Optional[str]:
    """
    Convenience function to upload file to S3 storage.
    
    Args:
        file_data: File data to upload
        board_id: Board ID for organization
        filename: Optional filename
        content_type: MIME type of the file
        
    Returns:
        Optional[str]: S3 object key if successful, None otherwise
    """
    s3_storage = get_s3_storage()
    success, object_key = s3_storage.upload_file(
        file_data, board_id, filename, content_type
    )
    return object_key if success else None


def download_from_s3(object_key: str) -> Optional[IO[bytes]]:
    """
    Convenience function to download file from S3 storage.
    
    Args:
        object_key: S3 object key
        
    Returns:
        Optional[IO[bytes]]: File-like object if successful, None otherwise
    """
    s3_storage = get_s3_storage()
    success, file_data = s3_storage.download_file(object_key)
    return file_data if success else None
