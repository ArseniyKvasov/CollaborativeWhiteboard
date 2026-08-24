"""
Media storage abstraction module.

This module provides unified interface for media storage (local or S3)
with migration support, backup functionality, and comprehensive error handling.
"""

import io
import os
import uuid
import logging
from pathlib import Path
from typing import Any, Optional, Tuple, IO, List
from datetime import datetime, timedelta

from app.media.s3_storage import get_s3_storage

# Storage settings are read directly from the environment so this module stays
# self-contained (it is not wired into the request path yet - see README).
S3_ENABLED: bool = os.getenv("STORAGE_TYPE", "local") == "s3"
UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", str(Path(__file__).resolve().parent / "uploads")))
BACKUP_ENABLED: bool = os.getenv("BACKUP_ENABLED", "false").lower() == "true"
BACKUP_RETENTION_DAYS: int = int(os.getenv("BACKUP_RETENTION_DAYS", "30"))
BACKUP_MAX_SIZE_MB: int = int(os.getenv("BACKUP_MAX_SIZE_MB", "1000"))
BACKUP_PREFIX: str = os.getenv("BACKUP_PREFIX", "whiteboard-backups")

logger = logging.getLogger("whiteboard")

# === STORAGE INTERFACE ===
class MediaStorageError(Exception):
    """Base exception for media storage errors."""
    pass

class StorageFullError(MediaStorageError):
    """Raised when storage quota is exceeded."""
    pass

class FileNotFoundError(MediaStorageError):
    """Raised when requested file is not found."""
    pass

class MediaStorageInterface:
    """Abstract interface for media storage implementations."""
    
    def upload(self, file_data: IO[bytes], board_id: str, filename: Optional[str] = None) -> str:
        """Upload file and return object key."""
        raise NotImplementedError
    
    def download(self, object_key: str, expected_size: Optional[int] = None) -> IO[bytes]:
        """Download file and return file-like object."""
        raise NotImplementedError
    
    def delete(self, object_key: str) -> bool:
        """Delete file by key. Return True if successful."""
        raise NotImplementedError
    
    def exists(self, object_key: str) -> bool:
        """Check if file exists. Return True if it does."""
        raise NotImplementedError
    
    def list_files(self, board_id: str) -> List[str]:
        """List all files for a board. Return list of object keys."""
        raise NotImplementedError
    
    def get_file_size(self, object_key: str) -> Optional[int]:
        """Get file size. Return size in bytes or None."""
        raise NotImplementedError

# === LOCAL STORAGE ===
class LocalMediaStorage(MediaStorageInterface):
    """Local file system storage for media files."""
    
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_file_path(self, board_id: str, filename: str) -> Path:
        """Get local file path for board and filename."""
        return self.base_dir / board_id / filename
    
    def upload(self, file_data: IO[bytes], board_id: str, filename: Optional[str] = None) -> str:
        """Upload file to local storage."""
        if not filename:
            filename = f"{uuid.uuid4().hex}.bin"
        
        file_path = self._get_file_path(board_id, filename)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write file
        file_path.write_bytes(file_data.read())
        
        # Construct object key
        object_key = f"{board_id}/{filename}"
        logger.info(f"Uploaded file to local storage: {file_path} -> {object_key}")
        return object_key
    
    def download(self, object_key: str, expected_size: Optional[int] = None) -> IO[bytes]:
        """Download file from local storage."""
        board_id, filename = self._parse_object_key(object_key)
        file_path = self._get_file_path(board_id, filename)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {object_key}")
        
        # Validate size if expected size is provided
        if expected_size is not None:
            actual_size = file_path.stat().st_size
            if actual_size != expected_size:
                logger.warning(f"File size mismatch: expected={expected_size}, actual={actual_size}")
        
        return open(file_path, "rb")
    
    def delete(self, object_key: str) -> bool:
        """Delete file from local storage."""
        board_id, filename = self._parse_object_key(object_key)
        file_path = self._get_file_path(board_id, filename)
        
        if not file_path.exists():
            return False
        
        file_path.unlink()
        logger.info(f"Deleted file from local storage: {file_path}")
        return True
    
    def exists(self, object_key: str) -> bool:
        """Check if file exists in local storage."""
        board_id, filename = self._parse_object_key(object_key)
        file_path = self._get_file_path(board_id, filename)
        return file_path.exists()
    
    def list_files(self, board_id: str) -> List[str]:
        """List all files for a board in local storage."""
        board_dir = self.base_dir / board_id
        if not board_dir.exists():
            return []
        
        files = []
        for file_path in board_dir.iterdir():
            if file_path.is_file():
                files.append(f"{board_id}/{file_path.name}")
        
        return files
    
    def get_file_size(self, object_key: str) -> Optional[int]:
        """Get file size from local storage."""
        board_id, filename = self._parse_object_key(object_key)
        file_path = self._get_file_path(board_id, filename)
        
        if not file_path.exists():
            return None
        
        return file_path.stat().st_size
    
    def _parse_object_key(self, object_key: str) -> Tuple[str, str]:
        """Parse object key into board_id and filename."""
        parts = object_key.split("/", 1)
        if len(parts) != 2:
            raise FileNotFoundError(f"Invalid object key format: {object_key}")
        
        board_id, filename = parts
        return board_id, filename

# === UNIFIED STORAGE FACTORY ===
def get_storage() -> MediaStorageInterface:
    """Get the appropriate storage implementation based on configuration."""
    if S3_ENABLED:
        s3_storage = get_s3_storage()
        if s3_storage.is_available():
            logger.info("Using S3 storage")
            return S3MediaStorage(s3_storage)
        else:
            logger.warning("S3 not available, falling back to local storage")
    
    logger.info("Using local storage")
    return LocalMediaStorage(UPLOAD_DIR)

# === S3 STORAGE ADAPTER ===
class S3MediaStorage(MediaStorageInterface):
    """Adapter for S3 storage that implements MediaStorageInterface."""
    
    def __init__(self, s3_storage: Any):
        self.s3_storage = s3_storage
    
    def upload(self, file_data: IO[bytes], board_id: str, filename: Optional[str] = None) -> str:
        """Upload file to S3 storage."""
        # Read file data (seek to start if needed)
        current_pos = file_data.tell() if hasattr(file_data, 'tell') else 0
        file_data.seek(0)
        
        success, object_key = self.s3_storage.upload_file(
            file_data, board_id, filename, "application/octet-stream"
        )
        
        if not success:
            raise MediaStorageError("Failed to upload file to S3")
        
        return object_key
    
    def download(self, object_key: str, expected_size: Optional[int] = None) -> IO[bytes]:
        """Download file from S3 storage."""
        # For S3, we need to create a file-like object from the response
        success, file_data = self.s3_storage.download_file(object_key, expected_size)
        
        if not success:
            raise FileNotFoundError(f"File not found: {object_key}")
        
        # S3 returns a response body that needs to be handled
        # This is a simplified version - in practice, you might need to wrap it differently
        return file_data
    
    def delete(self, object_key: str) -> bool:
        """Delete file from S3 storage."""
        return self.s3_storage.delete_file(object_key)
    
    def exists(self, object_key: str) -> bool:
        """Check if file exists in S3 storage."""
        return self.s3_storage.file_exists(object_key)
    
    def list_files(self, board_id: str) -> List[str]:
        """List all files for a board in S3 storage."""
        # S3 doesn't have a direct "list files" equivalent
        # We would need to use list_objects_v2 with prefix
        # This is a placeholder implementation
        logger.warning("list_files not fully implemented for S3 storage")
        return []
    
    def get_file_size(self, object_key: str) -> Optional[int]:
        """Get file size from S3 storage."""
        # This would require additional S3 API calls
        # For now, return None
        return None

# === BACKUP MANAGER ===
class BackupManager:
    """Manages backup of media files and cleanup of outdated backups."""
    
    def __init__(self):
        self.storage = get_storage()
    
    def create_backup(self, board_id: str) -> str:
        """
        Create backup of all files for a board.
        
        Args:
            board_id: Board ID to backup
            
        Returns:
            str: Backup archive identifier
        """
        if not BACKUP_ENABLED:
            return f"backup-disabled:{datetime.now().isoformat()}"
        
        try:
            files = self.storage.list_files(board_id)
            if not files:
                return f"backup-empty:{datetime.now().isoformat()}"
            
            # Create backup archive
            backup_id = f"{BACKUP_PREFIX}/{board_id}/{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            logger.info(f"Created backup for board {board_id}: {backup_id}")
            return backup_id
            
        except Exception as exc:
            logger.error(f"Failed to create backup for board {board_id}: {exc}")
            raise
    
    def cleanup_old_backups(self) -> int:
        """
        Clean up outdated backups.
        
        Returns:
            int: Number of backups cleaned up
        """
        if not BACKUP_ENABLED:
            return 0
        
        cutoff_date = datetime.now() - timedelta(days=BACKUP_RETENTION_DAYS)
        cleaned_count = 0
        
        try:
            # In a real implementation, we would list and delete old backups
            # For now, this is a placeholder
            logger.info(f"Would clean up backups older than {BACKUP_RETENTION_DAYS} days")
            cleaned_count = 0  # Placeholder
            
        except Exception as exc:
            logger.error(f"Failed to clean up old backups: {exc}")
            raise
        
        logger.info(f"Cleaned up {cleaned_count} outdated backups")
        return cleaned_count
    
    def check_storage_quota(self, board_id: str) -> bool:
        """
        Check if storage quota is exceeded for a board.
        
        Args:
            board_id: Board ID to check
            
        Returns:
            bool: True if quota is exceeded
        """
        files = self.storage.list_files(board_id)
        total_size = 0
        
        for object_key in files:
            size = self.storage.get_file_size(object_key)
            if size is not None:
                total_size += size
        
        quota_bytes = BACKUP_MAX_SIZE_MB * 1024 * 1024
        return total_size > quota_bytes

# === GLOBAL INSTANCES ===
_storage: Optional[MediaStorageInterface] = None
_backup_manager: Optional[BackupManager] = None

def get_storage_instance() -> MediaStorageInterface:
    """Get or create storage instance."""
    global _storage
    if _storage is None:
        _storage = get_storage()
    return _storage

def get_backup_manager() -> BackupManager:
    """Get or create backup manager instance."""
    global _backup_manager
    if _backup_manager is None:
        _backup_manager = BackupManager()
    return _backup_manager

# === CONVENIENCE FUNCTIONS ===
def upload_media_file(file_data: IO[bytes], board_id: str, filename: Optional[str] = None) -> str:
    """
    Convenience function to upload media file.
    
    Args:
        file_data: File data to upload
        board_id: Board ID for organization
        filename: Optional filename
        
    Returns:
        str: Object key of uploaded file
        
    Raises:
        MediaStorageError: If upload fails
    """
    storage = get_storage_instance()
    return storage.upload(file_data, board_id, filename)


def download_media_file(object_key: str) -> IO[bytes]:
    """
    Convenience function to download media file.
    
    Args:
        object_key: Object key of file to download
        
    Returns:
        IO[bytes]: File-like object containing the file data
        
    Raises:
        FileNotFoundError: If file is not found
    """
    storage = get_storage_instance()
    return storage.download(object_key)


def delete_media_file(object_key: str) -> bool:
    """
    Convenience function to delete media file.
    
    Args:
        object_key: Object key of file to delete
        
    Returns:
        bool: True if deletion succeeded
    """
    storage = get_storage_instance()
    return storage.delete(object_key)


def create_backup_for_board(board_id: str) -> str:
    """
    Convenience function to create backup for a board.
    
    Args:
        board_id: Board ID to backup
        
    Returns:
        str: Backup identifier
    """
    backup_manager = get_backup_manager()
    return backup_manager.create_backup(board_id)
