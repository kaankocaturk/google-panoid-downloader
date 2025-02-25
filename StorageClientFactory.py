from abc import ABC, abstractmethod
import os
import configparser
from urllib.parse import urlparse

# ---------------------------------------------------------------------
# SOLID Principle: Interface Segregation and Dependency Inversion
# We define a minimal interface that all storage clients must implement.
# Client code depends on this abstraction, not on concrete classes.
# ---------------------------------------------------------------------
class StorageClientInterface(ABC):
    @abstractmethod
    def file_exists(self, file_path: str) -> bool:
        """Check if the file (or object) exists in the storage."""
        pass

# ---------------------------------------------------------------------
# SOLID Principle: Single Responsibility
# This class is solely responsible for interacting with S3-compatible
# storage (using the MinIO Python client).
# ---------------------------------------------------------------------
class S3StorageClient(StorageClientInterface):
    def __init__(self, remote_url: str, config: dict):
        self.remote_url = remote_url
        self.config = config
        self.client = self._init_client()

    def _init_client(self):
        from minio import Minio
        endpoint = self.config.get("endpointurl")
        if not endpoint:
            raise ValueError("Missing 'endpointurl' in configuration.")
        access_key = self.config.get("access_key_id")
        secret_key = self.config.get("secret_access_key")
        if not access_key or not secret_key:
            raise ValueError("Missing S3 credentials in configuration.")
        secure = not endpoint.startswith("http://")
        return Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )

    def file_exists(self, file_path: str) -> bool:
        # SOLID: Liskov Substitution is achieved by implementing file_exists
        # in a way that client code can use any StorageClientInterface instance.
        from minio.error import S3Error
        parsed = urlparse(self.remote_url)
        bucket = parsed.netloc  # Bucket name is extracted from the URL
        key = file_path.lstrip('/')  # Ensure we have a valid object key
        try:
            self.client.stat_object(bucket, key)
            return True
        except S3Error as err:
            if err.code == 'NoSuchKey':
                return False
            else:
                raise

# ---------------------------------------------------------------------
# SOLID Principle: Single Responsibility
# This class handles local file system operations.
# ---------------------------------------------------------------------
class LocalStorageClient(StorageClientInterface):
    def __init__(self, base_path: str):
        self.base_path = base_path

    def file_exists(self, file_path: str) -> bool:
        full_path = os.path.join(self.base_path, file_path)
        return os.path.exists(full_path)

# ---------------------------------------------------------------------
# SOLID Principle: Open/Closed
# The factory can be extended to support new storage systems without modifying
# the client code that uses the interface.
# ---------------------------------------------------------------------
class StorageClientFactory:
    @staticmethod
    def create_storage_client(repo_path: str, remote_name: str) -> StorageClientInterface:
        config = StorageClientFactory._get_dvc_remote_config(repo_path, remote_name)
        remote_url = config.get("url", "")
        if remote_url.startswith("s3://"):
            return S3StorageClient(remote_url, config)
        elif remote_url.startswith("file://") or os.path.exists(remote_url):
            # For local storage, remove file:// if present
            base_path = remote_url.replace("file://", "")
            return LocalStorageClient(base_path)
        else:
            raise ValueError("Unsupported or unknown storage type.")

    @staticmethod
    def _get_dvc_remote_config(repo_path: str, remote_name: str) -> dict:
        """
        Reads the DVC configuration file (.dvc/config) and returns the configuration
        for the specified remote.
        """
        config_path = os.path.join(repo_path, ".dvc", "config")
        config_parser = configparser.ConfigParser()
        config_parser.read(config_path)
        section = f'remote "{remote_name}"'
        if section not in config_parser:
            raise ValueError(f"Remote '{remote_name}' not found in DVC config.")
        return dict(config_parser[section])

# ---------------------------------------------------------------------
# Client Code
# Note how the client code depends only on the StorageClientInterface abstraction.
# This is an application of Dependency Inversion, allowing us to switch storage types
# without affecting this part of the code.
# ---------------------------------------------------------------------
if __name__ == "__main__":
    repo_path = "/path/to/your/dvc_repo"  # Adjust to your local DVC repository path
    remote_name = "minio"                # The remote name as specified in your .dvc/config

    # The factory determines the correct storage client to use.
    client = StorageClientFactory.create_storage_client(repo_path, remote_name)
    
    # For S3, file_path should be the object key relative to the bucket.
    # For local storage, it's a relative file system path.
    file_key = "md5/23/37abcdef1234567890"  # Example file key

    if client.file_exists(file_key):
        print("File exists in the remote storage.")
    else:
        print("File does not exist in the remote storage.")
