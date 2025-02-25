from abc import ABC, abstractmethod
import os
import configparser
from urllib.parse import urlparse

# ---------------------------------------------------------------------
# Interface (Dependency Inversion / Interface Segregation)
# Clients depend only on this minimal abstraction.
# ---------------------------------------------------------------------
class StorageClientInterface(ABC):
    @abstractmethod
    def file_exists(self, file_path: str) -> bool:
        """Check if a file or object exists in the storage."""
        pass

    @abstractmethod
    def get_file_key(self, local_file_path: str, rev: str = "HEAD") -> str:
        """
        Given a local file path that is tracked by DVC,
        return the remote file key (object key) using the DVC API.
        """
        pass

    @classmethod
    @abstractmethod
    def from_dvc_config(cls, repo_path: str, remote_name: str):
        """Factory method to create an instance from DVC configuration."""
        pass

# ---------------------------------------------------------------------
# S3 Storage Client using MinIO (Single Responsibility)
# Implements its own DVC config extraction and get_file_key.
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
        return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)

    def file_exists(self, file_path: str) -> bool:
        from minio.error import S3Error
        # Extract bucket from the remote URL (e.g., s3://bucket_name/...)
        parsed = urlparse(self.remote_url)
        bucket = parsed.netloc
        key = file_path.lstrip('/')
        try:
            self.client.stat_object(bucket, key)
            return True
        except S3Error as err:
            if err.code == 'NoSuchKey':
                return False
            else:
                raise

    def get_file_key(self, local_file_path: str, rev: str = "HEAD") -> str:
        import dvc.api
        # dvc.api.get_url returns the remote URL for the local file,
        # for example: "s3://bucket/md5/ab/abcdef1234567890..."
        remote_file_url = dvc.api.get_url(
            local_file_path, repo=self.repo_path, remote=self.remote_name, rev=rev
        )
        parsed = urlparse(remote_file_url)
        return parsed.path.lstrip('/')

    @classmethod
    def from_dvc_config(cls, repo_path: str, remote_name: str) -> "S3StorageClient":
        config = cls._get_dvc_remote_config(repo_path, remote_name)
        remote_url = config.get("url")
        if not remote_url or not remote_url.startswith("s3://"):
            raise ValueError("DVC remote URL is not S3 for this client.")
        instance = cls(remote_url, config)
        # Save the repo path and remote name for later use in dvc.api calls.
        instance.repo_path = repo_path
        instance.remote_name = remote_name
        return instance

    @staticmethod
    def _get_dvc_remote_config(repo_path: str, remote_name: str) -> dict:
        """
        Extracts and returns the DVC configuration for this remote.
        This method is custom for the S3 storage client.
        """
        config_path = os.path.join(repo_path, ".dvc", "config")
        parser = configparser.ConfigParser()
        parser.read(config_path)
        section = f'remote "{remote_name}"'
        if section not in parser:
            raise ValueError(f"Remote '{remote_name}' not found in DVC config.")
        return dict(parser[section])

# ---------------------------------------------------------------------
# Local Storage Client (Single Responsibility)
# Implements its own DVC config extraction and get_file_key.
# ---------------------------------------------------------------------
class LocalStorageClient(StorageClientInterface):
    def __init__(self, base_path: str):
        self.base_path = base_path

    def file_exists(self, file_path: str) -> bool:
        full_path = os.path.join(self.base_path, file_path)
        return os.path.exists(full_path)

    def get_file_key(self, local_file_path: str, rev: str = "HEAD") -> str:
        import dvc.api
        # For local storage, dvc.api.get_url typically returns a file:// URL.
        remote_file_url = dvc.api.get_url(
            local_file_path, repo=self.repo_path, remote=self.remote_name, rev=rev
        )
        parsed = urlparse(remote_file_url)
        return parsed.path.lstrip('/')

    @classmethod
    def from_dvc_config(cls, repo_path: str, remote_name: str) -> "LocalStorageClient":
        config = cls._get_dvc_remote_config(repo_path, remote_name)
        remote_url = config.get("url")
        if not remote_url:
            raise ValueError("No URL found in DVC config for local storage.")
        # Remove the "file://" prefix if present.
        base_path = remote_url.replace("file://", "") if remote_url.startswith("file://") else remote_url
        instance = cls(base_path)
        instance.repo_path = repo_path
        instance.remote_name = remote_name
        return instance

    @staticmethod
    def _get_dvc_remote_config(repo_path: str, remote_name: str) -> dict:
        """
        Extracts and returns the DVC configuration for local storage.
        This method is custom for the local storage client.
        """
        config_path = os.path.join(repo_path, ".dvc", "config")
        parser = configparser.ConfigParser()
        parser.read(config_path)
        section = f'remote "{remote_name}"'
        if section not in parser:
            raise ValueError(f"Remote '{remote_name}' not found in DVC config.")
        return dict(parser[section])

# ---------------------------------------------------------------------
# Factory (Open/Closed)
# Creates the correct storage client based on the DVC remote URL.
# ---------------------------------------------------------------------
class StorageClientFactory:
    @staticmethod
    def create_storage_client(repo_path: str, remote_name: str) -> StorageClientInterface:
        config_path = os.path.join(repo_path, ".dvc", "config")
        parser = configparser.ConfigParser()
        parser.read(config_path)
        section = f'remote "{remote_name}"'
        if section not in parser:
            raise ValueError(f"Remote '{remote_name}' not found in DVC config.")
        config = dict(parser[section])
        remote_url = config.get("url", "")

        if remote_url.startswith("s3://"):
            return S3StorageClient.from_dvc_config(repo_path, remote_name)
        elif remote_url.startswith("file://") or os.path.exists(remote_url):
            return LocalStorageClient.from_dvc_config(repo_path, remote_name)
        else:
            raise ValueError("Unsupported or unknown storage type.")

# ---------------------------------------------------------------------
# Client Code (Dependency Inversion)
# Uses the abstract interface without knowledge of underlying implementations.
# ---------------------------------------------------------------------
if __name__ == "__main__":
    repo_path = "/path/to/your/dvc_repo"  # Adjust to your local DVC repository
    remote_name = "minio"                # Use the remote name as in your .dvc/config

    # The factory instantiates the correct storage client.
    client = StorageClientFactory.create_storage_client(repo_path, remote_name)
    
    # Provide a local file path (tracked by DVC).
    local_file_path = "public/panoramic_12345.jpg"
    
    try:
        # Derive the remote file key from the local file path using DVC API.
        file_key = client.get_file_key(local_file_path)
        print("Derived file key from DVC API:", file_key)
    except Exception as e:
        print("Error deriving file key:", e)
    
    # Optionally, check if the file exists in the remote storage.
    if client.file_exists(file_key):
        print("File exists in the remote storage.")
    else:
        print("File does not exist in the remote storage.")
