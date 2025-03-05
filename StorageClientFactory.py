import os
import configparser
from abc import ABC, abstractmethod
from urllib.parse import urlparse

# ---------------------------------------------------------------------
# StorageClientInterface (Dependency Inversion / Interface Segregation)
# ---------------------------------------------------------------------
class StorageClientInterface(ABC):
    @abstractmethod
    def file_exists(self, file_path: str) -> bool:
        """Check if a file or object exists in the storage."""
        pass

    @abstractmethod
    def get_file_key(self, local_file_path: str, rev: str = "HEAD") -> str:
        """
        Given a local file path tracked by DVC, return the remote file key.
        """
        pass

    @abstractmethod
    def list_files(self, dir_path: str) -> list:
        """Lists all file keys (or paths) under the given directory."""
        pass

    @classmethod
    @abstractmethod
    def from_dvc_config(cls, repo_path: str, remote_name: str):
        """Factory method to create an instance from DVC configuration."""
        pass

# ---------------------------------------------------------------------
# S3StorageClient using MinIO (Single Responsibility)
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
        remote_file_url = dvc.api.get_url(
            local_file_path, repo=self.repo_path, remote=self.remote_name, rev=rev
        )
        parsed = urlparse(remote_file_url)
        return parsed.path.lstrip('/')

    def list_files(self, dir_path: str) -> list:
        parsed = urlparse(self.remote_url)
        bucket = parsed.netloc
        prefix = dir_path.lstrip('/')
        objects = self.client.list_objects(bucket, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects if obj.object_name.lower().endswith('.jpg')]

    @classmethod
    def from_dvc_config(cls, repo_path: str, remote_name: str) -> "S3StorageClient":
        config = cls._get_dvc_remote_config(repo_path, remote_name)
        remote_url = config.get("url")
        if not remote_url or not remote_url.startswith("s3://"):
            raise ValueError("DVC remote URL is not S3 for this client.")
        instance = cls(remote_url, config)
        instance.repo_path = repo_path
        instance.remote_name = remote_name
        return instance

    @staticmethod
    def _get_dvc_remote_config(repo_path: str, remote_name: str) -> dict:
        config_path = os.path.join(repo_path, ".dvc", "config")
        parser = configparser.ConfigParser()
        parser.read(config_path)
        section = f'remote "{remote_name}"'
        if section not in parser:
            raise ValueError(f"Remote '{remote_name}' not found in DVC config.")
        return dict(parser[section])

# ---------------------------------------------------------------------
# LocalStorageClient (Single Responsibility)
# ---------------------------------------------------------------------
class LocalStorageClient(StorageClientInterface):
    def __init__(self, base_path: str):
        self.base_path = base_path

    def file_exists(self, file_path: str) -> bool:
        full_path = os.path.join(self.base_path, file_path)
        return os.path.exists(full_path)

    def get_file_key(self, local_file_path: str, rev: str = "HEAD") -> str:
        import dvc.api
        remote_file_url = dvc.api.get_url(
            local_file_path, repo=self.repo_path, remote=self.remote_name, rev=rev
        )
        from urllib.parse import urlparse
        parsed = urlparse(remote_file_url)
        return parsed.path.lstrip('/')

    def list_files(self, dir_path: str) -> list:
        full_dir = os.path.join(self.base_path, dir_path)
        return [os.path.join(dir_path, f) for f in os.listdir(full_dir) if f.lower().endswith('.jpg')]

    @classmethod
    def from_dvc_config(cls, repo_path: str, remote_name: str) -> "LocalStorageClient":
        config = cls._get_dvc_remote_config(repo_path, remote_name)
        remote_url = config.get("url")
        if not remote_url:
            raise ValueError("No URL found in DVC config for local storage.")
        base_path = remote_url.replace("file://", "") if remote_url.startswith("file://") else remote_url
        instance = cls(base_path)
        instance.repo_path = repo_path
        instance.remote_name = remote_name
        return instance

    @staticmethod
    def _get_dvc_remote_config(repo_path: str, remote_name: str) -> dict:
        config_path = os.path.join(repo_path, ".dvc", "config")
        parser = configparser.ConfigParser()
        parser.read(config_path)
        section = f'remote "{remote_name}"'
        if section not in parser:
            raise ValueError(f"Remote '{remote_name}' not found in DVC config.")
        return dict(parser[section])

# ---------------------------------------------------------------------
# StorageClientFactory (Open/Closed Principle)
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
