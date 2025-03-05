import io
from abc import ABC, abstractmethod
from PIL import Image

class ImageLoader(ABC):
    @abstractmethod
    def load_image(self, path: str) -> Image.Image:
        """Loads an image from the given path and returns a PIL Image."""
        pass

class LocalImageLoader(ImageLoader):
    def load_image(self, path: str) -> Image.Image:
        return Image.open(path).convert('RGB')

class S3ImageLoader(ImageLoader):
    def __init__(self, client, bucket_name: str):
        self.client = client
        self.bucket_name = bucket_name

    def load_image(self, path: str) -> Image.Image:
        response = self.client.get_object(self.bucket_name, path)
        try:
            data = response.read()
            return Image.open(io.BytesIO(data)).convert('RGB')
        finally:
            response.close()
            response.release_conn()
