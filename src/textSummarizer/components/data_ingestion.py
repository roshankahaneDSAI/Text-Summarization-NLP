import os
import urllib.request as request
import zipfile
from textSummarizer.logging import logger
from textSummarizer.utils.common import get_size
from pathlib import Path
import requests
from textSummarizer.entity import DataIngestionConfig


class DataIngestion:
    def __init__(self, config: DataIngestionConfig):
        self.config = config

    def download_file(self):
        dst = Path(self.config.local_data_file)
        dst.parent.mkdir(parents=True, exist_ok=True)

        url = self.config.source_URL
        resp = requests.get(url, stream=True, allow_redirects=True, timeout=60)
        print("status:", resp.status_code)
        print("headers:", resp.headers)

        resp.raise_for_status()  # raise if 4xx/5xx

        with dst.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        size = dst.stat().st_size
        print("written bytes:", size)
        if size == 0:
            raise RuntimeError("Downloaded file is 0 bytes, aborting.")

        
    
    def extract_zip_file(self):
        """
        zip_file_path: str
        Extracts the zip file into the data directory
        Function returns None
        """
        zip_path = Path(self.config.local_data_file)
        unzip_path = Path(self.config.unzip_dir)
        unzip_path.mkdir(parents=True, exist_ok=True)

        try:
            with zipfile.ZipFile(str(zip_path), "r") as zip_ref:
                logger.info("Zip contents: %s", zip_ref.namelist()[:10])
                zip_ref.extractall(str(unzip_path))
        except (zipfile.BadZipFile, EOFError) as e:
            sample = zip_path.read_bytes()[:1024].decode(errors="replace")
            raise RuntimeError(
                f"Zip extraction failed (corrupt/truncated). Size={zip_path.stat().st_size}. Preview:\n{sample}"
            ) from e
