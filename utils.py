import polars as pl
from pathlib import Path
import os

class FileExt:
    PARQUET = ".parquet"
    JSON = ".json"
    CSV = ".csv"
    LOG = ".log"


def change_extension(file: Path, ext: str) -> None:
    assert ext in [FileExt.JSON, FileExt.LOG, FileExt.CSV]
    stem, _ = os.path.splitext(file)
    new_file = f"{stem}{ext}"
    os.rename(file, new_file)
