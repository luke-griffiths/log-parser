import polars as pl
from typing import NewType
from pathlib import Path
import os
from dataclasses import dataclass
from typing import Any

from utils import FileExt, change_extension

# max compression should be slow, but very space efficient
ZSTD_COMPRESSION_LEVEL = 22
MAX_DISPLAY_ROWS = 6


JSONFile = NewType('JSONFile', Path)
ParquetFile = NewType('ParquetFile', Path)
LogFile = NewType('LogFile', Path)
CSVFile = NewType('CSVFile', Path)


@dataclass
class LogConfig:
    """
    Configuration info that helps a LogParser interact with a log

    Attributes
        comparator: The log field used for sorting. Usually a datetime.
        schema_overrides: Explicitly declare data types for log fields, 
            which can improve LogParser performance.
        preferred_order: Allows specification of field order 
            in LogParser output.
    """
    comparator: str
    schema_overrides: dict[str, Any] | None = None
    preferred_order: list[str] | None = None


class LogParser:
    """
    LogParser converts log files into a format that can be queried 
    and stored efficiently
    """
    def __init__(self, log: JSONFile | ParquetFile, config: LogConfig):
        self._log = Path(log)
        self._config = config
        self._comparator = self._config.comparator
        self._schema_overrides = self._config.schema_overrides
        self._stem, ext = os.path.splitext(self._log)
        match ext:
            case FileExt.PARQUET:
                self._lf = pl.scan_parquet(self._log)
            case FileExt.JSON:
                self._lf = pl.scan_ndjson(
                    self._log, schema_overrides=self._schema_overrides
                )
            case _:
                raise ValueError(f"Unknown file extension type '{ext}'")
            
        self._columns = self._lf.collect_schema().names()
        if self._comparator not in self._columns:
            raise ValueError(
                f"LogParser requires a valid comparator\n"
                f"'{self._comparator}' not found in {self._columns}"
            )
        
        if self._config.preferred_order:
            # order the LazyFrame columns based on preferences
            other_cols = set(self._columns) - set(self._config.preferred_order)
            self._lf = self._lf.select(
                self._config.preferred_order + list(other_cols)
            )
                        
    def __str__(self) -> str:
        n = MAX_DISPLAY_ROWS // 2
        row_count = self._lf.select(pl.len()).collect().item()
        first_n = self._lf.head(n)
        last_n = self._lf.tail(n)
        df = pl.concat([first_n, last_n]).unique().sort(self._comparator).collect()
        s = f"LogParser ({hex(id(self))}) with {row_count} log entries\n"
        if df.height > MAX_DISPLAY_ROWS:
            s += f"Showing first and last {n} rows\n"
        return s + str(df)
    
    @property
    def lf(self) -> pl.LazyFrame:
        """Access to the logs in lazy form. Calling 'collect' on larger 
        than memory log files will trigger a MemoryError"""
        return self._lf
    
    def save_log(self, ext: str, overwrite: bool = False) -> None:
        """
        Write log to selected file type

        Attributes
            ext: Selected file type.
            overwrite: Overwrite existing log file with same name.
        """
        lf = self.lf
        ext = "." + ext.lstrip(".")
        file = Path(f"{self._stem}{ext}")
        if file.exists() and not overwrite:
            raise FileExistsError
        match ext:
            case FileExt.PARQUET:
                lf.sink_parquet(file, compression_level=ZSTD_COMPRESSION_LEVEL)
            case FileExt.JSON:
                lf.sink_ndjson(file)
            case FileExt.CSV:
                lf.sink_csv(file)
            case FileExt.LOG:
                lf.sink_csv(file, include_header=False, separator="\t") 
                change_extension(file, FileExt.LOG)
            case _:
                raise ValueError(f"Unknown file extension type '{ext}'")

    def save_compressed_log(self, overwrite: bool = False) -> None:
        """Write a maximally compressed copy of the log file"""
        self.save_log(ext=FileExt.PARQUET, overwrite=overwrite)

    def match(self, expr: pl.Expr, return_ordered: bool = False) -> pl.DataFrame | None:
        """
        Returns all log entries matching the input expression

        With log files that exceed memory, calling this method may fail
        when attempting to allocate memory for a DataFrame larger than memory.
        It is up to the caller to handle potential MemoryError 
        and try again with a more narrow expression.

        Attributes
            expr: A Polars expression determining which entries to filter out.
            return_ordered: Return the result ordered by comparator field.
                Defaults to false for better performance.
        """
        lf = self._lf.filter(expr)
        if return_ordered:
            lf = lf.sort(self._comparator)
        df = lf.collect()
        return None if df.is_empty() else df