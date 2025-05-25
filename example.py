from pathlib import Path
import polars as pl

import log_compressor


if __name__ == "__main__":
    # assuming you've created a fake 'example.json' log file
    # using the provided Go code 
    config = log_compressor.LogConfig(
        comparator="timestamp", 
        schema_overrides={
                "timestamp": pl.Datetime(time_unit="ns"),
                "container": pl.Categorical,
                "level": pl.Enum(["DEBUG", "INFO", "WARN", "ERROR", "FATAL"])
            }, 
    )
    log = Path("log-maker/example.json")

    lp = log_compressor.LogCompressor(log=log, config=config)

    lp.save_compressed_log()
