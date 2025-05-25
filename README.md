# Storing Large Logs
Let's say you have a logger that generates some data. You want to process that data in batches (daily), do some queries on the logged info, and then store the logs somewhere for safekeeping (S3 bucket?) for legal reasons. If your logger is something like Docker's default `json-file` log driver, it will generate JSON files that take up a lot of space. Storing these logs is expensive, so you need to compress them. 

The simplest solution is to use a standard linux command. For the following tests, I ran `go run create.go 20000000` to create a fake log with 20M entries, occupying ~2.6GB. Let's compress this using `zstd -19 log-maker/example.json`. This results in a 280MB `.zst` file, which is great! We've nearly lowered our storage size by an order of magnitude. Unfortunately that simple command took almost 12 minutes to run. 

`1392.67s user 4.98s system 199% cpu 11:40.91 total`

This is the baseline. Can we do better?

The `example.py` script configures `LogCompressor` with basic information about our logged data (`LogConfig`). This information is pretty fundamental to how the logger is implemented, so you would only ever need to update the `LogConfig` if you choose a different logger. Run the script using `python3 example.py`. That creates a 235MB Parquet file--even smaller than the `zstd` file--and takes 1/6th the time. 

`586.79s user 10.12s system 510% cpu 1:56.89 total`

Since `LogCompressor` stores data in Parquet format, if uploaded to S3 (or similar) it can be read back locally by passing a path to the S3 object to `LogCompressor`. 

## Note
`LogCompressor` implements a `match` method to query the logs--this was mainly to enhance testing and debugging. Because `LogCompressor` was designed to handle larger than memory log files, it is not optimized for log queries, and `match` is recommended only for toy queries on small logs.
