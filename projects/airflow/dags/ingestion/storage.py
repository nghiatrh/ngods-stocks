"""
MinIO/S3 parquet storage utilities for the bronze zone.

Paths:
  Market data:    s3://warehouse/bronze/market/<dataset>/date=YYYY-MM-DD/part-0.parquet
  Reference data: s3://warehouse/bronze/reference/<dataset>/data.parquet
"""

import os
import logging
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

log = logging.getLogger(__name__)

BUCKET = "warehouse"
BRONZE_PREFIX = "bronze"

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minio")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minio123")


def get_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        anon=False,
        key=AWS_ACCESS_KEY,
        secret=AWS_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )


def market_path(dataset: str, date: str) -> str:
    """Return S3 path for a market dataset partition."""
    return f"{BUCKET}/{BRONZE_PREFIX}/market/{dataset}/date={date}/part-0.parquet"


def reference_path(dataset: str) -> str:
    """Return S3 path for a reference dataset."""
    return f"{BUCKET}/{BRONZE_PREFIX}/reference/{dataset}/data.parquet"


def write_parquet(df, path: str, fs: s3fs.S3FileSystem) -> None:
    """Write a DataFrame as a snappy-compressed parquet file to S3.

    Timestamps are coerced to microsecond precision (us) so that Spark can read
    them natively. Pandas defaults to nanosecond (ns) which produces
    INT64 (TIMESTAMP(NANOS)) in parquet — a type Spark rejects at read time.
    """
    if df is None or df.empty:
        log.warning("Empty DataFrame, skipping write to %s", path)
        return
    table = pa.Table.from_pandas(df, preserve_index=False)
    with fs.open(path, "wb") as f:
        pq.write_table(
            table,
            f,
            compression="snappy",
            coerce_timestamps="us",           # downcast ns → us
            allow_truncated_timestamps=True,  # silent truncation (sub-us precision is irrelevant here)
        )
    log.info("Written %d rows to %s", len(df), path)


def delete_reference(dataset: str, fs: s3fs.S3FileSystem) -> None:
    """Delete existing reference parquet to implement truncate-before-load."""
    path = reference_path(dataset)
    if fs.exists(path):
        fs.rm(path)
        log.info("Deleted existing reference data at %s", path)
