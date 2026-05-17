import io
import logging
import os

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.deleteobjects import DeleteObject

log = logging.getLogger(__name__)


def _build_client() -> Minio:
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    secure   = endpoint.startswith("https://")
    host     = endpoint.split("://", 1)[-1]
    return Minio(
        host,
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=secure,
    )


class MinioStore:
    """MinIO wrapper focused on Parquet reads and lightweight existence checks."""

    def __init__(self, bucket: str, client: Minio | None = None) -> None:
        self.bucket  = bucket
        self._client = client or _build_client()

    def list_objects(self, prefix: str = "", recursive: bool = True):
        return self._client.list_objects(self.bucket, prefix=prefix, recursive=recursive)

    def get_object(self, key: str):
        return self._client.get_object(self.bucket, key)

    def read_avro(self, key: str) -> list[dict]:
        response = self.get_object(key)
        try:
            return list(fastavro.reader(io.BytesIO(response.read())))
        finally:
            response.close()
            response.release_conn()

    def write_parquet(self, key: str, schema: pa.Schema, rows: list[dict]) -> None:
        if not rows:
            return
        table = pa.Table.from_pylist(rows, schema=schema)
        buf   = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        data  = buf.getvalue()
        self._client.put_object(
            self.bucket, key, io.BytesIO(data), len(data),
            content_type="application/octet-stream",
        )
        log.info("wrote %d rows → s3://%s/%s", len(rows), self.bucket, key)
