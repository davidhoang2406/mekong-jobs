import os

from pyspark.sql import SparkSession


class SparkFactory:
    """Wraps SparkSession creation with S3A/MinIO config pre-wired.

    Use as a context manager — session is always stopped on exit even if the job raises.
    """

    def __init__(self, app_name: str) -> None:
        self._app_name = app_name
        self._session: SparkSession | None = None

    @property
    def session(self) -> SparkSession:
        if self._session is None:
            self._session = self._build()
        return self._session

    def stop(self) -> None:
        if self._session is not None:
            self._session.stop()
            self._session = None

    def __enter__(self) -> SparkSession:
        return self.session

    def __exit__(self, *_) -> None:
        self.stop()

    def _build(self) -> SparkSession:
        master = os.getenv("SPARK_MASTER_URL", "local[*]")
        spark = (SparkSession.builder
                 .appName(self._app_name)
                 .master(master)
                 .config("spark.hadoop.fs.s3a.endpoint",          os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
                 .config("spark.hadoop.fs.s3a.access.key",        os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
                 .config("spark.hadoop.fs.s3a.secret.key",        os.getenv("MINIO_SECRET_KEY", "minioadmin"))
                 .config("spark.hadoop.fs.s3a.path.style.access", "true")
                 .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
                 .config("spark.sql.adaptive.enabled",                    "true")
                 .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                 .config("spark.sql.shuffle.partitions",                  "8")
                 .config("spark.eventLog.enabled", "true")
                 .config("spark.eventLog.dir",     "/tmp/spark-events")
                 .getOrCreate())
        spark.sparkContext.setLogLevel("WARN")
        return spark
