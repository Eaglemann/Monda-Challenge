from prefect import task
from prefect.cache_policies import NONE
from minio import Minio
import pandas as pd

from config.settings import MinIOSettings


@task(cache_policy=NONE, retries=3)
def upload_to_minio(
    file_path: str, object_name: str, minio_config: MinIOSettings
) -> str:
    """
    Upload CSV to MinIO for local object-store coverage in the ETL contract.

    Cache is disabled so reruns always publish the file requested by the
    current flow invocation.
    """

    client = Minio(
        minio_config.endpoint,
        access_key=minio_config.access_key,
        secret_key=minio_config.secret_key,
        secure=minio_config.secure,
    )

    # Local docker runs bootstrap bucket state on demand.
    if not client.bucket_exists(minio_config.bucket):
        client.make_bucket(minio_config.bucket)

    client.fput_object(minio_config.bucket, object_name, file_path)

    return f"s3://{minio_config.bucket}/{object_name}"


@task(cache_policy=NONE)
def download_from_minio(
    object_name: str, output_path: str, minio_config: MinIOSettings
) -> pd.DataFrame:
    """
    Download object from MinIO and return parsed CSV rows for downstream typing.

    Cache is disabled to avoid reusing stale local files across flow runs.
    """

    client = Minio(
        minio_config.endpoint,
        access_key=minio_config.access_key,
        secret_key=minio_config.secret_key,
        secure=minio_config.secure,
    )

    client.fget_object(minio_config.bucket, object_name, output_path)

    return pd.read_csv(output_path)
