from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.models import AirbyteCatalog, AirbyteStream, SyncMode
from typing import Any, Mapping, Optional, Tuple
from .streams import FileInputStream
import boto3
import psycopg2
import logging


class SourceS3PostgresLoader(AbstractSource):

    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Optional[str]]:
        # Validate required keys (including s3_prefix)
        required_keys = [
            "s3_bucket", "postgres_host", "postgres_user", "postgres_password", "postgres_db",
            "aws_access_key_id", "aws_secret_access_key", "region_name", "s3_prefix"
        ]
        for key in required_keys:
            if key not in config:
                return False, f"Missing config key: {key}"

        try:
            # Check S3 connection
            boto3.client(
                "s3",
                aws_access_key_id=config["aws_access_key_id"],
                aws_secret_access_key=config["aws_secret_access_key"],
                region_name=config["region_name"]
            ).list_buckets()

            # Check Postgres connection
            psycopg2.connect(
                host=config["postgres_host"],
                user=config["postgres_user"],
                password=config["postgres_password"],
                dbname=config["postgres_db"]
            ).close()

            return True, None
        except Exception as e:
            logger.error(f"Connection check failed: {str(e)}")
            return False, str(e)

    def streams(self, config: Mapping[str, Any]):
        # Split config for Postgres and S3
        postgres_config = {
            "host": config["postgres_host"],
            "user": config["postgres_user"],
            "password": config["postgres_password"],
            "dbname": config["postgres_db"]
        }

        s3_config = {
            "aws_access_key_id": config["aws_access_key_id"],
            "aws_secret_access_key": config["aws_secret_access_key"],
            "bucket": config["s3_bucket"],
            "region_name": config["region_name"],
            "prefix": config["s3_prefix"],  # required, accessed directly
        }

        return [FileInputStream(postgres_config, s3_config)]

    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        stream = AirbyteStream(
        name="file_input_stream",
        supported_sync_modes=[SyncMode.full_refresh],
        json_schema={
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {},
        },
    )
        return AirbyteCatalog(streams=[stream])



