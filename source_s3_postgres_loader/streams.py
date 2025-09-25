import psycopg2
import boto3
import csv
import logging
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.models import AirbyteMessage, Type, AirbyteRecordMessage
from datetime import datetime
from typing import Iterator, Mapping, Any


class FileInputStream(Stream):
    name = "file_input_stream"
    primary_key = []

    def __init__(self, postgres_config: Mapping[str, Any], s3_config: Mapping[str, Any]):
        super().__init__()
        self.postgres_config = postgres_config
        self.s3_config = s3_config
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=s3_config.get("aws_access_key_id"),
            aws_secret_access_key=s3_config.get("aws_secret_access_key"),
            region_name=s3_config.get("region_name"),
        )
        self.s3_bucket = s3_config["bucket"]

    def get_json_schema(self) -> Mapping[str, Any]:
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "data": {"type": "string"},
            },
        }

    def read_records(self, **kwargs) -> Iterator[AirbyteMessage]:
        logger = logging.getLogger("airbyte.file_input_stream")

        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(**self.postgres_config)
            cursor = conn.cursor()
            cursor.execute("SELECT file_name FROM file_input WHERE status = 'NEW'")
            files = cursor.fetchall()

            logger.info(f"Found {len(files)} new files in file_input")

            if not files:
                return

            prefix = self.s3_config["prefix"]  # required, no default

            for (file_name,) in files:
                try:
                    logger.info(f"Processing file: {file_name}")
                    cursor.execute("UPDATE file_input SET status = 'IN PROGRESS' WHERE file_name = %s", (file_name,))

                    s3_key = f"{prefix}{file_name}"
                    s3_obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
                    lines = s3_obj['Body'].read().decode('utf-8').splitlines()
                    reader = csv.DictReader(lines)

                    for row in reader:
                        yield AirbyteMessage(
                            type=Type.RECORD,
                            record=AirbyteRecordMessage(
                                stream=self.name,
                                data=row,
                                emitted_at=int(datetime.now().timestamp() * 1000),
                            ),
                        )

                    cursor.execute("UPDATE file_input SET status = 'COMPLETED' WHERE file_name = %s", (file_name,))
                    conn.commit()

                except Exception as e:
                    logger.error(f"Failed processing {file_name}: {str(e)}")
                    cursor.execute("UPDATE file_input SET status = 'FAILED' WHERE file_name = %s", (file_name,))
                    conn.commit()
                    raise e

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
