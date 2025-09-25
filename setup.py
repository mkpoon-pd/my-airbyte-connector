# setup.py

from setuptools import setup, find_packages

setup(
    name="airbyte-source-s3-postgres-loader",
    version="0.1.0",
    description="Airbyte source connector for loading files from S3 based on Postgres queue",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "airbyte-cdk>=0.1.0",
        "boto3",
        "psycopg2-binary",
    ],
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
 #           "airbyte-source-s3-postgres-loader=main:main",
            "airbyte-source-s3-postgres-loader=source_s3_postgres_loader.main:main",
        ],
    },
)

