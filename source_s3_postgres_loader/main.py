# main.py

import sys
from source_s3_postgres_loader.source import SourceS3PostgresLoader
# from source import SourceS3PostgresLoader
from airbyte_cdk.entrypoint import launch

def main():
    source = SourceS3PostgresLoader()
    launch(source, sys.argv[1:])

if __name__ == "__main__":
    main()
