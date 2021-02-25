import os

config = dict(DATA_SCHEMA = "public",
    DATA_TABLENAME = "refresher",
    PARALLEL_PROCESSES = 10,
    SOURCE_CONTAINER_NAME = os.getenv('AZURE_STORAGE_CONTAINER_SOURCE'),
    VALIDATION_CONTAINER_NAME = os.getenv('AZURE_STORAGE_CONTAINER_VALIDATION'),
    STORAGE_CONNECTION_STR = os.getenv('AZURE_STORAGE_CONNECTION_STRING'),
    SERVICE_LOOP_SLEEP = 60,
    RETRY_ERRORS_AFTER_LOOP = 60,
    DB_USER = os.getenv('DB_USER'),
    DB_PASS = os.getenv('DB_PASS'),
    DB_HOST = os.getenv('DB_HOST'),
    DB_PORT = os.getenv('DB_PORT'),
    DB_NAME = os.getenv('DB_NAME'),
    DDS = dict(
        SKIP_EXISTING_FILES = True,
        PARALLEL_PROCESSES = 20
    ),
    VALIDATION = dict(
        PARALLEL_PROCESSES = 1,
        FILE_VALIDATION_URL = os.getenv('VALIDATOR_API_URL')
    )
)