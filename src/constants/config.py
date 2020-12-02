import os

config = dict(DATA_SCHEMA = "public",
    DATA_TABLENAME = "refresher",
    PARALLEL_PROCESSES = 10,
    CONTAINER_NAME = os.getenv('AZURE_STORAGE_CONTAINER_SOURCE'),
    STORAGE_CONNECTION_STR = os.getenv('AZURE_STORAGE_CONNECTION_STRING'),
    DB_USER = os.getenv('DB_USER'),
    DB_PASS = os.getenv('DB_PASS'),
    DB_HOST = os.getenv('DB_HOST'),
    DB_PORT = os.getenv('DB_PORT'),
    DB_NAME = os.getenv('DB_NAME')
)