import os

config = dict(DATA_SCHEMA = "public",
    PARALLEL_PROCESSES = 10,
    SOURCE_CONTAINER_NAME = os.getenv('AZURE_STORAGE_CONTAINER_SOURCE'),
    ACTIVITIES_LAKE_CONTAINER_NAME = os.getenv('ACTIVITIES_LAKE_CONTAINER_NAME'),
    STORAGE_CONNECTION_STR = os.getenv('AZURE_STORAGE_CONNECTION_STRING'),
    SERVICE_LOOP_SLEEP = 60,
    RETRY_ERRORS_AFTER_LOOP = 30,
    PUBLISHER_SAFETY_PERCENTAGE = 50,
    DOCUMENT_SAFETY_PERCENTAGE = 50,
    MAX_BLOB_DELETE = 250,
    NOTIFICATION_URL = 'https://func-communications-hub-dev.azurewebsites.net/api/pvt/notification/slack',
    NOTIFICATION_KEY_NAME = 'x-functions-key',
    NOTIFICATION_KEY_VALUE = 'RBNjwKyhQflZc3S6cVcf4owzQv9gYOBLTm6OFn1VYLJ39FeJ45EZUg==',
    DB_USER = os.getenv('DB_USER'),
    DB_PASS = os.getenv('DB_PASS'),
    DB_HOST = os.getenv('DB_HOST'),
    DB_PORT = os.getenv('DB_PORT'),
    DB_NAME = os.getenv('DB_NAME'),
    VALIDATION = dict(
        PARALLEL_PROCESSES = 1,
        ADHOC_PARALLEL_PROCESSES = 1,
        ACTIVITY_LEVEL_PARALLEL_PROCESSES = 1,
        FILE_VALIDATION_URL = os.getenv('VALIDATOR_API_URL'),
        FILE_VALIDATION_KEY_NAME = os.getenv('VALIDATOR_API_KEY_NAME'),
        FILE_VALIDATION_KEY_VALUE = os.getenv('VALIDATOR_API_KEY_VALUE'),
        ALV_THRESHOLD = 1000,
        ALV_PERIOD = 24
    ),
    FLATTEN = dict(
        PARALLEL_PROCESSES = 1,
        FLATTENER_URL = os.getenv('FLATTENER_API_URL'),
        FLATTENER_KEY_NAME = os.getenv('FLATTENER_KEY_NAME'),
        FLATTENER_KEY_VALUE = os.getenv('FLATTENER_KEY_VALUE')
    ),
    SOLRIZE = dict(
        PARALLEL_PROCESSES = 5,
        SOLR_API_URL = os.getenv('SOLR_API_URL'),
        SOLR_USER = os.getenv('SOLR_USER'),
        SOLR_PASSWORD = os.getenv('SOLR_PASSWORD'),
        EXPLODE_ELEMENTS = '["transaction", "budget"]',
        SOLR_500_SLEEP = os.getenv('SOLR_500_SLEEP')
    ),
    LAKIFY = dict(
        PARALLEL_PROCESSES = 10
    )
)