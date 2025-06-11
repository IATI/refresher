"""This module defines project-level constants and environment variables"""

import os


def load_config_from_env():

    limit_to_reporting_orgs = [k.strip() for k in os.getenv("LIMIT_TO_REPORTING_ORGS", "").split(",") if k != ""]
    limit_to_datasets = [k.strip() for k in os.getenv("LIMIT_TO_DATASETS", "").split(",") if k != ""]

    return dict(
        # Logs
        LOG_LEVEL=os.getenv("LOG_LEVEL") or "info",
        SENTRY_DSN=os.getenv("SENTRY_DSN", default=None),
        # Database Connection
        DB_USER=os.getenv("DB_USER"),
        DB_PASS=os.getenv("DB_PASS"),
        DB_HOST=os.getenv("DB_HOST"),
        DB_PORT=os.getenv("DB_PORT"),
        DB_NAME=os.getenv("DB_NAME"),
        DB_SSL_MODE=os.getenv("DB_SSL_MODE") or "require",
        # Database retry/timeout constants
        DB_CONN_RETRY_LIMIT=8,
        DB_CONN_SLEEP_START=5,
        DB_CONN_SLEEP_MAX=60,
        DB_CONN_TIMEOUT=5,
        DB_KEEPALIVE_IDLE=int(os.getenv("DB_KEEPALIVE_IDLE", default=60)),
        DB_KEEPALIVE_INTERVAL=int(os.getenv("DB_KEEPALIVE_INTERVAL", default=15)),
        DB_KEEPALIVE_COUNT=int(os.getenv("DB_KEEPALIVE_COUNT", default=5)),
        # Azure Storage Account Connection String
        # This can be found in the Azure Portal > Storage Account > Access Keys
        STORAGE_CONNECTION_STR=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
        # The string name of your blob container in Azure E.g. "source"
        # Stores source documents as downloaded from publishers
        SOURCE_CONTAINER_NAME=os.getenv("AZURE_STORAGE_CONTAINER_SOURCE"),
        # The string name of your blob container in Azure E.g. "clean"
        # Stores "cleaned" documents with only valid activities
        CLEAN_CONTAINER_NAME=os.getenv("AZURE_STORAGE_CONTAINER_CLEAN"),
        # The string name of your blob container in Azure E.g. "activity-lake"
        # Stores single activity XML and JSON for valid activities
        ACTIVITIES_LAKE_CONTAINER_NAME=os.getenv("ACTIVITIES_LAKE_CONTAINER_NAME"),
        # Communications Hub API URL/key
        NOTIFICATION_URL=os.getenv("COMMSHUB_URL"),
        NOTIFICATION_KEY_NAME="x-functions-key",
        NOTIFICATION_KEY_VALUE=os.getenv("COMMSHUB_KEY"),
        REFRESHER=dict(
            LIMIT_ENABLED=os.getenv("LIMIT_ENABLED", "no"),
            LIMIT_TO_REPORTING_ORGS=limit_to_reporting_orgs,
            LIMIT_TO_DATASETS=limit_to_datasets,
            # Number of parallel processes to run the refresh loop with
            PARALLEL_PROCESSES=10,
            # How long to sleep in seconds between loops if ETags are available
            SERVICE_LOOP_SLEEP=os.getenv("REFRESH_STAGE_LOOP_SLEEP", 60),
            # How many refresh loops to run before re-trying files with download errors
            RETRY_ERRORS_AFTER_LOOP=30,
            # the URL of the Bulk Data Service's Minimal Dataset Index
            BULK_DATA_SERVICE_DATASET_INDEX_URL=os.getenv("BULK_DATA_SERVICE_DATASET_INDEX_URL"),
            # the URL of the Bulk Data Service's Reporting Orgs Index
            BULK_DATA_SERVICE_REPORTING_ORG_INDEX_URL=os.getenv("BULK_DATA_SERVICE_REPORTING_ORG_INDEX_URL"),
            # the timeout when contacting the Bulk Data Service
            BULK_DATA_SERVICE_HTTP_TIMEOUT=int(os.getenv("BULK_DATA_SERVICE_HTTP_TIMEOUT", default=600)),
            # Percent of Publishers/Documents that must disappear from the Registry to stop the refresher from syncing
            PUBLISHER_SAFETY_PERCENTAGE=50,
            DOCUMENT_SAFETY_PERCENTAGE=50,
            # Maximum number of blobs to delete in a single request when cleaning up blob containers
            MAX_BLOB_DELETE=250,
            PROM_PORT=9091,
            PROM_METRIC_DEFS=[
                ("registered_publishers", "The number of publishers on the CKAN Registry"),
                ("registered_datasets", "The number of datasets on the CKAN Registry"),
                ("datasets_changed", "The number of changed datasets that have been changed"),
                ("datasets_to_download", "The number of datasets that need re-downloading"),
            ],
        ),
        VALIDATION=dict(
            # Number of parallel processes to run the validation loop with
            PARALLEL_PROCESSES=1,
            # Schema Validation API URL/key
            SCHEMA_VALIDATION_URL=os.getenv("SCHEMA_VALIDATION_API_URL"),
            SCHEMA_VALIDATION_KEY_NAME=os.getenv("SCHEMA_VALIDATION_KEY_NAME"),
            SCHEMA_VALIDATION_KEY_VALUE=os.getenv("SCHEMA_VALIDATION_KEY_VALUE"),
            SCHEMA_VALIDATION_TIMEOUT=int(os.getenv("SCHEMA_VALIDATOR_API_TIMEOUT", default=3600)),
            # Full Validation API URL/key
            FULL_VALIDATION_URL=os.getenv("VALIDATOR_API_URL"),
            FULL_VALIDATION_KEY_NAME=os.getenv("VALIDATOR_API_KEY_NAME"),
            FULL_VALIDATION_KEY_VALUE=os.getenv("VALIDATOR_API_KEY_VALUE"),
            FULL_VALIDATION_TIMEOUT=int(os.getenv("VALIDATOR_API_TIMEOUT", default=3600)),
            # Publisher Black Flagging Period and Threshold
            # Number of Critically Invalid Documents
            SAFETY_CHECK_THRESHOLD=100,
            # Hours
            SAFETY_CHECK_PERIOD=2,
            PROM_PORT=9092,
            PROM_METRIC_DEFS=[
                ("new_flagged_publishers", "The number of publishers that have been newly flagged"),
                ("datasets_to_validate", "The number of datasets that need validating"),
            ],
        ),
        CLEAN=dict(
            # Number of parallel processes to run the clean loop with
            PARALLEL_PROCESSES=1,
            PROM_PORT=9093,
            PROM_METRIC_DEFS=[
                ("valid_datasets_to_progress", "The number of valid datasets to progress to flatten stage"),
                ("invalid_datasets_to_clean", "The number of invalid datasets that need cleaning"),
            ],
        ),
        FLATTEN=dict(
            # Number of parallel processes to run the flatten loop with
            PARALLEL_PROCESSES=1,
            PROM_PORT=9094,
            PROM_METRIC_DEFS=[
                ("datasets_to_flatten", "The number of datasets that need flattening"),
            ],
        ),
        LAKIFY=dict(
            # Number of parallel processes to run the lakify loop with
            PARALLEL_PROCESSES=10,
            PROM_PORT=9095,
            PROM_METRIC_DEFS=[
                ("datasets_to_lakify", "The number of datasets that need lakifying"),
            ],
        ),
        SOLRIZE=dict(
            # Number of parallel processes to run the solrize loop with
            PARALLEL_PROCESSES=int(os.getenv("SOLR_PARALLEL_PROCESSES") or 1),
            # Solr API URL and Username/Password
            SOLR_API_URL=os.getenv("SOLR_API_URL"),
            SOLR_USER=os.getenv("SOLR_USER"),
            SOLR_PASSWORD=os.getenv("SOLR_PASSWORD"),
            # Elements to explode into their own collections with one solr document per element
            EXPLODE_ELEMENTS='["transaction", "budget"]',
            # Maximum number of solr documents to index in one request
            MAX_BATCH_LENGTH=500,
            # Timeout for pysolr package
            PYSOLR_TIMEOUT=600,
            # Time in seconds to sleep after receiving a 5XX error from Solr
            SOLR_500_SLEEP=os.getenv("SOLR_500_SLEEP"),
            PROM_PORT=9096,
            PROM_METRIC_DEFS=[
                ("datasets_to_solrize", "The number of datasets that need solrizing"),
            ],
        ),
    )


config = load_config_from_env()
