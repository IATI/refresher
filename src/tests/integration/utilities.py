from typing import Optional

import psycopg2
import psycopg2.extras
from azure.storage.blob import BlobServiceClient

from constants.config import config, load_config_from_env


def patch_bds_index_url(mocker, url: str):
    mocker.patch.dict(
        "constants.config.config",
        {"REFRESHER": config["REFRESHER"] | {"BULK_DATA_SERVICE_DATASET_INDEX_URL": "http://localhost:3005" + url}},
    )


def patch_bds_reporting_org_index_url(mocker, url: str):
    mocker.patch.dict(
        "constants.config.config",
        {
            "REFRESHER": config["REFRESHER"]
            | {"BULK_DATA_SERVICE_REPORTING_ORG_INDEX_URL": "http://localhost:3005" + url}
        },
    )


def empty_azurite_containers(config: dict):

    if config["STORAGE_CONNECTION_STR"].find("localhost:12000") == -1:
        raise RuntimeError("Misconfigured tests. Expecting to connect to Azurite on localhost")

    blob_service_client = BlobServiceClient.from_connection_string(config["STORAGE_CONNECTION_STR"])

    containers = [
        config["SOURCE_CONTAINER_NAME"],
        config["CLEAN_CONTAINER_NAME"],
        config["ACTIVITIES_LAKE_CONTAINER_NAME"],
    ]

    for container in containers:
        empty_azurite_container(config, blob_service_client, container)


def empty_azurite_container(config: dict, blob_service_client: BlobServiceClient, container_name: str):

    container_client = blob_service_client.get_container_client(container_name)

    container_client.delete_container()

    container_client = blob_service_client.get_container_client(container_name)

    container_client.create_container()


def get_dataset(config: dict, dataset_id: str) -> Optional[psycopg2.extras.RealDictRow]:
    conn = get_db_connection(config)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    sql = "SELECT * FROM document WHERE id=%(id)s"
    cur.execute(sql, {"id": dataset_id})
    results = cur.fetchone()
    cur.close()
    return results


def get_publisher_name(config: dict, publisher_id: str) -> str | None:
    conn = get_db_connection(config)
    cur = conn.cursor()
    sql = "SELECT name FROM publisher WHERE org_id=%(publisher_id)s"
    cur.execute(sql, {"publisher_id": publisher_id})
    conn.commit()
    results = cur.fetchone()
    cur.close()
    return results[0] if results is not None else None


def truncate_db_tables(config: dict):
    conn = get_db_connection(config)

    if conn is not None:
        for table in ["document", "publisher"]:
            truncate_db_table(table, conn)

    conn.commit()


def truncate_db_table(table_name: str, conn: psycopg2.extensions.connection):
    if conn.info.dsn_parameters["host"] != "localhost":
        raise RuntimeError("Misconfigured tests. Expecting to connect to database on localhost")
    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE table {table_name} CASCADE")
    cursor.close()


def get_db_connection_patched(retry_counter: int = 0) -> psycopg2.extensions.connection:
    config = load_config_from_env()
    return get_db_connection(config)


def get_db_connection(config: dict) -> psycopg2.extensions.connection:
    connection = None

    if config["DB_HOST"] != "localhost":
        raise RuntimeError("Misconfigured tests. Expecting to connect to database on localhost")

    connection = psycopg2.connect(
        dbname=config["DB_NAME"],
        user=config["DB_USER"],
        password=config["DB_PASS"],
        host=config["DB_HOST"],
        port=config["DB_PORT"],
        sslmode="prefer" if config["DB_SSL_MODE"] is None else config["DB_SSL_MODE"],
        connect_timeout=config["DB_CONN_TIMEOUT"],
    )

    return connection
