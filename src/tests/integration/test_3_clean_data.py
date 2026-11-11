import json

import pytest
from azure.core import exceptions as AzureExceptions
from azure.storage.blob import BlobServiceClient

from constants.config import config
from library import clean, db
from library.refresher import refresh_publisher_and_dataset_info, reload
from tests.integration.common_setup_and_teardown import setup_and_teardown  # noqa: F401
from tests.integration.utilities import get_dataset, get_db_connection, patch_bds_index_url

DATASET_ID = "aaaaaaaa-9bc5-41dd-9b16-9d9ea28cdf0b"


@pytest.mark.parametrize(
    "setup_and_teardown", ["src/tests/integration/unified-pipeline-test-env-setup.env"], indirect=True
)
def test_clean_stages_parse_xml_doesnt_resolve_file_entities(setup_and_teardown, mocker):  # noqa: F811
    # Regression test to ensure that XML custom ENTITIES that reference files aren't resolved
    # Prior to 2.9.3, lxml 4.9.3 was used, and it defaulted to resolving file entities

    patch_bds_index_url(
        mocker, "/bulk-data-service-indices/minimal-1-activity-file-200-status-containing-custom-entity.json"
    )

    # run the refresh stage once
    refresh_publisher_and_dataset_info()

    # download datasets
    reload(False)

    # mock the validate stage, marking the document invalid so clean parses the XML,
    # with the activity itself valid so the clean stage tries to re-serialise it
    row = get_dataset(config, DATASET_ID)
    assert row is not None
    report = {
        "fileType": "iati-activities",
        "iatiVersion": "2.03",
        "iati-activities": [{"index": 0, "valid": True}],
    }
    conn = get_db_connection(config)
    db.updateValidationState(
        conn, row["id"], row["hash"], row["url"], row["publisher"], False, json.dumps(report), row["name"]
    )
    conn.close()

    # run the real clean stage
    clean.clean_invalid()

    dataset = get_dataset(config, DATASET_ID)
    assert dataset is not None and dataset["clean_start"] is not None

    blob_service_client = BlobServiceClient.from_connection_string(config["STORAGE_CONNECTION_STR"])

    blob_client = blob_service_client.get_blob_client(
        container=config["CLEAN_CONTAINER_NAME"], blob=row["hash"] + ".xml"
    )

    try:
        blob_stream_downloader = blob_client.download_blob(encoding="UTF-8")
        file_content_azure = blob_stream_downloader.readall()
    except AzureExceptions.ResourceNotFoundError:
        # clean rejects a file it cannot parse, so there may be no clean copy at all
        file_content_azure = ""

    with open("src/tests/artifacts/iati-xml-files/test-org-1-activity-2-should-not-read.txt", encoding="UTF-8") as f:
        should_not_read_content = f.read().strip()

    assert should_not_read_content not in file_content_azure
