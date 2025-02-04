from datetime import datetime

import pytest
from azure.core import exceptions as AzureExceptions
from azure.storage.blob import BlobServiceClient

from constants.config import config
from library.refresher import refresh_publisher_and_dataset_info, reload
from tests.integration.common_setup_and_teardown import setup_and_teardown  # noqa: F401
from tests.integration.utilities import get_dataset, patch_bds_index_url


@pytest.mark.parametrize(
    "setup_and_teardown", ["src/tests/integration/unified-pipeline-test-env-setup.env"], indirect=True
)
def test_download_add_single_dataset_updates_db(setup_and_teardown, mocker):  # noqa: F811

    patch_bds_index_url(mocker, "/bulk-data-service-indices/minimal-1-activity-file-200-status.json")

    # run the refresh stage once
    refresh_publisher_and_dataset_info()

    # download datasets
    reload(False)

    dataset = get_dataset(config, "aaaaaaaa-9bc5-41dd-9b16-9d9ea28cdf0a")

    assert dataset["download_error"] is None
    assert dataset["downloaded"] is not None


@pytest.mark.parametrize(
    "setup_and_teardown", ["src/tests/integration/unified-pipeline-test-env-setup.env"], indirect=True
)
def test_download_add_single_dataset_is_uploaded_to_azure(setup_and_teardown, mocker):  # noqa: F811

    patch_bds_index_url(mocker, "/bulk-data-service-indices/minimal-1-activity-file-200-status.json")

    # run the refresh stage once
    refresh_publisher_and_dataset_info()

    # download datasets
    reload(False)

    blob_service_client = BlobServiceClient.from_connection_string(config["STORAGE_CONNECTION_STR"])

    blob_client = blob_service_client.get_blob_client(
        container=config["SOURCE_CONTAINER_NAME"], blob="fbd23a51c3cc20d6fd53bd3d5c8b1568ec802170.xml"
    )

    blob_stream_downloader = blob_client.download_blob(encoding="UTF-8")
    file_content_azure = blob_stream_downloader.readall()

    with open("src/tests/artifacts/iati-xml-files/test-org-1-activity-1.xml", encoding="UTF-8") as f:
        file_content_disk = f.read()

    assert file_content_azure == file_content_disk


@pytest.mark.parametrize(
    "setup_and_teardown", ["src/tests/integration/unified-pipeline-test-env-setup.env"], indirect=True
)
def test_download_add_multiple_datasets_updates_db(setup_and_teardown, mocker):  # noqa: F811

    patch_bds_index_url(mocker, "/bulk-data-service-indices/minimal-3-activity-files-3-publishers-200-status.json")

    # run the refresh stage once
    refresh_publisher_and_dataset_info()

    # download datasets
    reload(False)

    for dataset_id in [
        "aaaaaaaa-9bc5-41dd-9b16-9d9ea28cdf0a",
        "bbbbbbbb-9bc5-41dd-9b16-9d9ea28cdf0b",
        "cccccccc-9bc5-41dd-9b16-9d9ea28cdf0c",
    ]:
        dataset = get_dataset(config, dataset_id)
        assert dataset is not None
        assert dataset["download_error"] is None
        assert dataset["downloaded"] is not None


@pytest.mark.parametrize(
    "setup_and_teardown", ["src/tests/integration/unified-pipeline-test-env-setup.env"], indirect=True
)
def test_download_add_single_dataset_bds_download_fail(setup_and_teardown, mocker):  # noqa: F811

    patch_bds_index_url(mocker, "/bulk-data-service-indices/minimal-1-activity-file-bds-failed-to-download.json")

    # run the refresh stage once
    refresh_publisher_and_dataset_info()

    # download datasets
    reload(False)

    dataset = get_dataset(config, "aaaaaaaa-9bc5-41dd-9b16-9d9ea28cdf0a")

    assert dataset["download_error"] == 4
    assert dataset["downloaded"] is None


@pytest.mark.parametrize(
    "setup_and_teardown", ["src/tests/integration/unified-pipeline-test-env-setup.env"], indirect=True
)
def test_download_add_single_dataset_azure_download_fail(setup_and_teardown, mocker):  # noqa: F811

    # Note: this test takes a long time (2 mins) because the download process (reload()) attempts
    # to remove old uploads from Azure storage containers when the download fails, and because
    # this is happening in subprocesses, all the mocking is undone, so we have to wait for timeouts.

    patch_bds_index_url(mocker, "/bulk-data-service-indices/minimal-1-activity-file-dataset-404s.json")

    # run the refresh stage once
    refresh_publisher_and_dataset_info()

    # download datasets
    reload(False)

    dataset = get_dataset(config, "aaaaaaaa-9bc5-41dd-9b16-9d9ea28cdf0a")

    assert dataset["download_error"] == 404
    assert dataset["downloaded"] is None


@pytest.mark.parametrize(
    "setup_and_teardown", ["src/tests/integration/unified-pipeline-test-env-setup.env"], indirect=True
)
def test_download_update_single_dataset_updates_db(setup_and_teardown, mocker):  # noqa: F811

    patch_bds_index_url(mocker, "/bulk-data-service-indices/minimal-1-activity-file-200-status.json")

    refresh_publisher_and_dataset_info()

    reload(False)

    time_before_dataset_updated = datetime.now()

    patch_bds_index_url(mocker, "/bulk-data-service-indices/minimal-1-activity-file-200-status-modified.json")

    refresh_publisher_and_dataset_info()

    reload(False)

    dataset = get_dataset(config, "aaaaaaaa-9bc5-41dd-9b16-9d9ea28cdf0a")

    assert dataset["download_error"] is None
    assert dataset["downloaded"] > time_before_dataset_updated


@pytest.mark.parametrize(
    "setup_and_teardown", ["src/tests/integration/unified-pipeline-test-env-setup.env"], indirect=True
)
def test_download_update_single_dataset_is_uploaded_to_azure(setup_and_teardown, mocker):  # noqa: F811

    patch_bds_index_url(mocker, "/bulk-data-service-indices/minimal-1-activity-file-200-status.json")

    refresh_publisher_and_dataset_info()

    reload(False)

    patch_bds_index_url(mocker, "/bulk-data-service-indices/minimal-1-activity-file-200-status-modified.json")

    refresh_publisher_and_dataset_info()

    reload(False)

    blob_service_client = BlobServiceClient.from_connection_string(config["STORAGE_CONNECTION_STR"])

    blob_client_old = blob_service_client.get_blob_client(
        container=config["SOURCE_CONTAINER_NAME"], blob="fbd23a51c3cc20d6fd53bd3d5c8b1568ec802170.xml"
    )

    with pytest.raises(AzureExceptions.ResourceNotFoundError):
        blob_stream_downloader = blob_client_old.download_blob(encoding="UTF-8")

    blob_client_new = blob_service_client.get_blob_client(
        container=config["SOURCE_CONTAINER_NAME"], blob="043e0e0fe9d02418d50c37c058dba8c9a02a5ee4.xml"
    )

    blob_stream_downloader = blob_client_new.download_blob(encoding="UTF-8")
    file_content_azure = blob_stream_downloader.readall()

    with open("src/tests/artifacts/iati-xml-files/test-org-1-activity-1-modified.xml", encoding="UTF-8") as f:
        file_content_disk = f.read()

    assert file_content_azure == file_content_disk
