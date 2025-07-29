import pytest

import library.db as db
from constants.config import config
from library.refresher import refresh_publisher_and_dataset_info
from tests.integration.common_setup_and_teardown import setup_and_teardown  # noqa: F401
from tests.integration.utilities import (
    get_dataset,
    get_publisher_name,
    patch_bds_index_url,
    patch_bds_reporting_org_index_url,
)


@pytest.mark.parametrize(
    "setup_and_teardown", ["src/tests/integration/unified-pipeline-test-env-setup.env"], indirect=True
)
def test_publisher_add_single(setup_and_teardown, mocker):  # noqa: F811

    patch_bds_index_url(mocker, "/bulk-data-service-indices/minimal-1-activity-file-200-status.json")
    patch_bds_reporting_org_index_url(
        mocker, "/bulk-data-service-indices/reporting-org-index-03-a-single-reporting-org.json"
    )

    # run the refresh stage once
    refresh_publisher_and_dataset_info()

    number_publishers = db.getNumPublishers(db.getDirectConnection())
    assert number_publishers == 1

    publisher_name_from_db = get_publisher_name(config, "aaaaaaaa-pub1-4cb6-b08e-496e634e0cf0")
    assert publisher_name_from_db == "test-org-1"


@pytest.mark.parametrize(
    "setup_and_teardown", ["src/tests/integration/unified-pipeline-test-env-setup.env"], indirect=True
)
def test_publisher_add_multiple(setup_and_teardown, mocker):  # noqa: F811

    patch_bds_index_url(mocker, "/bulk-data-service-indices/minimal-3-activity-files-3-publishers-200-status.json")

    # run the refresh stage once
    refresh_publisher_and_dataset_info()

    number_publishers = db.getNumPublishers(db.getDirectConnection())
    assert number_publishers == 3

    publisher_name_from_db = get_publisher_name(config, "aaaaaaaa-pub1-4cb6-b08e-496e634e0cf0")
    assert publisher_name_from_db == "test-org-1"

    publisher_name_from_db = get_publisher_name(config, "bbbbbbbb-pub2-4cb6-b08e-496e634e0cf0")
    assert publisher_name_from_db == "test-org-2"

    publisher_name_from_db = get_publisher_name(config, "cccccccc-pub3-4cb6-b08e-496e634e0cf0")
    assert publisher_name_from_db == "test-org-3"


@pytest.mark.parametrize(
    "setup_and_teardown", ["src/tests/integration/unified-pipeline-test-env-setup.env"], indirect=True
)
def test_publisher_remove_old(setup_and_teardown, mocker):  # noqa: F811

    # in sync_publishers(), there is code which deletes datasets (for removed publishers) from Azure
    # storage. For the lakified activities, this code uses blob_service_client.find_blobs_by_tags,
    # but this is not implemented by Azurite yet. So our tests cannot test the deletion of AZ blobs.
    # There is a PR which will implement this on Azurite
    # (https://github.com/Azure/Azurite/pull/2311) and when it is merged, we will be able to improve
    # these tests
    mocker.patch("azure.storage.blob.BlobServiceClient.from_connection_string")

    patch_bds_index_url(mocker, "/bulk-data-service-indices/minimal-3-activity-files-3-publishers-200-status.json")

    # run the refresh stage once
    refresh_publisher_and_dataset_info()

    number_publishers = db.getNumPublishers(db.getDirectConnection())
    assert number_publishers == 3

    patch_bds_index_url(mocker, "/bulk-data-service-indices/minimal-2-activity-files-2-publishers-200-status.json")
    patch_bds_reporting_org_index_url(
        mocker, "/bulk-data-service-indices/reporting-org-index-02-1-reporting-org-removed.json"
    )

    # re-run the refresh stage once
    refresh_publisher_and_dataset_info()

    number_publishers = db.getNumPublishers(db.getDirectConnection())
    assert number_publishers == 2

    publisher_name_from_db = get_publisher_name(config, "aaaaaaaa-pub1-4cb6-b08e-496e634e0cf0")
    assert publisher_name_from_db == "test-org-1"

    publisher_name_from_db = get_publisher_name(config, "bbbbbbbb-pub2-4cb6-b08e-496e634e0cf0")
    assert publisher_name_from_db == "test-org-2"


@pytest.mark.parametrize(
    "setup_and_teardown", ["src/tests/integration/unified-pipeline-test-env-setup.env"], indirect=True
)
def test_update_stops_if_num_publishers_drops_by_50_percent(setup_and_teardown, mocker):  # noqa: F811

    patch_bds_index_url(mocker, "/bulk-data-service-indices/minimal-3-activity-files-3-publishers-200-status.json")

    # run the refresh stage once
    refresh_publisher_and_dataset_info()

    number_publishers = db.getNumPublishers(db.getDirectConnection())
    assert number_publishers == 3

    patch_bds_index_url(mocker, "/bulk-data-service-indices/minimal-1-activity-file-200-status.json")

    # re-run the refresh stage once
    refresh_publisher_and_dataset_info()

    number_publishers = db.getNumPublishers(db.getDirectConnection())
    assert number_publishers == 3


@pytest.mark.parametrize(
    "setup_and_teardown", ["src/tests/integration/unified-pipeline-test-env-setup.env"], indirect=True
)
def test_dataset_add_single(setup_and_teardown, mocker):  # noqa: F811

    patch_bds_index_url(mocker, "/bulk-data-service-indices/minimal-1-activity-file-200-status.json")

    # run the refresh stage once
    refresh_publisher_and_dataset_info()

    number_datasets = db.getNumDocuments(db.getDirectConnection())
    assert number_datasets == 1

    dataset = get_dataset(config, "aaaaaaaa-9bc5-41dd-9b16-9d9ea28cdf0a")
    assert dataset is not None, "Dataset doesn't exist in the database"
    assert dataset["hash"] == "fbd23a51c3cc20d6fd53bd3d5c8b1568ec802170"
    assert dataset["cached_dataset_url"] == "http://localhost:3005/iati-xml-files/test-org-1-activity-1.xml"
    assert dataset["url"] == "http://test-org-1.org/iati-xml-files/test-org-1-activity-1.xml"
    assert dataset["name"] == "test-org-1-act-01"
    assert dataset["publisher"] == "aaaaaaaa-pub1-4cb6-b08e-496e634e0cf0"


@pytest.mark.parametrize(
    "setup_and_teardown", ["src/tests/integration/unified-pipeline-test-env-setup.env"], indirect=True
)
def test_dataset_add_single_non_200(setup_and_teardown, mocker):  # noqa: F811

    patch_bds_index_url(mocker, "/bulk-data-service-indices/minimal-1-activity-file-bds-failed-to-download.json")

    # run the refresh stage once
    refresh_publisher_and_dataset_info()

    number_datasets = db.getNumDocuments(db.getDirectConnection())
    assert number_datasets == 1

    dataset = get_dataset(config, "aaaaaaaa-9bc5-41dd-9b16-9d9ea28cdf0a")
    assert dataset is not None, "Dataset doesn't exist in the database"
    assert dataset["hash"] == ""
    assert dataset["cached_dataset_url"] is None
    assert dataset["url"] == "http://test-org-1.org/iati-xml-files/test-org-1-activity-1-404.xml"
    assert dataset["name"] == "test-org-1-act-01"
    assert dataset["publisher"] == "aaaaaaaa-pub1-4cb6-b08e-496e634e0cf0"


@pytest.mark.parametrize(
    "setup_and_teardown", ["src/tests/integration/unified-pipeline-test-env-setup.env"], indirect=True
)
def test_dataset_update_single(setup_and_teardown, mocker):  # noqa: F811

    # library.refresher imports addCore from library.solrize; by the time we get here, this import
    # has already happened, so we have to mock on library.refresher and not where code is
    mocker.patch("library.refresher.addCore")

    # in sync_publishers(), there is code which searches for and deletes datasets from Azure
    # storage by tag, but this is not implemented by Azurite yet, so we can't test this.
    # There is a PR which will implement this on Azurit and when it is merged, we will be able
    # to improve these tests (https://github.com/Azure/Azurite/pull/2311)
    mocker.patch("azure.storage.blob.BlobServiceClient.from_connection_string")

    patch_bds_index_url(mocker, "/bulk-data-service-indices/minimal-1-activity-file-200-status.json")

    # run the refresh stage once
    refresh_publisher_and_dataset_info()

    dataset = get_dataset(config, "aaaaaaaa-9bc5-41dd-9b16-9d9ea28cdf0a")
    assert dataset is not None, "Dataset doesn't exist in the database"
    assert dataset["hash"] == "fbd23a51c3cc20d6fd53bd3d5c8b1568ec802170"

    patch_bds_index_url(mocker, "/bulk-data-service-indices/minimal-1-activity-file-200-status-modified.json")

    # run the refresh stage once
    refresh_publisher_and_dataset_info()

    dataset = get_dataset(config, "aaaaaaaa-9bc5-41dd-9b16-9d9ea28cdf0a")
    assert dataset is not None, "Dataset doesn't exist in the database"
    assert dataset["hash"] == "043e0e0fe9d02418d50c37c058dba8c9a02a5ee4"
    assert dataset["cached_dataset_url"] == "http://localhost:3005/iati-xml-files/test-org-1-activity-1-modified.xml"
    assert dataset["url"] == "http://test-org-1.org/iati-xml-files/test-org-1-activity-1-modified.xml"
    assert dataset["publisher"] == "aaaaaaaa-pub1-4cb6-b08e-496e634e0cf0"


@pytest.mark.parametrize(
    "setup_and_teardown", ["src/tests/integration/unified-pipeline-test-env-setup.env"], indirect=True
)
def test_dataset_add_multiple(setup_and_teardown, mocker):  # noqa: F811

    patch_bds_index_url(mocker, "/bulk-data-service-indices/minimal-3-activity-files-3-publishers-200-status.json")

    # run the refresh stage once
    refresh_publisher_and_dataset_info()

    number_datasets = db.getNumDocuments(db.getDirectConnection())
    assert number_datasets == 3

    dataset2 = get_dataset(config, "bbbbbbbb-9bc5-41dd-9b16-9d9ea28cdf0b")
    assert dataset2["hash"] == "2f70ee0c0ee83daaf0bbbdc8c87a52eef2377c7b"
    assert dataset2["cached_dataset_url"] == "http://localhost:3005/iati-xml-files/test-org-2-activity-1.xml"
    assert dataset2["url"] == "http://test-org-2.org/iati-xml-files/test-org-2-activity-1.xml"
    assert dataset2["name"] == "test-org-2-act-01"
    assert dataset2["publisher"] == "bbbbbbbb-pub2-4cb6-b08e-496e634e0cf0"

    dataset3 = get_dataset(config, "cccccccc-9bc5-41dd-9b16-9d9ea28cdf0c")
    assert dataset3["hash"] == "5664beb2ec52ad5f158c1615666d69458a2cb188"
    assert dataset3["cached_dataset_url"] == "http://localhost:3005/iati-xml-files/test-org-3-activity-1.xml"
    assert dataset3["url"] == "http://test-org-3.org/iati-xml-files/test-org-3-activity-1.xml"
    assert dataset3["name"] == "test-org-3-act-01"
    assert dataset3["publisher"] == "cccccccc-pub3-4cb6-b08e-496e634e0cf0"
