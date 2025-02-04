import json
import multiprocessing
import time
import traceback
from datetime import datetime

import chardet
import requests
from azure.core import exceptions as AzureExceptions
from azure.storage.blob import BlobServiceClient
from psycopg2 import Error as DbError

import library.db as db
from constants.config import config
from constants.version import __version__
from library.bulk_data_service import (
    get_dataset_index,
    get_dataset_list_etag,
    get_reporting_orgs_supplemented_metadata,
    populate_reporting_orgs_with_dataset_count,
)
from library.http import requests_retry_session
from library.logger import getLogger
from library.prometheus import set_prom_metric
from library.solrize import addCore
from library.utils import find_object_by_key

multiprocessing.set_start_method("spawn", True)


logger = getLogger("refresher")


def clean_containers_by_id(
    blob_service_client, document_id, containers=[config["SOURCE_CONTAINER_NAME"], config["CLEAN_CONTAINER_NAME"]]
):
    for container_name in containers:
        try:
            container_client = blob_service_client.get_container_client(container_name)
            filter_config = "@container='" + str(container_name) + "' and document_id='" + document_id + "'"
            assoc_blobs = list(blob_service_client.find_blobs_by_tags(filter_config, timeout=1))
            if len(assoc_blobs) > 0:
                logger.info(f"Removing document ID {document_id} from {container_name} container.")
                container_client.delete_blobs(assoc_blobs[0]["name"])
        except Exception as e:
            logger.warning(f"Failed to clean up {container_name} for id: {document_id}. {e}")


def clean_datasets(stale_datasets, changed_datasets):
    blob_service_client = BlobServiceClient.from_connection_string(config["STORAGE_CONNECTION_STR"])

    # clean up activity lake for stale_datasets, doesn't need to be done for
    # changed_datasets as iati-identifiers hash probably didn't change
    if len(stale_datasets) > 0:
        try:
            logger.info("Removing " + str(len(stale_datasets)) + " stale documents from lake")
            lake_container_client = blob_service_client.get_container_client(config["ACTIVITIES_LAKE_CONTAINER_NAME"])

            for file_id, file_hash in stale_datasets:
                filter_config = (
                    "@container='"
                    + str(config["ACTIVITIES_LAKE_CONTAINER_NAME"])
                    + "' and dataset_hash='"
                    + file_hash
                    + "'"
                )
                assoc_blobs = blob_service_client.find_blobs_by_tags(filter_config)
                name_list = [blob["name"] for blob in assoc_blobs]
                max_blob_delete = config["REFRESHER"]["MAX_BLOB_DELETE"]
                if len(name_list) > max_blob_delete:
                    chunked_list = [
                        name_list[i : i + max_blob_delete] for i in range(0, len(name_list), max_blob_delete)
                    ]
                    for list_chunk in chunked_list:
                        lake_container_client.delete_blobs(*list_chunk)
                else:
                    lake_container_client.delete_blobs(*name_list)
        except Exception as e:
            logger.warning("Failed to clean up lake for id: " + file_id + " and hash: " + file_hash + " " + str(e))

    # clean up source xml and solr for both stale and changed datasets
    if len(stale_datasets) > 0 or len(changed_datasets) > 0:
        logger.info(
            "Removing " + str(len(stale_datasets)) + " stale and " + str(len(changed_datasets)) + " changed documents"
        )

        # prep solr connections
        try:
            solr_cores = {}
            solr_cores["activity"] = addCore("activity")
            explode_elements = json.loads(config["SOLRIZE"]["EXPLODE_ELEMENTS"])

            for core_name in explode_elements:
                solr_cores[core_name] = addCore(core_name)

            for core_name in solr_cores:
                solr_cores[core_name].ping()
        except Exception as e:
            logger.error("ERROR with Initialising Solr to delete stale or changed documents")
            print(traceback.format_exc())
            if hasattr(e, "args"):
                logger.error(e.args[0])
            if hasattr(e, "message"):
                logger.error(e.message)
            if hasattr(e, "msg"):
                logger.error(e.msg)
            try:
                logger.warning(e.args[0])
            except:
                pass

        source_container_client = blob_service_client.get_container_client(config["SOURCE_CONTAINER_NAME"])
        clean_container_client = blob_service_client.get_container_client(config["CLEAN_CONTAINER_NAME"])

        # stale documents are more important to clean up as they won't be caught later in pipeline
        for file_id, file_hash in stale_datasets:
            try:
                # remove from source and clean containers
                try:
                    source_container_client.delete_blob(file_hash + ".xml")
                except AzureExceptions.ResourceNotFoundError:
                    logger.error(
                        f"Can not delete blob from {config['SOURCE_CONTAINER_NAME']} as does not exist: "
                        f"{file_hash}.xml and id: {file_id}. Attempting to delete by ID."
                    )
                    clean_containers_by_id(blob_service_client, file_id, containers=[config["SOURCE_CONTAINER_NAME"]])
                try:
                    clean_container_client.delete_blob(file_hash + ".xml")
                except AzureExceptions.ResourceNotFoundError:
                    logger.warning(
                        f"Can not delete blob from {config['CLEAN_CONTAINER_NAME']} as does not exist: "
                        f"{file_hash}.xml and id: {file_id}. Attempting to delete by ID."
                    )
                    clean_containers_by_id(blob_service_client, file_id, containers=[config["CLEAN_CONTAINER_NAME"]])

                # remove from all solr collections
                for core_name in solr_cores:
                    try:
                        solr_cores[core_name].delete(q="iati_activities_document_id:" + file_id)
                    except:
                        logger.error(
                            "Failed to remove stale docs from solr with hash: "
                            + file_hash
                            + " and id: "
                            + file_id
                            + " from core with name "
                            + core_name
                        )
                # Maybe we should clean up the last_solrize_end field here, as they are now gone?
                # However, we don't have to as if you look at how clean_datasets is called,
                #   in each case right afterwards the rows are removed from the DB anyway.
            except Exception:
                logger.error(
                    "Unknown error occurred while attempting to remove stale document ID {} "
                    "from Source and SOLR".format(file_id)
                )
        for file_id, file_hash in changed_datasets:
            try:
                # remove from source and clean containers
                try:
                    source_container_client.delete_blob(file_hash + ".xml")
                except AzureExceptions.ResourceNotFoundError:
                    logger.warning(
                        f"Can not delete blob from {config['SOURCE_CONTAINER_NAME']} as does not "
                        f"exist: {file_hash}.xml and id: {file_id}. Attempting to delete by ID."
                    )
                    clean_containers_by_id(blob_service_client, file_id, containers=[config["SOURCE_CONTAINER_NAME"]])
                try:
                    clean_container_client.delete_blob(file_hash + ".xml")
                except AzureExceptions.ResourceNotFoundError:
                    logger.info(
                        f"Can not delete blob from {config['CLEAN_CONTAINER_NAME']} as does not exist: "
                        f"{file_hash}.xml and id: {file_id}. Attempting to delete by ID."
                    )
                    clean_containers_by_id(blob_service_client, file_id, containers=[config["CLEAN_CONTAINER_NAME"]])

                # Don't remove from any solr collections!
                # We want old data to stay in the system until new data is ready to be put in Solr.
                # The Solrize stage will delete the old data from Solr.

            except Exception:
                logger.error(
                    "Unknown error occurred while attempting to remove changed "
                    "document ID {} from Source and SOLR".format(file_id)
                )


def sync_publishers(publishers_by_short_name: dict[str, dict]):
    conn = db.getDirectConnection()
    start_dt = datetime.now()

    known_publishers_num = db.getNumPublishers(conn)

    set_prom_metric("registered_publishers", len(publishers_by_short_name))

    logger.info(
        "Number publishers in DB: {}, in Bulk Data Service: {}".format(
            known_publishers_num, len(publishers_by_short_name)
        )
    )

    if (
        len(publishers_by_short_name)
        < (config["REFRESHER"]["PUBLISHER_SAFETY_PERCENTAGE"] / 100) * known_publishers_num
    ):
        logger.error(
            "Number of publishers reported by Bulk Data Service: "
            + str(len(publishers_by_short_name))
            + ", is less than "
            + str(config["REFRESHER"]["PUBLISHER_SAFETY_PERCENTAGE"])
            + r"% of previously known publishers: "
            + str(known_publishers_num)
            + ", NOT Updating Publishers at this time."
        )
        conn.close()
        raise

    for publisher_short_name, publisher in publishers_by_short_name.items():

        if (
            config["REFRESHER"]["LIMIT_ENABLED"] == "yes"
            and publisher_short_name not in config["REFRESHER"]["LIMIT_TO_REPORTING_ORGS"]
        ):
            continue

        try:
            db.insertOrUpdatePublisher(conn, publisher, start_dt)
        except DbError as e:
            e_message = ""
            if e.pgerror is not None:
                e_message = e.pgerror
            elif hasattr(e, "args"):
                e_message = e.args[0]
            logger.warning("Failed to update publisher with name " + publisher_short_name + ": DbError : " + e_message)
            conn.rollback()
            conn.close()
            raise e
        except Exception as e:
            e_message = ""
            if hasattr(e, "args"):
                e_message = e.args[0]
            logger.error(
                "Failed to update publisher with name " + publisher_short_name + " : Unidentified Error: " + e_message
            )
            conn.close()
            raise e

    stale_datasets = db.getFilesFromPublishersNotSeenAfter(conn, start_dt)
    if len(stale_datasets) > 0:
        clean_datasets(stale_datasets, [])
    db.removePublishersNotSeenAfter(conn, start_dt)
    conn.close()


def sync_documents(dataset_list: list[dict]):
    conn = db.getDirectConnection()
    start_dt = datetime.now()

    all_datasets = [
        {
            "id": dataset_metadata["id"],
            "name": dataset_metadata["short_name"],
            "hash": (
                dataset_metadata["hash_excluding_generated_timestamp"]
                if dataset_metadata["hash_excluding_generated_timestamp"] != None
                else ""
            ),
            "bds_cache_url": dataset_metadata["url_xml"],
            "url": dataset_metadata["source_url"],
            "publisher_id": dataset_metadata["reporting_org_id"],
        }
        for dataset_metadata in dataset_list
    ]

    set_prom_metric("registered_datasets", len(all_datasets))

    known_documents_num = db.getNumDocuments(conn)

    logger.info(
        "Number datasets in DB (before update): {}, in Bulk Data Service: {}".format(
            known_documents_num, len(all_datasets)
        )
    )

    if len(all_datasets) < (config["REFRESHER"]["DOCUMENT_SAFETY_PERCENTAGE"] / 100) * known_documents_num:
        logger.error(
            "Number of documents reported by Bulk Data Service: "
            + str(len(all_datasets))
            + ", is less than "
            + str(config["REFRESHER"]["DOCUMENT_SAFETY_PERCENTAGE"])
            + r"% of previously known documents: "
            + str(known_documents_num)
            + ", NOT Updating Documents at this time."
        )
        conn.close()
        raise

    changed_datasets = []

    for dataset in all_datasets:

        if (
            config["REFRESHER"]["LIMIT_ENABLED"] == "yes"
            and dataset["name"] not in config["REFRESHER"]["LIMIT_TO_DATASETS"]
        ):
            continue

        try:
            changed = db.getFileWhereHashChanged(conn, dataset["id"], dataset["hash"])
            if changed is not None:
                changed_datasets += [changed]
            db.insertOrUpdateDocument(conn, start_dt, dataset)
        except DbError as e:
            e_message = ""
            if e.pgerror is not None:
                e_message = e.pgerror
            logger.warning(
                "Failed to sync document with hash: "
                + dataset["hash"]
                + " and id: "
                + dataset["id"]
                + " : "
                + e_message
            )
            conn.rollback()
        except Exception:
            logger.error(
                "Failed to sync document with hash: "
                + dataset["hash"]
                + " and id: "
                + dataset["id"]
                + " : Unidentified Error"
            )

    set_prom_metric("datasets_changed", len(changed_datasets))

    stale_datasets = db.getFilesNotSeenAfter(conn, start_dt)

    known_documents_num_after = db.getNumDocuments(conn)

    logger.info(
        "Number datasets added: {}, updated: {}, deleted (stale): {}".format(
            known_documents_num_after - known_documents_num, len(changed_datasets), len(stale_datasets)
        )
    )

    if len(changed_datasets) > 0 or len(stale_datasets) > 0:
        clean_datasets(stale_datasets, changed_datasets)

    db.removeFilesNotSeenAfter(conn, start_dt)

    conn.close()


def refresh_publisher_and_dataset_info():
    logger.info("Begin refresh cycle")

    dataset_index = get_dataset_index()

    reporting_orgs_index_supplemented = get_reporting_orgs_supplemented_metadata()

    if (
        dataset_index["index_created_unix_timestamp"]
        != reporting_orgs_index_supplemented["index_created_unix_timestamp"]
    ):
        logger.info("Dataset and reporting orgs indices are from different runs of Bulk Data Service")
        logger.info("Skipping refresh")
        return

    populate_reporting_orgs_with_dataset_count(reporting_orgs_index_supplemented, dataset_index["datasets"])

    logger.info("Updating list of publishers from the Bulk Data Service...")
    try:
        sync_publishers(reporting_orgs_index_supplemented["reporting_orgs"])
        logger.info("Publishers synced.")
    except Exception:
        logger.error("Publishers failed to sync.")

    logger.info("Updating list of documents from the Bulk Data Service...")
    try:
        sync_documents(dataset_index["datasets"])
        logger.info("Documents synced.")
    except Exception:
        logger.error("Documents failed to sync.")

    logger.info("End refresh.")


def reload(retry_errors):
    logger.info("Start reload (with retry_errors = {})...".format(retry_errors))
    blob_service_client = BlobServiceClient.from_connection_string(config["STORAGE_CONNECTION_STR"])
    conn = db.getDirectConnection()

    datasets = db.getRefreshDataset(conn, retry_errors)

    set_prom_metric("datasets_to_download", len(datasets))

    chunked_datasets = list(split(datasets, config["REFRESHER"]["PARALLEL_PROCESSES"]))

    processes = []

    logger.info(
        "Downloading "
        + str(len(datasets))
        + " files in a maximum of "
        + str(config["REFRESHER"]["PARALLEL_PROCESSES"])
        + " processes."
    )

    for chunk in chunked_datasets:
        if len(chunk) == 0:
            continue
        process = multiprocessing.Process(target=download_chunk, args=(chunk, blob_service_client, datasets))
        process.start()
        processes.append(process)

    for process in processes:
        process.join()

    logger.info("Reload complete.")


def service_loop():
    logger.info("Start service loop")
    count = 0
    service_loop_sleep = int(config["REFRESHER"]["SERVICE_LOOP_SLEEP"])
    existing_bds_etag = None

    while True:
        count = count + 1

        latest_bds_etag = get_dataset_list_etag()

        if count > config["REFRESHER"]["RETRY_ERRORS_AFTER_LOOP"]:
            logger.info("Reached threshold for re-trying failed downloads, so running refresh")

            refresh_publisher_and_dataset_info()
            reload(True)
            count = 0

        elif existing_bds_etag != latest_bds_etag:
            logger.info("Bulk Data Service index ETag changed to {}, so running refresh".format(latest_bds_etag))

            refresh_publisher_and_dataset_info()
            reload(False)
            existing_bds_etag = latest_bds_etag

        else:
            logger.info("Bulk Data Service index ETag unchanged ({}) so skipping refresh".format(existing_bds_etag))

        logger.info("Sleeping for {} seconds".format(service_loop_sleep))
        time.sleep(service_loop_sleep)


def split(lst, n):
    k, m = divmod(len(lst), n)
    return (lst[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n))


def download_chunk(chunk, blob_service_client, datasets):
    conn = db.getDirectConnection()

    for dataset in chunk:

        id = dataset[0]
        hash = dataset[1]
        bds_cache_url = dataset[2]

        if bds_cache_url is None:
            db.updateFileAsDownloadError(conn, id, 4)
            continue

        try:
            blob_client = blob_service_client.get_blob_client(
                container=config["SOURCE_CONTAINER_NAME"], blob=hash + ".xml"
            )
            headers = {"User-Agent": "iati-unified-platform-refresher/" + __version__["number"]}
            logger.info("Trying to download url: " + bds_cache_url + " and hash: " + hash + " and id: " + id)
            download_response = requests_retry_session(retries=3).get(
                url=bds_cache_url, headers=headers, timeout=int(config["REFRESHER"]["BULK_DATA_SERVICE_HTTP_TIMEOUT"])
            )
            download_xml = download_response.content
            if download_response.status_code == 200:
                try:
                    detect_result = chardet.detect(download_xml)
                    charset = detect_result["encoding"]
                    # log error for undetectable charset, prevent PDFs from being downloaded to Unified Platform
                    if charset is None:
                        db.updateFileAsDownloadError(conn, id, 2)
                        clean_containers_by_id(blob_service_client, id)
                        continue
                except:
                    charset = "UTF-8"
                blob_client.upload_blob(download_xml, overwrite=True, encoding=charset)
                blob_client.set_blob_tags({"document_id": id})
                db.updateFileAsDownloaded(conn, id)
                logger.debug("Successfully downloaded url: " + bds_cache_url + " and hash: " + hash + " and id: " + id)
            else:
                db.updateFileAsDownloadError(conn, id, download_response.status_code)
                clean_containers_by_id(blob_service_client, id)
                logger.debug(
                    "HTTP "
                    + str(download_response.status_code)
                    + " when downloading url: "
                    + bds_cache_url
                    + " and hash: "
                    + hash
                    + " and id: "
                    + id
                )
        except requests.exceptions.SSLError:
            logger.debug("SSLError while downloading url: " + bds_cache_url + " and hash: " + hash + " and id: " + id)
            db.updateFileAsDownloadError(conn, id, 1)
            clean_containers_by_id(blob_service_client, id)
        except requests.exceptions.ConnectionError:
            logger.debug(
                "ConnectionError while downloading url: " + bds_cache_url + " and hash: " + hash + " and id: " + id
            )
            db.updateFileAsDownloadError(conn, id, 0)
            clean_containers_by_id(blob_service_client, id)
        except requests.exceptions.InvalidSchema as e:
            logger.warning("Failed to download file with hash: " + hash + " and id: " + id + " Error: " + e.args[0])
            db.updateFileAsDownloadError(conn, id, 3)
            clean_containers_by_id(blob_service_client, id)
        except AzureExceptions.ResourceNotFoundError as e:
            logger.debug(
                "ResourceNotFoundError while downloading url: "
                + bds_cache_url
                + " and hash: "
                + hash
                + " and id: "
                + id
            )
            db.updateFileAsDownloadError(conn, id, e.status_code)
            clean_containers_by_id(blob_service_client, id)
        except AzureExceptions.ServiceResponseError as e:
            logger.warning(
                "Failed to upload file with url: "
                + bds_cache_url
                + " and hash: "
                + hash
                + " and id: "
                + id
                + " - Azure error message: "
                + e.message
            )
        except Exception as e:
            e_message = ""
            if hasattr(e, "args"):
                e_message = e.args[0]
            logger.warning(
                "Failed to upload or download file with url: "
                + bds_cache_url
                + " and hash: "
                + hash
                + " and id: "
                + id
                + " Error: "
                + e_message
            )
