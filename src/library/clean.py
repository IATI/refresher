import time
import traceback
from io import BytesIO
from multiprocessing import Process

from azure.core import exceptions as AzureExceptions
from azure.storage.blob import BlobServiceClient
from lxml import etree

import library.db as db
import library.utils as utils
from constants.config import config
from library.logger import getLogger

logger = getLogger("clean")


def copy_valid_documents(documents):
    """copy valid activities documents to clean container storage

    Args:
        documents (list): list of tuples that contain the hash and id of the documents to copy
    """
    try:
        conn = db.getDirectConnection()

        for document in documents:
            hash = document[0]
            id = document[1]

            logger.info(f"Copying source xml to clean container for valid activity document id: {id} and hash: {hash}")
            blob_name = f"{hash}.xml"

            db.startClean(conn, id)

            try:
                blob_service_client = BlobServiceClient.from_connection_string(config["STORAGE_CONNECTION_STR"])

                source_blob_client = blob_service_client.get_blob_client(
                    container=config["SOURCE_CONTAINER_NAME"], blob=blob_name
                )

                clean_blob = blob_service_client.get_blob_client(
                    container=config["CLEAN_CONTAINER_NAME"], blob=blob_name
                )
                clean_blob.start_copy_from_url(source_blob_client.url)
                clean_blob.set_blob_tags({"document_id": id})
            except AzureExceptions.ResourceNotFoundError:
                err_msg = f"Blob not found for hash: {hash} and id: {id} updating as Not Downloaded for the refresher to pick up."
                logger.warning(err_msg)
                db.updateCleanError(conn, id, err_msg)
                db.updateFileAsNotDownloaded(conn, id)

            db.completeClean(conn, id)
    except Exception as e:
        logger.error("ERROR with copying valid documents to clean storage")
        print(traceback.format_exc())
        if hasattr(e, "message"):
            logger.error(e.message)
        if hasattr(e, "msg"):
            logger.error(e.msg)
        try:
            logger.warning(e.args[0])
        except:
            pass


def copy_valid():
    """gets list of valid documents to copy and sets up up multiprocessing"""
    logger.info("Starting copy of valid activities documents to clean container...")

    conn = db.getDirectConnection()

    logger.info("Resetting those unfinished")

    db.resetUnfinishedCleans(conn)

    documents = db.getValidActivitiesDocsToCopy(conn)

    if config["CLEAN"]["PARALLEL_PROCESSES"] == 1:
        logger.info(f"Copying {len(documents)} valid IATI files in a single process.")
        copy_valid_documents(documents)
    else:
        chunked_doc_lists = list(utils.chunk_list(documents, config["CLEAN"]["PARALLEL_PROCESSES"]))

        processes = []

        logger.info(
            f"Copying {len(documents)} valid IATI files in a maximum of {config['CLEAN']['PARALLEL_PROCESSES']} parallel processes."
        )

        for chunk in chunked_doc_lists:
            if len(chunk) == 0:
                continue
            process = Process(target=copy_valid_documents, args=(chunk,))
            process.start()
            processes.append(process)

        for process in processes:
            process.join()

    conn.close()
    logger.info("copy_valid Finished.")


def clean_invalid_documents(documents):
    """removes invalid activities from documents and saves to clean container storage

    Args:
        documents (list): list of tuples that contain the hash, id, and validation index of the documents to clean
    """
    try:
        conn = db.getDirectConnection()

        for document in documents:
            hash = document[0]
            id = document[1]
            index = document[2]
            valid_dict = {act["index"]: act["valid"] for act in index if act["valid"] is True}

            logger.info(
                f"Copying {len(valid_dict)} valid of {len(index)} total activities xml to clean container for invalid activity document id: {id} and hash: {hash}"
            )

            db.startClean(conn, id)

            blob_name = hash + ".xml"

            try:
                blob_service_client = BlobServiceClient.from_connection_string(config["STORAGE_CONNECTION_STR"])
                blob_client = blob_service_client.get_blob_client(
                    container=config["SOURCE_CONTAINER_NAME"], blob=blob_name
                )

                downloader = blob_client.download_blob()

                large_parser = etree.XMLParser(huge_tree=True)
                root = etree.parse(BytesIO(downloader.content_as_bytes()), parser=large_parser)
                iati_activities_el = root.getroot()
                file_encoding = "utf-8"
            except AzureExceptions.ResourceNotFoundError:
                logger.warning(
                    f"Blob not found for hash: {hash} and id: {id} - updating as Not Downloaded for the refresher to pick up."
                )
                db.updateFileAsNotDownloaded(conn, id)
            except etree.XMLSyntaxError:
                logger.warning(f"Cannot parse entire XML for hash: {hash} id: {id}. ")
                try:
                    file_text, file_encoding = utils.get_text_from_blob(downloader, hash, True)
                except:
                    logger.warning(f"Can not identify charset for hash: {hash} id: {id} to remove invalid activities.")
                    db.updateCleanError(conn, id, "Could not parse")
                    continue

            # loop and save valid activities to clean doc
            activities = iati_activities_el.xpath("iati-activity")

            cleanDoc = etree.Element("iati-activities")

            for att in iati_activities_el.attrib:
                cleanDoc.attrib[att] = iati_activities_el.attrib[att]

            i = 0
            for activity in activities:
                if i in valid_dict:
                    cleanDoc.append(activity)
                i += 1

            if len(cleanDoc) == 0:
                logger.info(f"No valid activities for hash: {hash} id: {id}. ")
                db.updateCleanError(conn, id, "No valid activities")
                continue

            # save valid activities in a doc to "clean" container
            activities_xml = etree.tostring(cleanDoc, encoding=file_encoding)
            blob_client = blob_service_client.get_blob_client(container=config["CLEAN_CONTAINER_NAME"], blob=blob_name)
            blob_client.upload_blob(activities_xml, overwrite=True, encoding=file_encoding)
            blob_client.set_blob_tags({"dataset_hash": hash, "document_id": id})

            del iati_activities_el
            del activities
            del cleanDoc
            del i

            db.completeClean(conn, id)

    except Exception as e:
        logger.error("ERROR with cleaning invalid documents and saving to clean storage")
        print(traceback.format_exc())
        if hasattr(e, "message"):
            logger.error(e.message)
        if hasattr(e, "msg"):
            logger.error(e.msg)
        try:
            logger.warning(e.args[0])
        except:
            pass


def clean_invalid():
    """gets list of invalid documents to clean and sets up up multiprocessing"""
    logger.info("Starting clean of invalid activities documents to save to clean container...")

    conn = db.getDirectConnection()

    logger.info("Resetting those unfinished")

    db.resetUnfinishedCleans(conn)

    # get invalid docs that have valid activities
    documents = db.getInvalidActivitiesDocsToClean(conn)

    if config["CLEAN"]["PARALLEL_PROCESSES"] == 1:
        logger.info(f"Cleaning and storing {len(documents)} IATI files in a single process.")
        clean_invalid_documents(documents)
    else:
        chunked_doc_lists = list(utils.chunk_list(documents, config["CLEAN"]["PARALLEL_PROCESSES"]))

        processes = []

        logger.info(
            f"Cleaning and storing {len(documents)} IATI files in a maximum of {config['CLEAN']['PARALLEL_PROCESSES']} parallel processes."
        )

        for chunk in chunked_doc_lists:
            if len(chunk) == 0:
                continue
            process = Process(target=clean_invalid_documents, args=(chunk,))
            process.start()
            processes.append(process)

        for process in processes:
            process.join()

    conn.close()
    logger.info("clean_invalid Finished.")


def service_loop():
    logger.info("Start service loop")

    while True:
        copy_valid()
        clean_invalid()
        time.sleep(60)
