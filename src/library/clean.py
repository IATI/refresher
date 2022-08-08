import os
import time
import sys
import traceback
from signal import pause
from re import A
from multiprocessing import Process
from library.logger import getLogger
import datetime
import requests
from constants.config import config
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from itertools import islice
from azure.core import exceptions as AzureExceptions
from lxml import etree
from io import BytesIO
import library.db as db
import json
import library.utils as utils
import re

logger = getLogger()


def chunk_list(l, n):
    for i in range(0, n):
        yield l[i::n]


def process_hash_list(document_datasets):

    conn = db.getDirectConnection()

    for file_data in document_datasets:
        try:
            file_hash = file_data[0]
            downloaded = file_data[1]
            file_id = file_data[2]

            db.updateActivityLevelValidationStart(conn, file_hash)

            logger.info('Processing for individual activity indexing the critically invalid doc with hash: ' +
                        file_hash + ' and id: ' + file_id + ', downloaded at ' + downloaded.isoformat())
            blob_name = file_hash + '.xml'

            blob_service_client = BlobServiceClient.from_connection_string(
                config['STORAGE_CONNECTION_STR'])
            blob_client = blob_service_client.get_blob_client(
                container=config['SOURCE_CONTAINER_NAME'], blob=blob_name)

            downloader = blob_client.download_blob()

            try:
                large_parser = etree.XMLParser(huge_tree=True)
                root = etree.parse(
                    BytesIO(downloader.content_as_bytes()), parser=large_parser)
                iati_activities_el = root.getroot()
                file_encoding = 'utf-8'
            except etree.XMLSyntaxError as e:
                logger.warning('Cannot parse entire XML for hash {} doc {}, attempting regex activity extraction.'.format(
                    file_hash, file_id))
                try:
                    file_text, file_encoding = utils.get_text_from_blob(
                        downloader, file_hash, True)
                except:
                    logger.warning('Can not identify charset for ' +
                                   file_hash + '.xml for activity-level validation.')
                    db.updateActivityLevelValidationError(
                        conn, file_hash, 'Could not parse')
                    continue

                activities_matcher = re.compile(r'<iati-activities[\s\S]*?>')
                activities_element_match = re.findall(
                    activities_matcher, file_text)
                if len(activities_element_match) > 0:
                    iati_activities_el = etree.fromstring(
                        activities_element_match[0].encode(file_encoding) + b"</iati-activities>")
                else:
                    logger.warning('No IATI activities element found for hash {} doc {}. Cannot ALV.'.format(
                        file_hash, file_id))
                    db.updateActivityLevelValidationError(
                        conn, file_hash, 'Could not parse')
                    continue

                activity_matcher = re.compile(
                    r'<iati-activity[\s\S]*?>[\s\S]*?<\/iati-activity>')
                activity_element_match = re.findall(
                    activity_matcher, file_text)
                for activity_element_text in activity_element_match:
                    try:
                        act_el = etree.fromstring(
                            activity_element_text.encode(file_encoding))
                        iati_activities_el.append(act_el)
                    except Exception as e:
                        pass
            except Exception as e:
                print(e)
                logger.warning('Could not parse ' + file_hash + '.xml')
                db.updateActivityLevelValidationError(
                    conn, file_hash, 'Could not parse')
                continue

            activities_loop = iati_activities_el.xpath("iati-activity")
            activities = iati_activities_el.xpath("iati-activity")

            origLen = len(activities)
            for activity in activities_loop:
                singleActivityDoc = etree.Element('iati-activities')
                for att in iati_activities_el.attrib:
                    singleActivityDoc.attrib[att] = iati_activities_el.attrib[att]
                singleActivityDoc.append(activity)
                payload = etree.tostring(
                    singleActivityDoc, encoding=file_encoding, method="xml").decode()
                payload = "".join(json.dumps(payload).split("\\n"))
                payload = payload.replace('\\"', '"')
                payload = payload[1:]
                payload = payload[:-1]
                headers = {config['VALIDATION']['SCHEMA_VALIDATION_KEY_NAME']: config['VALIDATION']['SCHEMA_VALIDATION_KEY_VALUE']}
                response = requests.post(
                    config['VALIDATION']['SCHEMA_VALIDATION_URL'], data=payload, headers=headers)
                db.updateValidationRequestDate(conn, file_id)
                if response.status_code != 200:
                    activities.remove(activity)
                    continue
                response_data = response.json()
                if response_data['valid'] == False:
                    activities.remove(activity)

            cleanDoc = etree.Element('iati-activities')

            for att in iati_activities_el.attrib:
                cleanDoc.attrib[att] = iati_activities_el.attrib[att]

            for activity in activities:
                cleanDoc.append(activity)

            logger.info(str(len(activities)) + ' of ' + str(origLen) +
                        ' parsable activities valid for hash: ' + file_hash + ' and id: ' + file_id)
            if len(activities) == 0:  # To prevent overwriting content with blank element
                db.updateActivityLevelValidationError(
                    conn, file_hash, 'No valid activities')
                continue
            activities_xml = etree.tostring(cleanDoc, encoding=file_encoding)
            blob_client = blob_service_client.get_blob_client(
                container=config['SOURCE_CONTAINER_NAME'], blob=blob_name)
            blob_client.upload_blob(
                activities_xml, overwrite=True, encoding=file_encoding)
            blob_client.set_blob_tags({"dataset_hash": file_hash})

            try:
                del root
            except NameError:
                pass
            try:
                del activities_element_match
            except NameError:
                pass
            try:
                del activity_element_match
            except NameError:
                pass
            try:
                del act_el
            except NameError:
                pass
            del iati_activities_el
            del activities
            del activities_loop
            del cleanDoc

            db.updateActivityLevelValidationEnd(conn, file_hash)

        except (AzureExceptions.ResourceNotFoundError) as e:
            logger.warning('Blob not found for hash ' + file_hash + ' and id: ' +
                           file_id + ' - updating as Not Downloaded for the refresher to pick up.')
            db.updateFileAsNotDownloaded(conn, file_id)
        except Exception as e:
            logger.error('ERROR with validating ' +
                         file_hash + ' and id: ' + file_id)
            print(traceback.format_exc())
            if hasattr(e, 'message'):
                logger.error(e.message)
                db.updateActivityLevelValidationError(
                    conn, file_hash, e.message)
            if hasattr(e, 'msg'):
                logger.error(e.msg)
                db.updateActivityLevelValidationError(conn, file_hash, e.msg)
            try:
                logger.warning(e.args[0])
                db.updateActivityLevelValidationError(
                    conn, file_hash, e.args[0])
            except:
                db.updateActivityLevelValidationError(
                    conn, file_hash, 'Unknown error')

    conn.close()


def main():
    logger.info(
        "Starting ")

    conn = db.getDirectConnection()

    file_hashes = db.getInvalidDatasetsForActivityLevelVal(
        conn, config['VALIDATION']['SAFETY_CHECK_PERIOD'])

    if config['VALIDATION']['ACTIVITY_LEVEL_PARALLEL_PROCESSES'] == 1:
        logger.info("Processing " + str(len(file_hashes)) +
                    " IATI files in a single process for activity level validation")
        process_hash_list(file_hashes)
    else:
        chunked_hash_lists = list(chunk_list(
            file_hashes, config['VALIDATION']['PARALLEL_PROCESSES']))

        processes = []

        logger.info("Processing " + str(len(file_hashes)) + " IATI files in a maximum of " + str(
            config['VALIDATION']['PARALLEL_PROCESSES']) + " parallel processes for activity level validation")

        for chunk in chunked_hash_lists:
            if len(chunk) == 0:
                continue
            process = Process(target=process_hash_list, args=(chunk,))
            process.start()
            processes.append(process)

        finished = False
        while finished == False:
            time.sleep(2)
            finished = True
            for process in processes:
                process.join(timeout=0)
                if process.is_alive():
                    finished = False

    conn.close()
    logger.info("Finished.")

# copy valid activities docs to clean container storage


def copy_valid():
    try:
        logger.info(
            "Starting copy of valid activities documents to clean container...")

        conn = db.getDirectConnection()

        db.resetUnfinishedCleans(conn)

        documents = db.getValidActivitiesDocsToClean(conn)

        for document in documents:
            hash = document[0]
            id = document[1]

            logger.info(
                f"Copying source xml to clean container for valid activity document id: {id} and hash: {hash}")
            blob_name = f"{hash}.xml"

            db.startClean(conn, id)

            try:
                blob_service_client = BlobServiceClient.from_connection_string(
                    config['STORAGE_CONNECTION_STR'])

                source_blob_client = blob_service_client.get_blob_client(
                    container=config['SOURCE_CONTAINER_NAME'], blob=blob_name)

                clean_blob = blob_service_client.get_blob_client(
                    container=config['CLEAN_CONTAINER_NAME'], blob=blob_name)
                clean_blob.start_copy_from_url(source_blob_client.url)
                clean_blob.set_blob_tags({"document_id": id})
            except (AzureExceptions.ResourceNotFoundError) as e:
                err_msg = f"Blob not found for hash: {hash} and id: {id} updating as Not Downloaded for the refresher to pick up."
                logger.warning(
                    err_msg)
                db.updateCleanError(conn, id, err_msg)
                db.updateFileAsNotDownloaded(conn, id)

            db.completeClean(conn, id)
    except Exception as e:
        logger.error(f"ERROR with copying valid documents to clean storage")
        print(traceback.format_exc())
        if hasattr(e, 'message'):
            logger.error(e.message)
        if hasattr(e, 'msg'):
            logger.error(e.msg)
        try:
            logger.warning(e.args[0])
        except:
            pass


def service_loop():
    logger.info("Start service loop")

    while True:
        copy_valid()
        time.sleep(60)
