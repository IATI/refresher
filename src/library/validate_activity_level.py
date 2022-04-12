import os, time, sys, traceback
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
from azure.storage.queue import QueueServiceClient

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

            logger.info('Processing for individual activity indexing the critically invalid doc with hash ' + file_hash + ', downloaded at ' + downloaded.isoformat())
            blob_name = file_hash + '.xml'

            blob_service_client = BlobServiceClient.from_connection_string(config['STORAGE_CONNECTION_STR'])
            blob_client = blob_service_client.get_blob_client(container=config['SOURCE_CONTAINER_NAME'], blob=blob_name)

            downloader = blob_client.download_blob()

            try:
                root = etree.parse(BytesIO(downloader.content_as_bytes()))
            except Exception as e:
                print(e)
                logger.warning('Could not parse ' + file_hash + '.xml')
                continue

            iati_activities_el = root.getroot()
            activities = root.xpath("iati-activity")

            origLen = len(activities)        

            for activity in activities:
                singleActivityDoc = etree.Element('iati-activities')

                for att in iati_activities_el.attrib:
                    singleActivityDoc.attrib[att] = iati_activities_el.attrib[att]

                singleActivityDoc.append(activity)

                payload = etree.tostring(singleActivityDoc, encoding="utf8", method="xml").decode()

                payload = "".join(json.dumps(payload).split("\\n"))
                payload = payload.replace('\\"', '"')
                payload = payload[1:]
                payload = payload[:-1]

                headers = { config['VALIDATION']['SCHEMA_VALIDATION_KEY_NAME']: config['VALIDATION']['SCHEMA_VALIDATION_KEY_VALUE'] }
                response = requests.post(config['VALIDATION']['SCHEMA_VALIDATION_URL'], data = payload, headers=headers)
                db.updateValidationRequestDate(conn, file_hash)

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

            logger.info(str(len(activities)) + ' of ' + str(origLen) + ' activities valid for ' + file_hash)
  
            activities_xml = etree.tostring(cleanDoc)
            blob_client = blob_service_client.get_blob_client(container=config['SOURCE_CONTAINER_NAME'], blob=blob_name)
            blob_client.upload_blob(activities_xml, overwrite=True)
            blob_client.set_blob_tags({"dataset_hash": file_hash})           

            del root
            del iati_activities_el
            del activities
            del cleanDoc          

            db.updateActivityLevelValidationState(conn, file_hash)         
            
        except (AzureExceptions.ResourceNotFoundError) as e:
            logger.warning('Blob not found for hash ' + file_hash + ' - updating as Not Downloaded for the refresher to pick up.')
            db.updateFileAsNotDownloaded(conn, file_id)
        except Exception as e:
            logger.error('ERROR with validating ' + file_hash)
            print(traceback.format_exc())
            if hasattr(e, 'message'):                         
                logger.error(e.message)
            if hasattr(e, 'msg'):                         
                logger.error(e.msg)
            try:
                logger.warning(e.args[0])
            except:
                pass        

    conn.close()

def service_loop():
    logger.info("Start service loop")

    while True:
        main()            
        time.sleep(60)

def main():
    logger.info("Starting validation of critically invalid docs at activity level...")

    conn = db.getDirectConnection()

    queue_service_client = QueueServiceClient.from_connection_string(config['STORAGE_CONNECTION_STR'])           
    queue_client = queue_service_client.get_queue_client("publisher-black-flag-remove")
    
    messages = queue_client.receive_messages()

    for message in messages:
        try:
            logger.info('Received message to remove black flag for publisher id ' + message.content)
            db.removeBlackFlag(conn, message.content)
            logger.info("Dequeueing message: " + message.content)
            queue_client.delete_message(message.id, message.pop_receipt)
        except Exception as e:
            logger.warning('Could not process message with id  ' + message.id)
            continue

    db.blackFlagDubiousPublishers(conn, config['VALIDATION']['ALV_THRESHOLD'], config['VALIDATION']['ALV_PERIOD'])

    black_flags = db.getUnnotifiedBlackFlags(conn)
    
    for black_flag in black_flags:
        notification = {
            "type": "NEW_BLACK_FLAG",
            "data": {
                "publisherId": black_flag[0],
                "reason": "Over " + str(config['VALIDATION']['ALV_THRESHOLD']) + " critical documents in the last " + str(config['VALIDATION']['ALV_PERIOD']) + " hours."
            }
        }
        headers = { 
            config['NOTIFICATION_KEY_NAME']: config['NOTIFICATION_KEY_VALUE'],
            'Content-Type': 'application/json'
        }

        try:
            response = requests.post(config['NOTIFICATION_URL'], data = json.dumps(notification), headers=headers)
        except Exception as e:
            logger.warning('Could notify Black Flag for  ' + black_flag.org_id + '.xml')
            continue

        if response.status_code != 200:
            logger.warning('Could not notify Black Flag for  ' + black_flag.org_id + '.xml')
            continue

        db.updateBlackFlagNotified(conn, black_flag[0])        

    file_hashes = db.getInvalidDatasetsForActivityLevelVal(conn)

    if config['VALIDATION']['ACTIVITY_LEVEL_PARALLEL_PROCESSES'] == 1:
        process_hash_list(file_hashes)
    else:
        chunked_hash_lists = list(chunk_list(file_hashes, config['VALIDATION']['PARALLEL_PROCESSES']))

        processes = []

        logger.info("Processing " + str(len(file_hashes)) + " IATI files in a maximum of " + str(config['VALIDATION']['PARALLEL_PROCESSES']) + " parallel processes for validation")

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
