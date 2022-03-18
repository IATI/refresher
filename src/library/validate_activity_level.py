import os, time, sys, traceback
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
            file_url = file_data[3]
            prior_error = file_data[4]
            publisher = file_data[5]

            logger.info('Processing for individual activity indexing the critically invalid doc with hash ' + file_hash + ', downloaded at ' + downloaded.isoformat())
            blob_name = file_hash + '.xml'

            blob_service_client = BlobServiceClient.from_connection_string(config['STORAGE_CONNECTION_STR'])
            blob_client = blob_service_client.get_blob_client(container=config['SOURCE_CONTAINER_NAME'], blob=blob_name)

            downloader = blob_client.download_blob()

            try:
                context = etree.iterparse(BytesIO(downloader.content_as_bytes()), tag='iati-activity', huge_tree=True)
            except:
                logger.warning('Could not identify charset for ' + file_hash + '.xml')
                continue          

            for _, activity in context:
                activitiesEl = etree.Element('iati-activities')
                singleActivityDoc = etree.ElementTree(activitiesEl)

                for att, val in activitiesEl:
                    singleActivityDoc.attrib[att] = val

                singleActivityDoc.append(activity)

                # @todo Send to as-yet nonexistant Validator Function instead of this
                headers = { config['VALIDATION']['FILE_VALIDATION_KEY_NAME']: config['VALIDATION']['FILE_VALIDATION_KEY_VALUE'] }
                response = requests.post(config['VALIDATION']['FILE_VALIDATION_URL'], data = payload.encode('utf-8'), headers=headers)
                db.updateValidationRequestDate(conn, file_hash)

                if response.status_code != 200:
                    #Because, we not that arsed, are we, if a single activity from a critical file doesn't go?
                    logger.warning('Activity Level Validator reports Error with status ' + str(response.status_code) + ' for source blob ' + file_hash + '.xml, activity id ' + activity_id)
                    invalid_activity = False
                    continue                
                
                invalid_activity = False

                if invalid_activity: #Get rid - why store a wrong 'un?
                    activity.getparent().remove(activity)

            #replace blob with the trimmed sort

            activities_xml = etree.tostring(context)
            act_blob_client = blob_service_client.get_blob_client(container=config['ACTIVITIES_LAKE_CONTAINER_NAME'], blob='{}.xml'.format(file_hash))
            act_blob_client.upload_blob(activities_xml, overwrite=True)
            act_blob_client.set_blob_tags({"dataset_hash": file_hash})

                #activity.clear()

            

            del context
            #Send to new Validate function, which will return true/false
            #If true add to new doc
            #Replace original with new (maybe rename original to keep it around)
            #Make an ALV column, update it with a date
            #Alter the flattener to pick up from that too
            #JOB'S A GOOD UN
            
            

            db.updateActivityLevelValidationState(conn, file_id, file_hash, file_url, publisher)
            
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

    #Black flag the swine!
    db.blackFlagDubiousPublishers(conn, 1000, 24)

    file_hashes = db.getInvalidDatasetsForActivityLevelVal(conn)

    if config['VALIDATION_AL']['PARALLEL_PROCESSES'] == 1:
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
