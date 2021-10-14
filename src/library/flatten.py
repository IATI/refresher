import os, time, sys, traceback
from multiprocessing import Process
from library.logger import getLogger
import datetime
import requests
from constants.config import config
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from itertools import islice
from azure.core import exceptions as AzureExceptions
import psycopg2
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
            doc_id = file_data[2]
            file_url = file_data[3]
            prior_error = file_data[4]

            if prior_error == 422 or prior_error == 400 or prior_error == 413: #explicit error codes returned from Flattener
                continue

            db.startFlatten(conn, doc_id)

            logger.info('Flattening file with hash ' + file_hash + ', downloaded at ' + downloaded.isoformat())
            blob_name = file_hash + '.xml'

            blob_service_client = BlobServiceClient.from_connection_string(config['STORAGE_CONNECTION_STR'])
            blob_client = blob_service_client.get_blob_client(container=config['SOURCE_CONTAINER_NAME'], blob=blob_name)

            downloader = blob_client.download_blob()

            try:
                payload = utils.get_text_from_blob(downloader, file_hash)
            except:
                logger.warning('Can not identify charset for ' + file_hash + '.xml')
                continue

            headers = { config['FLATTEN']['FLATTENER_KEY_NAME']: config['FLATTEN']['FLATTENER_KEY_VALUE'] }
            response = requests.post(config['FLATTEN']['FLATTENER_URL'], data = payload.encode('utf-8'), headers=headers)
            db.updateSolrizeStartDate(conn, file_hash)

            if response.status_code != 200:
                if response.status_code == 404:
                    logger.warning('Flattener reports Client Error with status 404 for source blob ' + file_hash + '.xml - giving it a chance to come back up...')
                    db.updateFlattenError(conn, doc_id, response.status_code)
                    time.sleep(360) #give the thing time to come back up
                    logger.warning('...and off we go again.')
                if response.status_code >= 400 and response.status_code < 500:
                    db.updateFlattenError(conn, doc_id, response.status_code)
                    logger.warning('Flattener reports Client Error with status ' + str(response.status_code) + ' for source blob ' + file_hash + '.xml')
                    continue
                elif response.status_code >= 500:
                    db.updateFlattenError(conn, doc_id, response.status_code)
                    logger.warning('Flattener reports Server Error with status ' + str(response.status_code) + ' for source blob ' + file_hash + '.xml')
                    continue
                else: 
                    logger.warning('Flattener reports status ' + str(response.status_code) + ' for source blob ' + file_hash + '.xml')
            
            flattenedObject = response.json()

            db.completeFlatten(conn, doc_id, json.dumps(flattenedObject))
            
        except (AzureExceptions.ResourceNotFoundError) as e:
            logger.warning('Blob not found for hash ' + file_hash + ' - updating as Not Downloaded for the refresher to pick up.')
            db.updateFileAsNotDownloaded(conn, doc_id)
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
    logger.info("Starting to flatten...")

    conn = db.getDirectConnection()

    logger.info("Resetting those unfinished")

    db.resetUnfinishedFlattens(conn)

    file_hashes = db.getUnflattenedDatasets(conn)

    if config['FLATTEN']['PARALLEL_PROCESSES'] == 1:
        process_hash_list(file_hashes)
    else:
        chunked_hash_lists = list(chunk_list(file_hashes, config['FLATTEN']['PARALLEL_PROCESSES']))

        processes = []

        logger.info("Flattening and storing " + str(len(file_hashes)) + " IATI files in a maximum of " + str(config['FLATTEN']['PARALLEL_PROCESSES']) + " parallel processes for validation")

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