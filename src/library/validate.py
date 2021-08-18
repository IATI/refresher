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
import chardet

logger = getLogger()

def chunk_list(l, n):
    for i in range(0, n):
        yield l[i::n]

def get_text_from_blob(downloader, file_hash):  
    # save off bytes if we need to detect charset later
    downloadBytes = downloader.content_as_bytes()
    try:
        return downloader.content_as_text()
    except UnicodeDecodeError:
        logger.info('File is not UTF-8, trying to detect encoding for file with hash' + file_hash)
        pass
    
    # If not UTF-8 try to detect charset and decode
    try:
        detect_result = chardet.detect(downloadBytes)
        charset = detect_result['encoding']
        logger.info('Charset detected: ' + charset + ' Confidence: ' + str(detect_result['confidence']) + ' Language: ' + detect_result['language'] + ' for file with hash' + file_hash)
        return downloader.content_as_text(encoding=charset)
    except:
        logger.warning('Could not determine charset to decode for file with hash' + file_hash)
        raise

def process_hash_list(document_datasets):

    conn = db.getDirectConnection()

    for file_data in document_datasets:
        try:
            file_hash = file_data[0]
            downloaded = file_data[1]
            file_id = file_data[2]
            file_url = file_data[3]
            prior_error = file_data[4]

            logger.info('Validating file with hash ' + file_hash + ', downloaded at ' + downloaded.isoformat())
            blob_name = file_hash + '.xml'

            blob_service_client = BlobServiceClient.from_connection_string(config['STORAGE_CONNECTION_STR'])
            blob_client = blob_service_client.get_blob_client(container=config['SOURCE_CONTAINER_NAME'], blob=blob_name)

            downloader = blob_client.download_blob()

            try:
                payload = get_text_from_blob(downloader, file_hash)
            except:
                logger.warning('Could not identify charset for ' + file_hash + '.xml')
                continue
            headers = { config['VALIDATION']['FILE_VALIDATION_KEY_NAME']: config['VALIDATION']['FILE_VALIDATION_KEY_VALUE'] }
            response = requests.post(config['VALIDATION']['FILE_VALIDATION_URL'], data = payload.encode('utf-8'), headers=headers)
            db.updateValidationRequestDate(conn, file_hash)

            if response.status_code != 200:
                if response.status_code == 400 or response.status_code == 413 or response.status_code == 422: # 'expected' error codes returned from Validator
                    # log db and move on to save the validation report
                    db.updateValidationError(conn, file_hash, response.status_code)
                elif response.status_code >= 400 and response.status_code < 500: # unexpected client errors
                    # log in db and 'continue' to break out of for loop for this file
                    db.updateValidationError(conn, file_hash, response.status_code)
                    logger.warning('Validator reports Client Error with status ' + str(response.status_code) + ' for source blob ' + file_hash + '.xml')
                    continue
                elif response.status_code >= 500: # server errors
                    # log in db and 'continue' to break out of for loop for this file
                    db.updateValidationError(conn, file_hash, response.status_code)
                    logger.warning('Validator reports Server Error with status ' + str(response.status_code) + ' for source blob ' + file_hash + '.xml')
                    continue
                else: 
                    logger.warning('Validator reports status ' + str(response.status_code) + ' for source blob ' + file_hash + '.xml')
            
            report = response.json()

            db.updateValidationState(conn, file_id, file_hash, file_url, True, json.dumps(report))
            
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
    logger.info("Starting validation...")

    conn = db.getDirectConnection()

    file_hashes = db.getUnvalidatedDatasets(conn)

    if config['VALIDATION']['PARALLEL_PROCESSES'] == 1:
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
