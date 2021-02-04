import os, time, sys, traceback
from multiprocessing import Process
import logging
import datetime
import requests
from library.dds import IATI_db
from constants.config import config
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from itertools import islice
from azure.core import exceptions as AzureExceptions
import psycopg2
import library.db as db
import json

conn = db.getDirectConnection()

def chunk_list(l, n):
    for i in range(0, n):
        yield l[i::n]

def process_hash_list(hash_list):
    for file_hash in hash_list:
        try:
            blob_name = file_hash[0] + '.xml'

            blob_service_client = BlobServiceClient.from_connection_string(config['STORAGE_CONNECTION_STR'])
            blob_client = blob_service_client.get_blob_client(container=config['SOURCE_CONTAINER_NAME'], blob=blob_name)

            downloader = blob_client.download_blob()
            payload = downloader.content_as_text()

            response = requests.post(config['VALIDATION']['FILE_VALIDATION_URL'], data = payload.encode('utf-8'))

            if response.status_code is not 200:
                if response.status_code >= 400 and response.status_code < 500:
                    logging.warning('Validotor reports Client Error with status ' + response.status_code + ' for source blob ' file_hash[0] + '.xml')
                    continue
                elif response.status_code >= 400 and response.status_code < 500:
                    logging.warning('Validotor reports Server Error with status ' + response.status_code + ' for source blob ' file_hash[0] + '.xml')
                    continue
                else 
                    logging.warning('Validotor reports status ' + response.status_code + ' for source blob ' file_hash[0] + '.xml')
            
            report = response.json()

            state = None

            if report['summary']['critical'] > 0 or report['summary']['danger'] > 0:
                state = False
            else:
                state = True

            blob_name = file_hash[0] + '.json'
            blob_client = blob_service_client.get_blob_client(container=config['VALIDATION_CONTAINER_NAME'], blob=blob_name)

            blob_client.upload_blob(json.dumps(report))
            
        except (AzureExceptions.ResourceExistsError) as e:
            pass
        except Exception as e:
            logging.error('ERROR with validating ' + file_hash[0])
            print(traceback.format_exc())
            if hasattr(e, 'message'):                         
                logging.error(e.message)
            if hasattr(e, 'msg'):                         
                logging.error(e.msg)
            try:
                logging.warning(e.args[0])
            except:
                pass

        db.updateValidationState(conn, file_hash[0], state)

    conn.close()

def main():
    logging.info("Starting validation...")

    file_hashes = db.getUnvalidatedDatasets(conn)

    if config['VALIDATION']['PARALLEL_PROCESSES'] == 1:
        process_hash_list(file_hashes)
    else:
        chunked_hash_lists = list(chunk_list(file_hashes, config['VALIDATION']['PARALLEL_PROCESSES']))

        processes = []

        logging.info("Processing " + str(len(file_hashes)) + " IATI files in " + str(config['DDS']['PARALLEL_PROCESSES']) + " parallel processes for validation")

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

    logging.info("Finished.")