import os, time, sys, traceback
from multiprocessing import Process
import logging
import datetime
import requests
from library.dds import IATI_db
from constants.config import config
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from itertools import islice
import psycopg2
import library.db as db

conn = db.getDirectConnection()

def chunk_list(l, n):
    for i in range(0, n):
        yield l[i::n]

def process_hash_list(hash_list):
    db = IATI_db()

    for file_hash in hash_list:
        try:
            blob_name = file_hash[0] + '.xml'

            blob_service_client = BlobServiceClient.from_connection_string(config['STORAGE_CONNECTION_STR'])
            blob_client = blob_service_client.get_blob_client(container=config['SOURCE_CONTAINER_NAME'], blob=blob_name)

            downloader = blob_client.download_blob()
            payload = downloader.content_as_text()

            report = requests.post(config['VALIDATION']['FILE_VALIDATION_URL'], data = payload.encode('utf-8'))

            blob_name = file_hash + '.json'
            blob_client = blob_service_client.get_blob_client(container=config['VALIDATION_CONTAINER_NAME'], blob=blob_name)

            blob_client.upload_blob(report.content)

            #discern state from report

            updateValidationState(conn, file_hash[0], state):
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
    
    db.close()

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