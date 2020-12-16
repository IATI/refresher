import os, time
from multiprocessing import Process
import logging
import datetime
from library.dds import IATI_db
from constants.config import config
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from itertools import islice


def split(iterable, n):
    i = iter(iterable)
    piece = list(islice(i, n))
    while piece:
        yield piece
        piece = list(islice(i, n))


def process_blob_list(blob_list):
    db = IATI_db()

    for source_blob in blob_list:
        try:
            db.create_from_iati_xml(source_blob) 
        except Exception as e:
            logging.error('ERROR with ' + source_blob.name)
            if hasattr(e, 'message'):                         
                logging.error(e.message)

            if hasattr(e, 'msg'):                         
                logging.error(e.msg)
            try:
                logging.warning(e.args[0])
            except:
                pass

def main():
    now = datetime.datetime.now()
    logging.info("Starting...")

    blob_service_client = BlobServiceClient.from_connection_string(config['STORAGE_CONNECTION_STR'])
    container_client = blob_service_client.get_container_client(config['SOURCE_CONTAINER_NAME'])
    blob_list = container_client.list_blobs()

    if config['DDS']['PARALLEL_PROCESSES'] == 1:
        process_blob_list(blob_list)
    else:
        chunked_blob_lists= list(split(blob_list, config['DDS']['PARALLEL_PROCESSES']))

        processes = []

        logging.info("Processing " + str(len(dirlist)) + " IATI files in " + str(config['DDS']['PARALLEL_PROCESSES']) + " parallel processes")

        for chunk in chunked_blob_lists:
            if len(chunk) == 0:
                continue
            process = Process(target=process_dirlist, args=(chunk,))
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

    now = datetime.datetime.now()
    logging.info("Finished.")