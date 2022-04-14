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
import pysolr
from lxml import etree
import chardet
from io import BytesIO
import library.utils as utils

logger = getLogger()

def chunk_list(l, n):
    for i in range(0, n):
        yield l[i::n]

def clean_identifier(identifier):
    return identifier.strip().replace('\n', '')

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

            db.startLakify(conn, doc_id)

            logger.info('Lakifying file with hash ' + file_hash + ', downloaded at ' + downloaded.isoformat())
            blob_name = file_hash + '.xml'

            blob_service_client = BlobServiceClient.from_connection_string(config['STORAGE_CONNECTION_STR'])
            blob_client = blob_service_client.get_blob_client(container=config['SOURCE_CONTAINER_NAME'], blob=blob_name)

            downloader = blob_client.download_blob()
            blob_bytes = BytesIO(downloader.content_as_bytes())

            large_parser = etree.XMLParser(huge_tree=True)
            root = etree.parse(blob_bytes, parser=large_parser).getroot()
            if root.tag != 'iati-activities':
                raise Exception('Blob returning non-IATI XML. Azure SDK returned: "{}"'.format(blob_bytes.read()))

            del root

            context = etree.iterparse(blob_bytes, tag='iati-activity', huge_tree=True)

            for _, activity in context:
                identifiers = activity.xpath("iati-identifier/text()")
                if identifiers:
                    id_hash = utils.get_hash_for_identifier(clean_identifier(identifiers[0]))
                    activity_xml = etree.tostring(activity)
                    act_blob_client = blob_service_client.get_blob_client(container=config['ACTIVITIES_LAKE_CONTAINER_NAME'], blob='{}.xml'.format(id_hash))
                    act_blob_client.upload_blob(activity_xml, overwrite=True)
                    act_blob_client.set_blob_tags({"dataset_hash": file_hash})
                # Free memory
                activity.clear()
                for ancestor in activity.xpath('ancestor-or-self::*'):
                    while ancestor.getprevious() is not None:
                        try:
                            del ancestor.getparent()[0]
                        except TypeError:
                            break
            del context

            db.completeLakify(conn, doc_id)

        except (etree.XMLSyntaxError, etree.SerialisationError) as e:
            logger.warning('Failed to extract activities to lake with hash ' + hash)
            db.lakifyError(conn, doc_id, 'Failed to extract activities')
        except Exception as e:
            logger.error('ERROR with Lakifiying ' + file_hash)
            print(traceback.format_exc())
            err_message = "Unkown error"
            if hasattr(e, 'args'):
                err_message = e.args[0]                    
            if hasattr(e, 'message'):
                err_message = e.message
            if hasattr(e, 'msg'):                         
                err_message = e.msg 

            logger.error(err_message)
            db.lakifyError(conn, doc_id, err_message)     

    conn.close()

def service_loop():
    logger.info("Start service loop")

    while True:
        main()            
        time.sleep(60)

def main():
    logger.info("Starting to Lakify...")

    conn = db.getDirectConnection()

    logger.info("Got DB connection")

    logger.info("Resetting those unfinished")

    db.resetUnfinishedLakifies(conn)

    file_hashes = db.getUnlakifiedDatasets(conn)

    logger.info("Got unlakified datasets")

    if config['LAKIFY']['PARALLEL_PROCESSES'] == 1:
        logger.info("Lakifiying " + str(len(file_hashes)) + " IATI docs in a single process")
        process_hash_list(file_hashes)
    else:
        chunked_hash_lists = list(chunk_list(file_hashes, config['LAKIFY']['PARALLEL_PROCESSES']))

        processes = []

        logger.info("Lakifiying " + str(len(file_hashes)) + " IATI docs in a maximum of " + str(config['LAKIFY']['PARALLEL_PROCESSES']) + " parallel processes")

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