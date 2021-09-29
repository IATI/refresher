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
import hashlib
import chardet

logger = getLogger()
solr = pysolr.Solr(config['SOLRIZE']['SOLR_API_URL'] + 'activity/', always_commit=True)

def chunk_list(l, n):
    for i in range(0, n):
        yield l[i::n]

def get_text_from_blob(downloader):
    #In order of likelihood
    charsets = ['UTF-8', 'latin-1', 'UTF-16', 'Windows-1252']

    for charset in charsets:
        try:
            return downloader.content_as_text(encoding=charset)
        except:
            continue

    raise ValueError('Charset unknown, or not in the list.')

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

            logger.info('Flattening file with hash ' + file_hash + ', downloaded at ' + downloaded.isoformat())
            blob_name = file_hash + '.xml'

            blob_service_client = BlobServiceClient.from_connection_string(config['STORAGE_CONNECTION_STR'])
            blob_client = blob_service_client.get_blob_client(container=config['SOURCE_CONTAINER_NAME'], blob=blob_name)

            downloader = blob_client.download_blob()

            try:
                xml_string = get_text_from_blob(downloader)
            except:
                logger.warning('Can not identify charset for ' + file_hash + '.xml')
                continue

            blob_service_client = BlobServiceClient.from_connection_string(config['STORAGE_CONNECTION_STR'])

            context = etree.iterparse(xml_string, tag='iati-activity', huge_tree=True)
            for _, activity in context:
                identifiers = activity.xpath("iati-identifier/text()")
                if identifiers:
                    identifier = identifiers[0]
                    identifier_hash = hashlib.sha1()
                    identifier_hash.update(identifier.encode())
                    identifier_hash_hex = identifier_hash.hexdigest()
                    activity_xml = etree.tostring(activity)
                    act_blob_client = blob_service_client.get_blob_client(container=config['ACTIVITIES_LAKE_CONTAINER_NAME'], blob='{}.xml'.format(identifier_hash_hex))
                    act_blob_client.upload_blob(activity_xml, overwrite=True)
                    act_blob_client.set_blob_tags({"dataset_hash": hash})
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
            db.lakifyError(conn, doc_id, 'Failed to extract activities')
            print(traceback.format_exc())
            err_message = "Unkown error"
            if hasattr(e, 'args'):
                err_message = e.args[0]                    
            if hasattr(e, 'message'):
                err_message = e.message
            if hasattr(e, 'msg'):                         
                err_message = e.msg 

            logger.error(err_message)
            db.lakifyError(conn, doc_id, 'Failed to extract activities')     

    conn.close()

def addToSolr(conn, batch, file_hash):
    response = solr.add(batch)

    if hasattr(response, 'status_code') and response.status_code != 200:
        if response.status_code >= 400 and response.status_code < 500:
            db.updateSolrError(conn, file_hash, response.status_code)
            logger.warning('Solr reports Client Error with status ' + str(response.status_code) + ' for source blob ' + file_hash + '.xml')
        elif response.status_code >= 500:
            db.updateSolrError(conn, file_hash, response.status_code)
            logger.warning('Solr reports Server Error with status ' + str(response.status_code) + ' for source blob ' + file_hash + '.xml')
        else: 
            logger.warning('Solr reports status ' + str(response.status_code) + ' for source blob ' + file_hash + '.xml')

def service_loop():
    logger.info("Start service loop")

    while True:
        main()            
        time.sleep(60)

def main():
    logger.info("Starting to Lakify...")

    conn = db.getDirectConnection()

    logger.info("Got DB connection")

    file_hashes = db.getUnlakifiedDatasets(conn)

    logger.info("Got unlakified datasets")

    if config['SOLRIZE']['PARALLEL_PROCESSES'] == 1:
        logger.info("Lakifiying " + str(len(file_hashes)) + " IATI docs in a a single process")
        process_hash_list(file_hashes)
    else:
        chunked_hash_lists = list(chunk_list(file_hashes, config['SOLRIZE']['PARALLEL_PROCESSES']))

        processes = []

        logger.info("Lakifiying " + str(len(file_hashes)) + " IATI docs in a maximum of " + str(config['SOLRIZE']['PARALLEL_PROCESSES']) + " parallel processes")

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