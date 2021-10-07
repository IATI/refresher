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
import library.utils as utils

logger = getLogger()
solr = pysolr.Solr(config['SOLRIZE']['SOLR_API_URL'] + 'activity/', always_commit=True, auth=(config['SOLRIZE']['SOLR_USER'], config['SOLRIZE']['SOLR_PASSWORD']))

def chunk_list(l, n):
    for i in range(0, n):
        yield l[i::n]

def process_hash_list(document_datasets):

    conn = db.getDirectConnection()
    blob_service_client = BlobServiceClient.from_connection_string(config['STORAGE_CONNECTION_STR'])   

    for file_data in document_datasets:
        try:
            file_hash = file_data[0]

            flattened_activities = db.getFlattenedActivitiesForDoc(conn, file_hash)

            solr.ping()

            db.updateSolrizeStartDate(conn, file_hash)

            logger.info("Removing any linging docs for hash " + file_hash)            
            solr.delete(q='iati_activities_document_hash:' + file_hash)

            logger.info("Adding docs for hash " + file_hash)

            for fa in flattened_activities[0]:
                blob_name = '{}.xml'.format(utils.get_hash_for_identifier(fa['iati_identifier']))
                blob_client = blob_service_client.get_blob_client(container=config['ACTIVITIES_LAKE_CONTAINER_NAME'], blob=blob_name)
                downloader = blob_client.download_blob()

                try:
                    fa['iati_xml'] = utils.get_text_from_blob(downloader, blob_name)
                except:
                    logger.warning('Could not identify charset for ' + file_hash + '.xml')
                    continue                
                
                fa['iati_activities_document_hash'] = file_hash
                addToSolr(conn, [fa], file_hash)

            logger.info("Updating DB for " + file_hash) 
            db.completeSolrize(conn, file_hash)     

        except Exception as e:
            logger.error('ERROR with Solrizing ' + file_hash)
            print(traceback.format_exc())
            if hasattr(e, 'args'):                     
                logger.error(e.args[0])
            if hasattr(e, 'message'):                         
                logger.error(e.message)
            if hasattr(e, 'msg'):                         
                logger.error(e.msg)
            try:
                logger.warning(e.args[0])
            except:
                pass        

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
    logger.info("Starting to Solrize...")

    conn = db.getDirectConnection()

    logger.info("Got DB connection")

    file_hashes = db.getUnsolrizedDatasets(conn)

    logger.info("Got unsolrized datasets")

    if config['SOLRIZE']['PARALLEL_PROCESSES'] == 1:
        logger.info("Solrizing " + str(len(file_hashes)) + " IATI docs in a a single process")
        process_hash_list(file_hashes)
    else:
        chunked_hash_lists = list(chunk_list(file_hashes, config['SOLRIZE']['PARALLEL_PROCESSES']))

        processes = []

        logger.info("Solrizing " + str(len(file_hashes)) + " IATI docs in a maximum of " + str(config['SOLRIZE']['PARALLEL_PROCESSES']) + " parallel processes")

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