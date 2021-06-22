import os, time, sys, traceback
from multiprocessing import Process
from library.logger import getLogger
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
import pysolr

logger = getLogger()

def chunk_list(l, n):
    for i in range(0, n):
        yield l[i::n]

def process_hash_list(document_datasets):

    conn = db.getDirectConnection()

    solr = pysolr.Solr(config['SOLRIZE']['SOLR_API_URL'] + 'activity/')  

    for file_data in document_datasets:
        try:
            file_hash = file_data[0]
            prior_error = file_data[4]

            if prior_error: #explicit error codes returned from Validator
                continue

            flattened_activities = db.getFlattenedActivitiesForDoc(conn, file_hash)

            solr.ping()

            db.updateSolrizeStartDate(conn, file_hash)

            #If doc_hash in Solr, delete and replace as transaction
            #Work out deletes

            solr.delete(q='iati_activities_document_hash:' + file_hash)

            for fa in flattened_activities[0]:
                fa['iati_activities_document_hash'] = file_hash

            solr.add(flattened_activities[0])

            response = solr.commit()
            
            if hasattr(response, 'status_code') and response.status_code != 200:
                if response.status_code >= 400 and response.status_code < 500:
                    db.updateSolrError(conn, file_hash, response.status_code)
                    logger.warning('Solr reports Client Error with status ' + str(response.status_code) + ' for source blob ' + file_hash + '.xml')
                    continue
                elif response.status_code >= 500:
                    db.updateSolrError(conn, file_hash, response.status_code)
                    logger.warning('Solr reports Server Error with status ' + str(response.status_code) + ' for source blob ' + file_hash + '.xml')
                    continue
                else: 
                    logger.warning('Solr reports status ' + str(response.status_code) + ' for source blob ' + file_hash + '.xml')

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

def service_loop():
    logger.info("Start service loop")

    while True:
        main()            
        time.sleep(60)

def main():
    logger.info("Starting to Solrize...")

    conn = db.getDirectConnection()

    file_hashes = db.getUnsolrizedDatasets(conn)

    if config['SOLRIZE']['PARALLEL_PROCESSES'] == 1:
        process_hash_list(file_hashes)
    else:
        chunked_hash_lists = list(chunk_list(file_hashes, config['SOLRIZE']['PARALLEL_PROCESSES']))

        processes = []

        logger.info("Solrizing " + str(len(file_hashes)) + " IATI docs in a maximum of " + str(config['DDS']['PARALLEL_PROCESSES']) + " parallel processes for validation")

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