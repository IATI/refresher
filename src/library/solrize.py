import os, time, sys, traceback, copy, json
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
import re

logger = getLogger()
solr_cores = {}
explode_elements = json.loads(config['SOLRIZE']['EXPLODE_ELEMENTS'])

def chunk_list(l, n):
    for i in range(0, n):
        yield l[i::n]

def addCore(core_name):
    return pysolr.Solr(config['SOLRIZE']['SOLR_API_URL'] + core_name + '_solrize/', always_commit=False, auth=(config['SOLRIZE']['SOLR_USER'], config['SOLRIZE']['SOLR_PASSWORD']))

def process_hash_list(document_datasets):

    conn = db.getDirectConnection()
    blob_service_client = BlobServiceClient.from_connection_string(config['STORAGE_CONNECTION_STR'])   

    for file_data in document_datasets:
        try:
            file_hash = file_data[0]
            file_id = file_data[1]

            flattened_activities = db.getFlattenedActivitiesForDoc(conn, file_hash)

            if flattened_activities is None or flattened_activities[0] is None:
                continue

            solr_cores['activity'] = addCore('activity')

            for core_name in explode_elements:
                solr_cores[core_name] = addCore(core_name)
            
            for core_name in solr_cores:
                solr_cores[core_name].ping()

            db.updateSolrizeStartDate(conn, file_hash)

            logger.info("Removing all docs docs for doc with id " + file_id)

            for core_name in solr_cores:
                try:
                    solr_cores[core_name].delete(q='iati_activities_document_id:' + file_id)
                except:
                    logger.warn("Failed to remove docs with hash " + file_hash + " from core with name " + core_name)                  
                
            logger.info("Adding docs for hash " + file_hash)

            for fa in flattened_activities[0]:
                blob_name = '{}.xml'.format(utils.get_hash_for_identifier(fa['iati_identifier']))

                try:
                    blob_client = blob_service_client.get_blob_client(container=config['ACTIVITIES_LAKE_CONTAINER_NAME'], blob=blob_name)
                    downloader = blob_client.download_blob()
                except:
                    logger.warning('Could not download XML activity blob: blob ' + blob_name + ', file hash ' + file_hash + ', iati id ' + fa['iati_identifier'])
                    continue
                
                try:
                    fa['iati_xml'] = utils.get_text_from_blob(downloader, blob_name)
                except:
                    logger.warning('Could not identify charset:  blob ' + blob_name + ', file hash ' + file_hash + ', iati id ' + fa['iati_identifier'])
                    continue
             
                fa['iati_activities_document_id'] = file_id
                addToSolr(conn, 'activity', [fa], file_hash)

                # don't index iati_xml into exploded elements
                del fa['iati_xml']

                for element_name in explode_elements:
                    res = explode_element(element_name, fa)
                    addToSolr(conn, element_name, res, file_hash)


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

def explode_element(element_name, passed_fa):
    fa = copy.deepcopy(passed_fa)

    exploded_docs = []
    exploded_elements = {}
    single_value_elements = {}

    for key in fa:
        if key.startswith(element_name + '_'):
            if isinstance (fa[key], list):
                exploded_elements[key] = fa[key]
            else:
                single_value_elements[key] = fa[key]
    
    if not exploded_elements and not single_value_elements:
        return []
    
    if not exploded_elements:
        return [fa]
            
    for key in exploded_elements:
        del fa[key]    
    
    i=0
    
    for value in exploded_elements[list(exploded_elements)[0]]:
        exploded_doc = {}

        for key in exploded_elements:
            try:            
                exploded_doc[key] = exploded_elements[key][i]
            except:
                pass        

        exploded_docs.append({**exploded_doc, **single_value_elements, **fa})

        i = i + 1

    return exploded_docs

def addToSolr(conn, core_name, batch, file_hash):
    
    clean_batch = []
   
    for doc in batch:
        cleanDoc = {}

        doc['id'] = utils.get_hash_for_identifier(json.dumps(doc))

        for key in doc:
            if doc[key] != '':
                cleanDoc[key] = doc[key]
        
        clean_batch.append(cleanDoc)

    batch = clean_batch
    del clean_batch

    try:
        response = solr_cores[core_name].add(batch)
    except Exception as e:
        status_code = 0 
        search_res = re.search(r'\(HTTP (\d{3})\)', e.args[0])
        if search_res is not None:
            status_code = int(search_res.group(1))
        if status_code >= 500:
            logger.warning('Solr reports Client Error ' + str(status_code) + ' for source blob ' + file_hash + '.xml. Sleeping for ' + config['SOLRIZE']['SOLR_500_SLEEP'] + ' seconds: ' + e.args[0])
            db.updateSolrError(conn, file_hash, e.args[0])
            time.sleep(int(config['SOLRIZE']['SOLR_500_SLEEP'])) # give the thing time to come back up
            logger.warning('...and off we go again after ' + str(status_code) + ' error for source blob ' + file_hash + '.xml.')
        else:
            db.updateSolrError(conn, file_hash, e.args[0])
            logger.warning('Solr reports Client Error for source blob ' + file_hash + '.xml: ' + e.args[0])

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