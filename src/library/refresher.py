from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core import exceptions as AzureExceptions
import json
import multiprocessing
multiprocessing.set_start_method('spawn', True)
import os, sys
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time
import library.db as db
from library.logger import getLogger
from constants.config import config
from datetime import datetime
import pysolr

logger = getLogger() #/action/organization_list

def requests_retry_session(
    retries=10,
    backoff_factor=0.3,
    status_forcelist=(),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def fetch_datasets():
    results = []
    api_url = "https://iatiregistry.org/api/3/action/package_search?rows=1000"
    response = requests_retry_session().get(url=api_url, timeout=30).content
    json_response = json.loads(response)
    full_count = json_response["result"]["count"]
    current_count = len(json_response["result"]["results"])
    results += [{"id": resource["package_id"], "hash": resource["hash"], "url": resource["url"], "org_id": result["owner_org"]} for result in json_response["result"]["results"] for resource in result["resources"]]

    while current_count < full_count:
        next_api_url = "{}&start={}".format(api_url, current_count)
        response = requests_retry_session().get(url=next_api_url, timeout=30).content
        json_response = json.loads(response)
        current_count += len(json_response["result"]["results"])
        results += [{"id": resource["package_id"], "hash": resource["hash"], "url": resource["url"], "org_id": result["owner_org"]} for result in json_response["result"]["results"] for resource in result["resources"]]
    
    return results

def get_paginated_response(url, offset, limit, retval = []):
    api_url = url + "?offset=" + str(offset) + "&limit=" + str(limit)
    try:
        response = requests_retry_session().get(url=api_url, timeout=30).content
        json_response = json.loads(response)

        if len(json_response['result']) != 0:
            retval = retval + json_response['result']
            offset = offset + limit
            return get_paginated_response(url, offset, limit, retval)
        else:
            return retval
    except Exception as e:
        logger.error('IATI Registry returned other than 200 when getting the list of orgs')

def sync_publishers():
    conn = db.getDirectConnection()
    start_dt = datetime.now()
    publisher_list = get_paginated_response("https://iatiregistry.org/api/3/action/organization_list", 0, 1000)

    for publisher_name in publisher_list:
        try:
            api_url = "https://iatiregistry.org/api/3/action/organization_show?id=" + publisher_name
            response = requests_retry_session().get(url=api_url, timeout=30).content
            json_response = json.loads(response)
            db.insertOrUpdatePublisher(conn, json_response['result'], start_dt)
        except Exception as e:
            logger.error('Failed to sync publisher with name ' + publisher_name)
    
    db.removePublishersNotSeenAfter(conn, start_dt)
    conn.close()

def sync_documents():
    conn = db.getDirectConnection()

    start_dt = datetime.now()
    all_datasets = fetch_datasets()
    logger.info('...Registry result got. Updating DB...')

    for dataset in all_datasets:   
        try:    
            db.insertOrUpdateDocument(conn, dataset['id'], dataset['hash'], dataset['url'], dataset['org_id'], start_dt)
        except Exception as e:
            logger.error('Failed to sync document with url ' + dataset['url'])
    
    stale_datasets = db.getFilesNotSeenAfter(conn, start_dt)

    blob_service_client = BlobServiceClient.from_connection_string(config['STORAGE_CONNECTION_STR'])
    container_client = blob_service_client.get_container_client(config['SOURCE_CONTAINER_NAME'])

    #todo perhaps - There's an argument for leaving the source files, or archiving them, or keeping a history, or whatever.

    for dataset in stale_datasets:
        file_hash = dataset[1]
        try:
            container_client.delete_blob(file_hash + '.xml')
        except (AzureExceptions.ResourceNotFoundError) as e:
            logger.warning('Can not delete blob as does not exist:' + file_hash + '.xml')

        solr = pysolr.Solr(config['SOLRIZE']['SOLR_API_URL'] + 'activity/', always_commit=True)

        try:
            solr.delete(q='iati_activities_document_hash:' + file_hash)
        except Exception as e:
            logger.warning('Failed to remove documents from Solr with document hash ' + file_hash)

    #todo perhaps - remove Validation Reports here. But maybe just leave them in place.

    db.removeFilesNotSeenAfter(conn, start_dt)

    conn.close()

def refresh():        
    logger.info('Begin refresh')       

    logger.info('Syncing publishers from the Registry...')
    sync_publishers()
    logger.info('Publishers synced. Updating publishers in DB...')

    logger.info('Syncing documents from the Registry...')
    sync_documents()
    logger.info('Documents synced.')
      
    logger.info('End refresh.')

def reload(retry_errors):
    logger.info('Start reload...')
    blob_service_client = BlobServiceClient.from_connection_string(config['STORAGE_CONNECTION_STR'])
    conn = db.getDirectConnection()

    datasets = db.getRefreshDataset(conn, retry_errors)

    chunked_datasets = list(split(datasets, config['PARALLEL_PROCESSES']))

    processes = []

    logger.info('Downloading ' + str(len(datasets)) + ' files in a maximum of ' + str(config['PARALLEL_PROCESSES']) + ' processes.' )
    
    for chunk in chunked_datasets:
        if len(chunk) == 0:
            continue
        process = multiprocessing.Process(target=download_chunk, args=(chunk, blob_service_client, datasets))
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

    logger.info("Reload complete.")

def service_loop():
    logger.info("Start service loop")
    count = 0
    while True:
        count = count + 1
        refresh()
        
        if count > config['RETRY_ERRORS_AFTER_LOOP']:
            count = 0
            reload(True)
        else:        
            reload(False)
            
        time.sleep(config['SERVICE_LOOP_SLEEP'])

def split(lst, n):
    k, m = divmod(len(lst), n)
    return (lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))

def download_chunk(chunk, blob_service_client, datasets):
    conn = db.getDirectConnection()

    for dataset in chunk:

        id = dataset[0]
        hash = dataset[1]
        url = dataset[2]

        try:
            download_xml = requests_retry_session(retries=3).get(url=url, timeout=5).content
            blob_client = blob_service_client.get_blob_client(container=config['SOURCE_CONTAINER_NAME'], blob=hash + '.xml')
            blob_client.upload_blob(download_xml)
            #MONDAY: why could the blob not be there?
            #perhaps some double-check
            db.updateFileAsDownloaded(conn, id)
        except (requests.exceptions.ConnectionError) as e:
            db.updateFileAsDownloadError(conn, id, 0)
        except (requests.exceptions.HTTPError) as e:
            db.updateFileAsDownloadError(conn, id, e.response.status_code)       
        except (AzureExceptions.ServiceResponseError) as e:
            logger.warning('Failed to upload XML with url ' + url + ' - Azure error message: ' + e.message)
        except (AzureExceptions.ResourceExistsError) as e:
            db.updateFileAsDownloaded(conn, id)
        except Exception as e:
            logger.warning('Failed to upload XML with url ' + url + ' - message: ' + e.message)
            