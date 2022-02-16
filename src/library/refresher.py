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
import chardet
from lxml import etree
from io import BytesIO
import hashlib
import time
from library.solrize import addCore


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
    response = requests_retry_session().get(url=api_url, timeout=30)
    if response.status_code == 200:
        json_response = json.loads(response.content)
        full_count = json_response["result"]["count"]
        current_count = len(json_response["result"]["results"])
        results += [{"id": resource["package_id"], "hash": resource["hash"], "url": resource["url"], "org_id": result["owner_org"]} for result in json_response["result"]["results"] for resource in result["resources"]]
    else:
        logger.error('IATI Registry returned ' + str(response.status_code) + ' when getting document metadata from https://iatiregistry.org/api/3/action/package_search')
        raise Exception()

    live_count = full_count
    last_live_count = None
    last_current_count = None
    numbers_not_changing_count = 0

    while current_count < live_count:
        time.sleep(1)

        next_api_url = "{}&start={}".format(api_url, current_count)
        response = requests_retry_session().get(url=next_api_url, timeout=30)
        if response.status_code == 200:
            json_response = json.loads(response.content)

            live_count = json_response["result"]["count"]

            if live_count != full_count:
                logger.info('The count changed whilst in the run - started at ' + str(full_count) + ', now at ' + str(live_count))

            current_count += len(json_response["result"]["results"])

            if current_count == last_current_count and live_count == last_live_count:
                numbers_not_changing_count = numbers_not_changing_count + 1

            if numbers_not_changing_count > 4:
                logger.warning('Numbers appear to have been the same for five iterations, indicating a problem - raising exception to end run.')
                raise Exception()

            last_current_count = current_count
            last_live_count = live_count

            results += [{"id": resource["package_id"], "hash": resource["hash"], "url": resource["url"], "org_id": result["owner_org"]} for result in json_response["result"]["results"] for resource in result["resources"]]
        else:
            logger.error('IATI Registry returned ' + str(response.status_code) + ' when getting document metadata from https://iatiregistry.org/api/3/action/package_search')
            raise Exception()

    logger.info('Final count:' + str(current_count) + ', full count: ' + str(live_count))
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


def clean_datasets(conn, stale_datasets, changed_datasets):
    blob_service_client = BlobServiceClient.from_connection_string(config['STORAGE_CONNECTION_STR'])

    # clean up activity lake for stale_datasets, doesn't need to be done for changed_datasets as iati-identifiers hash probably didn't change
    if len(stale_datasets) > 0:
        try:
            logger.info('Removing ' + str(len(stale_datasets)) + ' stale documents from lake')
            lake_container_client = blob_service_client.get_container_client(config['ACTIVITIES_LAKE_CONTAINER_NAME'])

            for (file_id, file_hash) in stale_datasets:
                filter_config = "@container='"+ str(config["ACTIVITIES_LAKE_CONTAINER_NAME"]) + "' and dataset_hash='" + file_hash + "'"
                assoc_blobs = blob_service_client.find_blobs_by_tags(filter_config)
                name_list = [ blob['name'] for blob in assoc_blobs ]
                max_blob_delete = config['MAX_BLOB_DELETE']
                if len(name_list) > max_blob_delete:
                    chunked_list = [name_list[i:i + max_blob_delete] for i in range(0, len(name_list), max_blob_delete)] 
                    for list_chunk in chunked_list:
                        lake_container_client.delete_blobs(*list_chunk)
                else:
                    lake_container_client.delete_blobs(*name_list)
        except Exception as e:
            logger.warning('Failed to clean up lake for id ' + file_id + ' and hash ' + file_hash, e)

    # clean up source xml and solr for both stale and changed datasets
    combined_datasets_toclean = stale_datasets + changed_datasets
    if len(combined_datasets_toclean) > 0:
        logger.info('Removing ' + str(len(stale_datasets)) + ' stale and ' + str(len(changed_datasets)) + ' changed documents')

        # prep solr connections
        try:
            solr_cores = {}
            solr_cores['activity'] = addCore('activity')
            explode_elements = json.loads(config['SOLRIZE']['EXPLODE_ELEMENTS'])        

            for core_name in explode_elements:
                solr_cores[core_name] = addCore(core_name)
            
            for core_name in solr_cores:
                solr_cores[core_name].ping()
        except Exception as e:
            logger.error('ERROR with Initialising Solr to delete stale or changed documents')
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

        source_container_client = blob_service_client.get_container_client(config['SOURCE_CONTAINER_NAME'])

        for (file_id, file_hash) in combined_datasets_toclean:
            # remove source xml
            try:
                source_container_client.delete_blob(file_hash + '.xml')
            except (AzureExceptions.ResourceNotFoundError) as e:
                logger.warning('Can not delete blob as does not exist:' + file_hash + '.xml')

            # remove from all solr collections
            for core_name in solr_cores:
                try:
                    solr_cores[core_name].delete(q='iati_activities_document_id:' + file_id)
                except:
                    logger.warn("Failed to remove docs with hash " + file_hash + " from core with name " + core_name)


def sync_publishers():
    conn = db.getDirectConnection()
    start_dt = datetime.now()
    publisher_list = get_paginated_response("https://iatiregistry.org/api/3/action/organization_list", 0, 1000)

    known_publishers_num = db.getNumPublishers(conn)
    if len(publisher_list) < (config['PUBLISHER_SAFETY_PERCENTAGE']/100) * known_publishers_num:
        logger.error('Number of publishers reported by registry: ' + str(len(publisher_list)) + ', is less than ' + str(config['PUBLISHER_SAFETY_PERCENTAGE']) + r'% of previously known publishers: ' + str(known_publishers_num) + ', NOT Updating Publishers at this time.')
        conn.close()
        raise

    logger.info('Syncing Publishers to DB...')
    for publisher_name in publisher_list:
        time.sleep(1)
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
    all_datasets = []
    try:
        all_datasets = fetch_datasets()
        logger.info('...Registry result got. Updating DB...')
    except Exception as e:
        logger.error('Failed to fetch datasets from Registry')
        conn.close()
        raise

    known_documents_num = db.getNumDocuments(conn)
    if len(all_datasets) < (config['DOCUMENT_SAFETY_PERCENTAGE']/100) * known_documents_num:
        logger.error('Number of documents reported by registry: ' + str(len(all_datasets)) + ', is less than ' + str(config['DOCUMENT_SAFETY_PERCENTAGE']) + r'% of previously known publishers: ' + str(known_documents_num) + ', NOT Updating Documents at this time.')
        conn.close()
        raise

    changed_datasets = []

    for dataset in all_datasets:
        try:
            changed = db.getFileWhereHashChanged(conn,  dataset['id'],  dataset['hash'])
            if changed is not None:
                changed_datasets += [changed]
            db.insertOrUpdateDocument(conn, dataset['id'], dataset['hash'], dataset['url'], dataset['org_id'], start_dt)
        except Exception as e:
            logger.error('Failed to sync document with url ' + dataset['url'])

    stale_datasets = db.getFilesNotSeenAfter(conn, start_dt)

    if (len(changed_datasets) > 0 or len(stale_datasets) > 0):
        clean_datasets(conn, stale_datasets, changed_datasets)

    db.removeFilesNotSeenAfter(conn, start_dt)

    conn.close()


def refresh():
    logger.info('Begin refresh')

    logger.info('Syncing publishers from the Registry...')
    try:
        sync_publishers()
        logger.info('Publishers synced.')
    except Exception as e:
        logger.error('Publishers failed to sync.')

    logger.info('Syncing documents from the Registry...')
    try:
        sync_documents()
        logger.info('Documents synced.')
    except Exception as e:
        logger.error('Documents failed to sync.')

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

        if hash == '':
            # Blank hash indicates the Registry has never successfully fetched the file, so mark download_error
            db.updateFileAsDownloadError(conn, id, 404)
            continue

        try:
            blob_client = blob_service_client.get_blob_client(container=config['SOURCE_CONTAINER_NAME'], blob=hash + '.xml')
            download_response = requests_retry_session(retries=3).get(url=url, timeout=5)
            download_xml = download_response.content
            if download_response.status_code == 200:
                try:
                    detect_result = chardet.detect(download_xml)
                    charset = detect_result['encoding']
                    # log error for undetectable charset, prevent PDFs from being downloaded to Unified Platform
                    if charset is None:
                        db.updateFileAsDownloadError(conn, id, 0 )
                        continue
                except:
                    charset = 'UTF-8'
                blob_client.upload_blob(download_xml, overwrite=True, encoding=charset)
                blob_client.set_blob_tags({ "document_id": id })
                db.updateFileAsDownloaded(conn, id)
            else:
                db.updateFileAsDownloadError(conn, id, download_response.status_code)
        except (requests.exceptions.ConnectionError) as e:
            db.updateFileAsDownloadError(conn, id, 0)
        except (AzureExceptions.ResourceNotFoundError) as e:
            db.updateFileAsDownloadError(conn, id, e.status_code)
        except (AzureExceptions.ServiceResponseError) as e:
            logger.warning('Failed to upload XML with url ' + url + ' - Azure error message: ' + e.message)
        except Exception as e:
            logger.warning('Failed to upload XML with url ' + url + ' and hash ' + hash)
