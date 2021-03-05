from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core import exceptions as AzureExceptions
import json
from multiprocessing import Process
import os, sys
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time
import sqlalchemy
from sqlalchemy import and_, or_
import library.db as db
from library.logger import getLogger
from constants.config import config
from datetime import datetime

logger = getLogger()

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
    results += [{"id": resource["package_id"], "hash": resource["hash"], "url": resource["url"]} for result in json_response["result"]["results"] for resource in result["resources"]]
    while current_count < full_count:
        next_api_url = "{}&start={}".format(api_url, current_count)
        response = requests_retry_session().get(url=next_api_url, timeout=30).content
        json_response = json.loads(response)
        current_count += len(json_response["result"]["results"])
        results += [{"id": resource["package_id"], "hash": resource["hash"], "url": resource["url"]} for result in json_response["result"]["results"] for resource in result["resources"]]
    return results

def refresh():
    logger.info('Begin refresh...')
    start_dt = datetime.now()
    conn = db.getDirectConnection()

    all_datasets = fetch_datasets()

    datasets = db.getDatasets(engine)

    for dataset in all_datasets:
        db.insertOrUpdateFile(conn, dataset['id'], dataset['hash'], dataset['url'], start_dt)

    conn.close()        
    logger.info('End refresh.')

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

def download_chunk(chunk, blob_service_client, datasets, download_errors):
    engine = db.getDbEngine()
    conn = engine.connect()
    for dataset in chunk:
        try:
            download_xml = requests_retry_session(retries=3).get(url=dataset["url"], timeout=5).content

            blob_client = blob_service_client.get_blob_client(container=config['SOURCE_CONTAINER_NAME'], blob=dataset['hash'] + '.xml')
            blob_client.upload_blob(download_xml)

            conn.execute(datasets.update().where(datasets.c.id == dataset["id"]).values(new=False, modified=False, stale=False, error=False))
        except (requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as e:
            download_errors += 1
            conn.execute(datasets.update().where(datasets.c.id == dataset["id"]).values(error=True))
        except (AzureExceptions.ServiceResponseError) as e:
            logger.warning('Failed to upload XML with url ' + dataset['url'] + ' - Azure error message: ' + e.message)
        except (AzureExceptions.ResourceExistsError) as e:
            pass
        except Exception as e:
            logger.warning('Failed to upload XML with url ' + dataset['url'] + ' - message: ' + e.message)
            
def reload(retry_errors):
    logger.info('Start reload...')
    blob_service_client = BlobServiceClient.from_connection_string(config['STORAGE_CONNECTION_STR'])
    engine = db.getDbEngine()
    conn = engine.connect()

    try:
        datasets = db.getDatasets(engine)
    except sqlalchemy.exc.NoSuchTableError:
        raise ValueError("No database found. Try running `-t refresh` first.")

    if retry_errors:
        dataset_filter = datasets.c.error == True
    else:
        dataset_filter = and_(
            or_(
                datasets.c.new == True,
                datasets.c.modified == True
            ),
            datasets.c.error == False
        )

    new_datasets = conn.execute(datasets.select().where(dataset_filter)).fetchall()

    chunked_datasets = list(split(new_datasets, config['PARALLEL_PROCESSES']))

    processes = []
    
    download_errors = 0

    logger.info('Downloading ' + str(len(new_datasets)) + ' files in a maximum of ' + str(config['PARALLEL_PROCESSES']) + ' processes.' )
    
    for chunk in chunked_datasets:
        if len(chunk) == 0:
            continue
        process = Process(target=download_chunk, args=(chunk, blob_service_client, datasets, download_errors))
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

    stale_datasets = conn.execute(datasets.select().where(datasets.c.stale == True)).fetchall()
    stale_dataset_ids = [dataset["id"] for dataset in stale_datasets]
    conn.execute(datasets.delete().where(datasets.c.id.in_(stale_dataset_ids)))

    container_client = blob_service_client.get_container_client(config['SOURCE_CONTAINER_NAME'])

    for dataset in stale_datasets:
        try:
            container_client.delete_blob(dataset['hash'] + '.xml')
        except (AzureExceptions.ResourceNotFoundError) as e:
            logger.warning('Can not delete blob as does not exist:' + dataset['hash'] + '.xml')

    logger.info("Reload complete. Failed to download {} datasets.".format(download_errors))