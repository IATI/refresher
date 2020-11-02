import boto3
import json
from multiprocessing import Process
import os
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, String, and_, or_
from sqlalchemy.types import Boolean
import time

DATA_SCHEMA = "public"
DATA_TABLENAME = "serverless_refresher"
PARALLEL_PROCESSES = 10
IATI_BUCKET_NAME = "iati-s3"
IATI_FOLDER_NAME = "serverless_refresher/"


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


def refresh(database_url):
    engine = create_engine(database_url)
    conn = engine.connect()
    meta = MetaData(engine)
    meta.reflect()

    all_datasets = fetch_datasets()
    new_count = 0

    try:
        datasets = Table(DATA_TABLENAME, meta, schema=DATA_SCHEMA, autoload=True)
    except sqlalchemy.exc.NoSuchTableError:  # First run
        datasets = Table(
            DATA_TABLENAME,
            meta,
            Column('id', String, primary_key=True),
            Column('hash', String),
            Column('url', String),
            Column('new', Boolean, unique=False, default=True),  # Marks whether a dataset is brand new
            Column('modified', Boolean, unique=False, default=False),  # Marks whether a dataset is old but modified
            Column('stale', Boolean, unique=False, default=False),  # Marks whether a dataset is scheduled for deletion
            Column('error', Boolean, unique=False, default=False),
            schema=DATA_SCHEMA
        )
        meta.create_all(engine)
        new_count += len(all_datasets)
        conn.execute(datasets.insert(), all_datasets)

    all_dataset_ids = [dataset["id"] for dataset in all_datasets]
    cached_datasets = conn.execute(datasets.select()).fetchall()
    cached_dataset_ids = [dataset["id"] for dataset in cached_datasets]
    stale_dataset_ids = list(set(cached_dataset_ids) - set(all_dataset_ids))
    conn.execute(datasets.update().where(datasets.c.id.in_(stale_dataset_ids)).values(new=False, modified=False, stale=True, error=False))

    stale_count = len(stale_dataset_ids)
    modified_count = 0
    for dataset in all_datasets:
        try:  # Try to insert new dataset
            dataset["new"] = True
            dataset["modified"] = False
            dataset["stale"] = False
            dataset["error"] = False
            conn.execute(datasets.insert(dataset))
            new_count += 1
        except sqlalchemy.exc.IntegrityError:  # Dataset ID already exists
            cached_dataset = conn.execute(datasets.select().where(datasets.c.id == dataset["id"])).fetchone()
            if cached_dataset["hash"] == dataset["hash"]:  # If the hashes match, carry on
                continue
            else:  # Otherwise, mark it modified and update the metadata
                dataset["new"] = False
                dataset["modified"] = True
                dataset["stale"] = False  # If for some reason, we pick up a previously stale dataset
                dataset["error"] = False
                conn.execute(datasets.update().where(datasets.c.id == dataset["id"]).values(dataset))
                modified_count += 1

    engine.dispose()

    return 200, "New: {}; Modified: {}; Stale: {}".format(new_count, modified_count, stale_count)


def split(lst, n):
    k, m = divmod(len(lst), n)
    return (lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))


def download_chunk(chunk, s3_client, conn, datasets, download_errors):
    for dataset in chunk:
        try:
            download_xml = requests_retry_session(retries=3).get(url=dataset["url"], timeout=5).content
            s3_client.put_object(Body=download_xml, Bucket=IATI_BUCKET_NAME, Key=IATI_FOLDER_NAME+dataset['id'])
            conn.execute(datasets.update().where(datasets.c.id == dataset["id"]).values(new=False, modified=False, stale=False, error=False))
        except (requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as e:
            download_errors += 1
            conn.execute(datasets.update().where(datasets.c.id == dataset["id"]).values(error=True))


def reload(database_url, s3_client, retry_errors):
    engine = create_engine(database_url)
    conn = engine.connect()
    meta = MetaData(engine)
    meta.reflect()

    try:
        datasets = Table(DATA_TABLENAME, meta, schema=DATA_SCHEMA, autoload=True)
    except sqlalchemy.exc.NoSuchTableError:
        return 500, "No database found. Try running `refresh` first."

    if retry_errors in [1, "TRUE", "true", "True", True]:
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

    chunked_datasets = list(split(new_datasets, PARALLEL_PROCESSES))

    processes = []
    
    download_errors = 0
    for chunk in chunked_datasets:
        if len(chunk) == 0:
            continue
        process = Process(target=download_chunk, args=(chunk, s3_client, conn, datasets, download_errors))
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
    for dataset in stale_datasets:
        s3_client.delete_object(Bucket=IATI_BUCKET_NAME, Key=IATI_FOLDER_NAME+dataset['id'])

    return 200, "Reload complete. Failed to download {} datasets.".format(download_errors)


def refresh_handler(event, context):
    body = {"input": event}
    try:
        status_code, body["message"] = refresh(event["database_url"])
    except Exception as e:
        body["message"] = str(e)
        status_code = 500

    response = {
        "statusCode": status_code,
        "body": json.dumps(body)
    }

    return response

def reload_handler(event, context):
    body = {"input": event}
    try:
        s3_session = boto3.session.Session()
        s3_client = s3_session.client(
            's3',
            region_name=event["s3_region"],
            endpoint_url=event["s3_host"],
            aws_access_key_id=event["s3_key"],
            aws_secret_access_key=event["s3_secret"]
        )
        status_code, body["message"] = reload(event["database_url"], s3_client, event["retry_errors"])
    except Exception as e:
        body["message"] = str(e)
        status_code = 500

    response = {
        "statusCode": status_code,
        "body": json.dumps(body)
    }

    return response
