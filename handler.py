import requests
import json
import progressbar
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, String
from sqlalchemy.types import Boolean
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

DATA_SCHEMA = "public"
DATA_TABLENAME = "serverless_refresher"


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

    return "New: {}; Modified: {}; Stale: {}".format(new_count, modified_count, stale_count)


def refresh_handler(event, context):
    body = {"input": event}
    try:
        body["message"] = refresh(event["database_url"])
        status_code = 200
    except Exception as e:
        body["message"] = str(e)
        status_code = 500

    response = {
        "statusCode": status_code,
        "body": json.dumps(body)
    }

    return response
