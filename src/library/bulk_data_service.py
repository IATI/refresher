import json
from typing import Any

from constants.config import config
from library.http import requests_retry_session


def get_dataset_list_etag() -> str:
    http_response = requests_retry_session().head(
        config["REFRESHER"]["BULK_DATA_SERVICE_DATASET_INDEX_URL"],
        timeout=int(config["REFRESHER"]["BULK_DATA_SERVICE_HTTP_TIMEOUT"]),
    )
    if "ETag" not in http_response.headers:
        raise RuntimeError("The Unified Pipeline refresher requires the Bulk Data Service to support etags")
    return http_response.headers["ETag"]


def get_reporting_org_index() -> dict:
    http_response = requests_retry_session().get(
        config["REFRESHER"]["BULK_DATA_SERVICE_REPORTING_ORG_INDEX_URL"],
        timeout=int(config["REFRESHER"]["BULK_DATA_SERVICE_HTTP_TIMEOUT"]),
    )
    return json.loads(http_response.content)


def get_reporting_orgs_supplemented_metadata() -> dict[str, Any]:
    reporting_org_index = get_reporting_org_index()
    reporting_orgs_by_short_name = {
        reporting_org["short_name"]: reporting_org | {"dataset_count": 0}
        for reporting_org in reporting_org_index["reporting_orgs"]
    }
    reporting_org_index["reporting_orgs"] = reporting_orgs_by_short_name
    return reporting_org_index


def populate_reporting_orgs_with_dataset_count(reporting_orgs_index_supplemented: dict[str, dict], datasets: dict):
    for dataset in datasets:
        if dataset["reporting_org_short_name"] in reporting_orgs_index_supplemented["reporting_orgs"]:
            reporting_org = reporting_orgs_index_supplemented["reporting_orgs"][dataset["reporting_org_short_name"]]
            if "dataset_count" in reporting_org:
                reporting_org["dataset_count"] += 1
            else:
                reporting_org["dataset_count"] = 1


def get_dataset_index() -> dict:
    http_response = requests_retry_session().get(
        config["REFRESHER"]["BULK_DATA_SERVICE_DATASET_INDEX_URL"],
        timeout=int(config["REFRESHER"]["BULK_DATA_SERVICE_HTTP_TIMEOUT"]),
    )
    return json.loads(http_response.content)
