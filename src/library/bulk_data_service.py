import json
from typing import Any

from requests import Response
from requests.exceptions import ChunkedEncodingError, ConnectionError, SSLError

from constants.config import config
from library.http import requests_retry_session


def make_remote_request(url: str, method: str, timeout: int) -> Response:
    error_str = "{} received when making {} request to {}. Details: {}"

    try:
        if method == "head":
            http_response = requests_retry_session().head(url=url, timeout=timeout)
        else:
            http_response = requests_retry_session().get(url=url, timeout=timeout)

    except (ConnectionError, SSLError, ChunkedEncodingError) as e:
        raise RuntimeError(error_str.format(type(e).__name__, method, url, str(e)))

    return http_response


def get_json_dict_from_url(url: str, timeout: int) -> dict:
    http_response = make_remote_request(url, "get", timeout)
    try:
        response_dict = json.loads(http_response.content)
    except json.JSONDecodeError as e:
        raise RuntimeError("Content from {} could not be parsed as JSON. Details: {}".format(url, e))

    return response_dict


def get_dataset_list_etag() -> str:
    http_response = make_remote_request(
        config["REFRESHER"]["BULK_DATA_SERVICE_DATASET_INDEX_URL"],
        "head",
        config["REFRESHER"]["BULK_DATA_SERVICE_HTTP_TIMEOUT"],
    )
    if "ETag" not in http_response.headers:
        raise RuntimeError("The Unified Pipeline refresher requires the Bulk Data Service to support etags")
    return http_response.headers["ETag"]


def get_reporting_org_index() -> dict:
    return get_json_dict_from_url(
        config["REFRESHER"]["BULK_DATA_SERVICE_REPORTING_ORG_INDEX_URL"],
        config["REFRESHER"]["BULK_DATA_SERVICE_HTTP_TIMEOUT"],
    )


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
    return get_json_dict_from_url(
        config["REFRESHER"]["BULK_DATA_SERVICE_DATASET_INDEX_URL"],
        config["REFRESHER"]["BULK_DATA_SERVICE_HTTP_TIMEOUT"],
    )
