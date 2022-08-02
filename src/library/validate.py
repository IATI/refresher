import time
import traceback
from datetime import timedelta, datetime
from multiprocessing import Process
from library.logger import getLogger
import requests
from constants.config import config
from azure.storage.blob import BlobServiceClient
from azure.core import exceptions as AzureExceptions
import library.db as db
import json
import library.utils as utils

logger = getLogger()


def chunk_list(l, n):
    for i in range(0, n):
        yield l[i::n]


def process_hash_list(document_datasets):

    conn = db.getDirectConnection()
    now = datetime.now()

    for file_data in document_datasets:
        try:
            file_hash = file_data[0]
            downloaded = file_data[1]
            file_id = file_data[2]
            file_url = file_data[3]
            prior_error = file_data[4]
            publisher = file_data[5]
            publisher_name = file_data[6]
            file_schema_valid = file_data[7]
            
            if file_schema_valid == False and downloaded > (now - timedelta(hours=config['VALIDATION']['SAFETY_VALVE_PERIOD'])):
                logger.info(f"Skipping Schema Invalid file for Full Validation until {config['VALIDATION']['SAFETY_VALVE_PERIOD']}hrs after download: {downloaded.isoformat()} for hash: {file_hash} and id: {file_id}")
                continue

            blob_name = file_hash + '.xml'

            blob_service_client = BlobServiceClient.from_connection_string(
                config['STORAGE_CONNECTION_STR'])
            blob_client = blob_service_client.get_blob_client(
                container=config['SOURCE_CONTAINER_NAME'], blob=blob_name)

            downloader = blob_client.download_blob()

            try:
                payload = utils.get_text_from_blob(downloader, file_hash)
            except:
                logger.warning(
                    f"Could not identify charset for hash: {file_hash} and id: {file_id}")
                continue
            
            if file_schema_valid is None:
                logger.info(
                    f"Schema Validating file hash: {file_hash} and id: {file_id}")
                schema_headers = { config['VALIDATION']['SCHEMA_VALIDATION_KEY_NAME']: config['VALIDATION']['SCHEMA_VALIDATION_KEY_VALUE'] }
                schema_response = requests.post(
                    config['VALIDATION']['SCHEMA_VALIDATION_URL'], data=payload.encode('utf-8'), headers=schema_headers)
                db.updateValidationRequestDate(conn, file_id)

                if schema_response.status_code != 200:
                    if schema_response.status_code >= 400 and schema_response.status_code < 500: # client errors
                        # log in db and 'continue' to break out of for loop for this file
                        db.updateValidationError(
                            conn, file_id, schema_response.status_code)
                        logger.warning(
                            f"Schema Validator reports Client Error HTTP {schema_response.status_code} for hash: {file_hash} and id: {file_id}")
                        continue
                    elif schema_response.status_code >= 500:  # server errors
                        # log in db and 'continue' to break out of for loop for this file
                        db.updateValidationError(
                            conn, file_id, schema_response.status_code)
                        logger.warning(
                            f"Schema Validator reports Server Error HTTP {schema_response.status_code} for hash: {file_hash} and id: {file_id}")
                        continue
                    else:
                        logger.error(
                            f"Schema Validator reports HTTP {schema_response.status_code} for hash: {file_hash} and id: {file_id}")
                try:
                    body = schema_response.json()
                    if body['valid'] == True or body['valid'] == False:
                        db.updateDocumentSchemaValidationStatus(
                            conn, file_id, body['valid'])
                        file_schema_valid = body['valid']
                    else:
                        raise
                except:
                    logger.error(
                        f"Unexpected response body from Schema validator for hash: {file_hash} and id: {file_id}")
                    continue
            
            if file_schema_valid == False and downloaded > (now - timedelta(hours=config['VALIDATION']['SAFETY_VALVE_PERIOD'])):
                logger.info(f"Skipping Schema Invalid file for Full Validation until {config['VALIDATION']['SAFETY_VALVE_PERIOD']}hrs after download: {downloaded.isoformat()} for hash: {file_hash} and id: {file_id}")
                continue

            logger.info(
                f"Full Validating file hash: {file_hash} and id: {file_id}")

            full_headers = {config['VALIDATION']['FULL_VALIDATION_KEY_NAME']: config['VALIDATION']['FULL_VALIDATION_KEY_VALUE']}
            full_response = requests.post(
                config['VALIDATION']['FULL_VALIDATION_URL'], data=payload.encode('utf-8'), headers=full_headers)
            db.updateValidationRequestDate(conn, file_id)

            if full_response.status_code != 200:
                # 'expected' error codes returned from Validator
                if full_response.status_code == 400 or full_response.status_code == 413 or full_response.status_code == 422:
                    # log db and move on to save the validation report
                    db.updateValidationError(
                        conn, file_id, full_response.status_code)
                elif full_response.status_code >= 400 and full_response.status_code < 500:  # unexpected client errors
                    # log in db and 'continue' to break out of for loop for this file
                    db.updateValidationError(
                        conn, file_id, full_response.status_code)
                    logger.warning(
                        f"Full Validator reports Client Error HTTP {full_response.status_code} for hash: {file_hash} and id: {file_id}")
                    continue
                elif full_response.status_code >= 500:  # server errors
                    # log in db and 'continue' to break out of for loop for this file
                    db.updateValidationError(
                        conn, file_id, full_response.status_code)
                    logger.warning(
                        f"Full Validator reports Server Error HTTP {full_response.status_code} for hash: {file_hash} and id: {file_id}")
                    continue
                else:
                    logger.error(
                        f"Full Validator reports HTTP {full_response.status_code} for hash: {file_hash} and id: {file_id}")

            report = full_response.json()

            state = report.get('valid', None)

            db.updateValidationState(
                conn, file_id, file_hash, file_url, publisher, state, json.dumps(report), publisher_name)

        except (AzureExceptions.ResourceNotFoundError) as e:
            logger.warning(
                f"Blob not found for hash: {file_hash} and id: {file_id} updating as Not Downloaded for the refresher to pick up.")
            db.updateFileAsNotDownloaded(conn, file_id)
        except Exception as e:
            logger.error(f"ERROR with validating {file_hash}")
            print(traceback.format_exc())
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
    logger.info("Starting validation...")

    conn = db.getDirectConnection()

    file_hashes = db.getUnvalidatedDatasets(conn)

    if config['VALIDATION']['PARALLEL_PROCESSES'] == 1:
        logger.info(
            f"Processing {len(file_hashes)} IATI files in a single process for validation")
        process_hash_list(file_hashes)
    else:
        chunked_hash_lists = list(chunk_list(
            file_hashes, config['VALIDATION']['PARALLEL_PROCESSES']))

        processes = []

        logger.info(
            f"Processing {len(file_hashes)} IATI files in a maximum of {config['VALIDATION']['PARALLEL_PROCESSES']} parallel processes for validation")

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
