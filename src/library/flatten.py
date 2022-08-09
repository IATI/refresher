import time
import traceback
from multiprocessing import Process
from library.logger import getLogger
import requests
from constants.config import config
from azure.storage.blob import BlobServiceClient
from azure.core import exceptions as AzureExceptions
import library.db as db
import json
from json.decoder import JSONDecodeError
import library.utils as utils

logger = getLogger()


def process_hash_list(document_datasets):

    conn = db.getDirectConnection()

    for file_data in document_datasets:
        try:
            file_hash = file_data[0]
            downloaded = file_data[1]
            doc_id = file_data[2]
            prior_error = file_data[3]

            # Explicit error codes returned from Flattener
            if prior_error == 422 or prior_error == 400 or prior_error == 413:
                continue

            db.startFlatten(conn, doc_id)

            logger.info('Flattening file with hash {} doc id {}, downloaded at {}'.format(
                file_hash, doc_id, downloaded.isoformat()))
            blob_name = file_hash + '.xml'

            blob_service_client = BlobServiceClient.from_connection_string(
                config['STORAGE_CONNECTION_STR'])
            blob_client = blob_service_client.get_blob_client(
                container=config['CLEAN_CONTAINER_NAME'], blob=blob_name)

            downloader = blob_client.download_blob()

            try:
                payload = utils.get_text_from_blob(downloader, file_hash)
            except:
                logger.warning(
                    'Can not identify charset for hash {} doc id {}'.format(file_hash, doc_id))
                continue

            headers = {config['FLATTEN']['FLATTENER_KEY_NAME']
                : config['FLATTEN']['FLATTENER_KEY_VALUE']}
            response = requests.post(
                config['FLATTEN']['FLATTENER_URL'], data=payload.encode('utf-8'), headers=headers)
            db.updateSolrizeStartDate(conn, file_hash)

            if response.status_code != 200:
                logger.warning('Flattener reports error status {} for hash {} doc id {}'.format(
                    str(response.status_code), file_hash, doc_id))
                if response.status_code == 404:
                    logger.warning('Giving it a chance to come back up...')
                    time.sleep(360)  # Give the thing time to come back up
                    logger.warning('...and off we go again.')
                db.updateFlattenError(conn, doc_id, response.status_code)
                continue

            flattenedObject = response.json()

            db.completeFlatten(conn, doc_id, json.dumps(flattenedObject))

        except (AzureExceptions.ResourceNotFoundError) as e:
            logger.warning('Blob not found for hash ' + file_hash +
                           ' - updating as Not Downloaded for the refresher to pick up.')
            db.updateFileAsNotDownloaded(conn, doc_id)
        except JSONDecodeError:
            logger.warning('Unable to decode JSON output from Flattener for hash {} doc id {}'.format(
                file_hash, doc_id))
            logger.warning('Assuming soft 404/500, waiting, and retrying...')
            time.sleep(360)
            logger.warning('...and off we go again.')
            db.updateFlattenError(conn, doc_id, 404)
        except Exception as e:
            logger.error('ERROR with flattening ' + file_hash)
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
    logger.info("Starting to flatten...")

    conn = db.getDirectConnection()

    logger.info("Resetting those unfinished")

    db.resetUnfinishedFlattens(conn)

    file_hashes = db.getUnflattenedDatasets(conn)

    if config['FLATTEN']['PARALLEL_PROCESSES'] == 1:
        logger.info("Flattening and storing " +
                    str(len(file_hashes)) + " IATI files in a single process.")
        process_hash_list(file_hashes)
    else:
        chunked_hash_lists = list(utils.chunk_list(
            file_hashes, config['FLATTEN']['PARALLEL_PROCESSES']))

        processes = []

        logger.info("Flattening and storing " + str(len(file_hashes)) + " IATI files in a maximum of " +
                    str(config['FLATTEN']['PARALLEL_PROCESSES']) + " parallel processes.")

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
