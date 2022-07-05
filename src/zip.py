from constants.config import config
import psycopg2
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError
import zipfile
import library.utils as utils
from library.logger import getLogger
from multiprocessing import Process, Manager
import library.db as db
from io import BytesIO
import time
import os

logger = getLogger()
manager = Manager()
input_queue = manager.Queue()
output_queue = manager.Queue()

def chunk_list(l, n):
    for i in range(0, n):
        yield l[i::n]


def getDatasetHashesByValidity(conn):
    cur = conn.cursor()
    sql = """
    SELECT hash, val.valid
    FROM document as doc
    LEFT JOIN validation as val ON doc.validation = val.id
    """
    cur.execute(sql)
    results = cur.fetchall()
    cur.close()
    return results

def process_hash_list():
    document_datasets = input_queue.get()
    conn = db.getDirectConnection()
    mem_zip_valid = BytesIO()
    mem_zip_invalid = BytesIO()
    blob_service_client = BlobServiceClient.from_connection_string(config['STORAGE_CONNECTION_STR'])

    with zipfile.ZipFile(mem_zip_valid, mode="w",compression=zipfile.ZIP_DEFLATED) as zf_v:
        with zipfile.ZipFile(mem_zip_invalid, mode="w",compression=zipfile.ZIP_DEFLATED) as zf_i:
            for file_data in document_datasets:
                file_hash = file_data[0]
                file_valid = file_data[1]
                blob_name = file_hash + '.xml'
                blob_client = blob_service_client.get_blob_client(container=config['SOURCE_CONTAINER_NAME'], blob=blob_name)
                try:
                    downloader = blob_client.download_blob(timeout=120)
                except ResourceNotFoundError:
                    logger.warning('Can not download file ' + file_hash + '.xml')
                    continue
                try:
                    blob_content = utils.get_text_from_blob(downloader, file_hash)
                except:
                    logger.warning('Can not identify charset for ' + file_hash + '.xml')
                    continue
                if file_valid:
                    zf_v.writestr(blob_name, blob_content)
                else:
                    zf_i.writestr(blob_name, blob_content)
    logger.info("Process {} finished.".format(os.getpid()))
    output_queue.put(
        (mem_zip_valid, mem_zip_invalid)
    )

def main():
    logger.info("Starting to zip...")

    conn = db.getDirectConnection()

    file_hashes = getDatasetHashesByValidity(conn)

    if config['SOLRIZE']['PARALLEL_PROCESSES'] == 1:
        (valid_zip, invalid_zip) = process_hash_list(file_hashes)

        with open('valid_iati.zip', 'wb') as zf:
            zf.write(valid_zip.getvalue())

        with open('invalid_iati.zip', 'wb') as zf:
            zf.write(invalid_zip.getvalue())
    else:
        chunked_hash_lists = list(chunk_list(file_hashes, config['SOLRIZE']['PARALLEL_PROCESSES']))

        results = []

        for chunk in chunked_hash_lists:
            if len(chunk) == 0:
                continue
            input_queue.put(chunk)
            process = Process(target=process_hash_list)
            process.start()

        while len(results) < config['SOLRIZE']['PARALLEL_PROCESSES']:
            results.append(output_queue.get())

        valid_zips = [zips[0] for zips in results]
        invalid_zips = [zips[1] for zips in results]
        
        with zipfile.ZipFile(valid_zips[0], mode="a", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as z1:
            for fname in valid_zips[1:]:
                zf = zipfile.ZipFile(fname, 'r')
                for n in zf.namelist():
                    z1.writestr(n, zf.open(n).read())
        
        with open('valid_iati.zip', 'wb') as zf:
            zf.write(valid_zips[0].getvalue())

        with zipfile.ZipFile(invalid_zips[0], mode="a", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as z1:
            for fname in invalid_zips[1:]:
                zf = zipfile.ZipFile(fname, 'r')
                for n in zf.namelist():
                    z1.writestr(n, zf.open(n).read())
        
        with open('invalid_iati.zip', 'wb') as zf:
            zf.write(invalid_zips[0].getvalue())

    conn.close()

if __name__ == '__main__':
    main()
