from constants.config import config
from azure.storage.blob import BlobServiceClient
import zipfile
import library.utils as utils
from library.logger import getLogger
import library.db as db
from io import BytesIO
import os
from tqdm import tqdm

logger = getLogger()


def getPublisherNamesForHashes(conn):
    cur = conn.cursor()
    sql = """
    SELECT doc.hash, pub.name
    FROM document AS doc
    LEFT JOIN publisher AS pub ON doc.publisher = pub.org_id
    """
    cur.execute(sql)
    results = cur.fetchall()
    cur.close()
    return results


def main():
    logger.info("Starting to zip...")

    mem_zip = BytesIO()

    conn = db.getDirectConnection()
    all_hashes = getPublisherNamesForHashes(conn)
    hash_dict = {res[0]: res[1] for res in all_hashes}

    blob_service_client = BlobServiceClient.from_connection_string(
        config['STORAGE_CONNECTION_STR'])
    container_client = blob_service_client.get_container_client(
        container=config['ACTIVITIES_LAKE_CONTAINER_NAME'])

    with zipfile.ZipFile(mem_zip, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zip:
        for blob in tqdm(container_client.walk_blobs(include=["tags"])):
            if blob.tags:
                if os.path.splitext(blob.name)[1] == '.xml' and blob.tags['dataset_hash'] in hash_dict.keys():
                    file_hash = blob.tags['dataset_hash']
                    publisher_name = hash_dict[file_hash]
                    downloader = container_client.download_blob(blob=blob)
                    blob_content = utils.get_text_from_blob(
                        downloader, file_hash)
                    blob_path = os.path.join("data", publisher_name, blob.name)
                    zip.writestr(blob_path, blob_content)

    with open('iati.zip', 'wb') as zf:
        zf.write(mem_zip.getvalue())

    conn.close()


if __name__ == '__main__':
    main()
