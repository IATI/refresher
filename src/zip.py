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
    SELECT doc.id, pub.name
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
    all_ids = getPublisherNamesForHashes(conn)
    id_dict = {res[0]: res[1] for res in all_ids}

    blob_service_client = BlobServiceClient.from_connection_string(
        config['STORAGE_CONNECTION_STR'])
    container_client = blob_service_client.get_container_client(
        container=config['CLEAN_CONTAINER_NAME'])

    with zipfile.ZipFile(mem_zip, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zip:
        for blob in tqdm(container_client.walk_blobs(include=["tags"])):
            if blob.tags:
                file_hash, file_ext = os.path.splitext(blob.name)
                if file_ext == '.xml' and blob.tags['document_id'] in id_dict.keys():
                    document_id = blob.tags['document_id']
                    publisher_name = id_dict[document_id]
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
