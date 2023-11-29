import time
import json
from multiprocessing import Process
from library.logger import getLogger
from constants.config import config
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
import library.db as db
from lxml import etree
from io import BytesIO
import library.utils as utils

logger = getLogger("lakify")


def clean_identifier(identifier):
    return identifier.strip().replace('\n', '')


def recursive_json_nest(element, output):
    element_dict = {'@{}'.format(e_key): element.get(e_key)
                    for e_key in element.keys()}
    element_tag = element.tag
    if element_tag is etree.Comment:
        element_tag = 'comment()'
    elif element_tag is etree.PI:
        element_tag = 'PI()'

    if element.text is not None and element.text.strip() != '':
        element_dict['text()'] = element.text
    else:
        inner_text = None
        if element.tag is not etree.Comment and element.tag is not etree.PI:
            inner_text = ''.join([inner_string.strip()
                                  for inner_string in element.itertext(tag=element_tag)])
        if inner_text is not None and inner_text != '':
            element_dict['text()'] = inner_text

    if element_tag == 'narrative' and 'text()' not in element_dict:
        element_dict['text()'] = ''

    for e_child in element.getchildren():
        element_dict = recursive_json_nest(e_child, element_dict)

    if element_tag in output.keys():
        output[element_tag].append(element_dict)
    else:
        output[element_tag] = [element_dict]

    return output


def process_hash_list(document_datasets):

    conn = db.getDirectConnection()

    for file_data in document_datasets:
        try:
            file_hash = file_data[0]
            downloaded = file_data[1]
            doc_id = file_data[2]

            db.startLakify(conn, doc_id)

            logger.info('Lakifying file with hash ' + file_hash + ', doc id ' + doc_id +
                        ', downloaded at ' + downloaded.isoformat())
            blob_name = file_hash + '.xml'

            blob_service_client = BlobServiceClient.from_connection_string(
                config['STORAGE_CONNECTION_STR'])
            blob_client = blob_service_client.get_blob_client(
                container=config['CLEAN_CONTAINER_NAME'], blob=blob_name)

            downloader = blob_client.download_blob()
            blob_bytes = BytesIO(downloader.content_as_bytes())

            large_parser = etree.XMLParser(huge_tree=True)
            root = etree.parse(blob_bytes, parser=large_parser).getroot()

            if root.tag != 'iati-activities':
                raise Exception('Blob returning non-IATI XML for doc id {} with hash {}. Returned: "{}"'.format(
                    doc_id, file_hash, blob_bytes.read()))

            del root

            context = etree.iterparse(
                blob_bytes, tag='iati-activity', huge_tree=True)

            for _, activity in context:
                identifiers = activity.xpath("iati-identifier/text()")
                if identifiers:
                    id_hash = utils.get_hash_for_identifier(
                        clean_identifier(identifiers[0]))
                    activity_xml = etree.tostring(activity, encoding='utf-8')
                    activity_json = recursive_json_nest(activity, {})
                    act_blob_client = blob_service_client.get_blob_client(
                        container=config['ACTIVITIES_LAKE_CONTAINER_NAME'], blob='{}.xml'.format(id_hash))
                    act_blob_client.upload_blob(activity_xml, overwrite=True)
                    act_blob_client.set_blob_tags({"dataset_hash": file_hash})
                    act_blob_json_client = blob_service_client.get_blob_client(
                        container=config['ACTIVITIES_LAKE_CONTAINER_NAME'], blob='{}.json'.format(id_hash))
                    act_blob_json_client.upload_blob(
                        json.dumps(activity_json, ensure_ascii=False).replace(
                            '{http://www.w3.org/XML/1998/namespace}', 'xml:').encode('utf-8'),
                        overwrite=True
                    )
                    act_blob_json_client.set_blob_tags(
                        {"dataset_hash": file_hash})
                # Free memory
                activity.clear()
                for ancestor in activity.xpath('ancestor-or-self::*'):
                    while ancestor.getprevious() is not None:
                        try:
                            del ancestor.getparent()[0]
                        except TypeError:
                            break
            del context

            db.completeLakify(conn, doc_id)

        except ResourceNotFoundError as e:
            err_message = "Unknown ResourceNotFoundError reason."
            if hasattr(e, 'reason'):
                err_message = e.reason
            logger.warning('Could not download hash {} and doc id {}. ResourceNotFoundError: {} In storage container: {}. Sending back to clean.'.format(
                file_hash, doc_id, err_message, config['CLEAN_CONTAINER_NAME']))
            db.sendLakifyErrorToClean(conn, doc_id)
        except (etree.XMLSyntaxError, etree.SerialisationError) as e:
            err_message = "Unknown error"
            if hasattr(e, 'msg'):
                err_message = e.msg
            logger.warning('Failed to extract activities to lake with hash {} and doc id {}. Error: {}. Sending back to clean.'.format(
                file_hash, doc_id, err_message))
            db.sendLakifyErrorToClean(conn, doc_id)
        except Exception as e:
            err_message = "Unknown error"
            if hasattr(e, 'args') and len(e.args) > 0:
                err_message = e.args[0]
            if hasattr(e, 'message'):
                err_message = e.message
            if hasattr(e, 'msg'):
                err_message = e.msg
            logger.error('ERROR with Lakifiying hash {} and doc id {}. Error: {}'.format(
                file_hash, doc_id, err_message))
            db.lakifyError(conn, doc_id, err_message)

    conn.close()


def service_loop():
    logger.info("Start service loop")

    while True:
        main()
        time.sleep(60)


def main():
    logger.info("Starting to Lakify...")

    conn = db.getDirectConnection()

    logger.info("Resetting those unfinished")

    db.resetUnfinishedLakifies(conn)

    file_hashes = db.getUnlakifiedDatasets(conn)

    logger.info("Got unlakified datasets")

    if config['LAKIFY']['PARALLEL_PROCESSES'] == 1:
        logger.info("Lakifiying " + str(len(file_hashes)) +
                    " IATI docs in a single process")
        process_hash_list(file_hashes)
    else:
        chunked_hash_lists = list(utils.chunk_list(
            file_hashes, config['LAKIFY']['PARALLEL_PROCESSES']))

        processes = []

        logger.info("Lakifiying " + str(len(file_hashes)) + " IATI docs in a maximum of " +
                    str(config['LAKIFY']['PARALLEL_PROCESSES']) + " parallel processes")

        for chunk in chunked_hash_lists:
            if len(chunk) == 0:
                continue
            process = Process(target=process_hash_list, args=(chunk,))
            process.start()
            processes.append(process)

        for process in processes:
            process.join()

    conn.close()
    logger.info("Finished.")
