import time
import traceback
import copy
import json
from multiprocessing import Process
from library.logger import getLogger
from constants.config import config
from azure.storage.blob import BlobServiceClient
from xml.sax.saxutils import escape
import library.db as db
import pysolr
import library.utils as utils
import re

logger = getLogger("solrize")
solr_cores = {}
explode_elements = json.loads(config['SOLRIZE']['EXPLODE_ELEMENTS'])


def parse_status_code(error_str):
    status_code = 0
    search_res = re.search(r'\(HTTP (\d{3})\)', error_str)
    if search_res is not None:
        status_code = int(search_res.group(1))
    return status_code


class SolrError(Exception):
    def __init__(self, message):
        super(SolrError, self).__init__(message)
        self.status_code = parse_status_code(message)
        if self.status_code >= 500:
            self.type = 'Server'
        elif self.status_code < 500 and self.status_code >= 400:
            self.type = 'Client'
        elif 'timed out' in message:
            self.type = 'Timeout'
        elif 'NewConnectionError' in message:
            self.type = 'Connection'
        elif 'RemoteDisconnected' in message:
            self.type = 'Connection'
        else:
            self.type = 'Unknown Type'

        if self.status_code != 0:
            self.message = 'Solr ' + self.type + ' HTTP: ' + \
                str(self.status_code) + ' Error ' + message
        else:
            self.message = 'Solr ' + self.type + ' Error ' + message


class SolrPingError(SolrError):
    pass


class SolrizeSourceError(Exception):
    def __init__(self, message):
        super(SolrizeSourceError, self).__init__(message)
        self.message = message


def sleep_solr(file_hash, file_id, err_type=""):
    logger.warning('Sleeping for ' + config['SOLRIZE']['SOLR_500_SLEEP'] +
                   ' seconds for hash: ' + file_hash + ' and id: ' + file_id)
    # give the thing time to come back up
    time.sleep(int(config['SOLRIZE']['SOLR_500_SLEEP']))
    logger.warning('...and off we go again after ' + err_type +
                   ' error for hash: ' + file_hash + ' and id: ' + file_id)


def chunk_list(l, n):
    for i in range(0, n):
        yield l[i::n]


def addCore(core_name):
    return pysolr.Solr(config['SOLRIZE']['SOLR_API_URL'] + core_name + '_solrize/', always_commit=False, auth=(config['SOLRIZE']['SOLR_USER'], config['SOLRIZE']['SOLR_PASSWORD']), timeout=config['SOLRIZE']['PYSOLR_TIMEOUT'])


def validateLatLon(point_pos):
    try:
        lat_str, lon_str = point_pos.split()
        lat = float(lat_str)
        lon = float(lon_str)
        if abs(lat) <= 90 and abs(lon) <= 180:
            return "{},{}".format(lat, lon)
    except (AttributeError, ValueError) as e:
        pass
    return None

def delete_from_solr(cores: dict, file_id: str, file_hash: str, query: dict):
    """Delete from the Solr cores"""

    for core_name in cores:
        # if one of these deletions fails, the docs from last time in
        # core which failed to delete will be cleaned up at end of solrize
        # process
        try:
            cores[core_name].delete(**query)
        except Exception as e:
            e_message = e.args[0] if hasattr(e, 'args') else ''
            raise SolrError('DELETING hash: ' + file_hash + ' and id: ' + file_id +
                            ', from collection with name ' + core_name + ': ' + e_message)

def get_blob_data(blob_client: object, conn: object, file_id: str, file_hash: str,
                  fa: dict, hashed_identifier: str, blob_type: str):
    """Downloads and returns activity blob data of the specified type"""

    blob_name = '{}/{}.{}'.format(file_id, hashed_identifier, blob_type)

    try:
        blob_client = blob_client.get_blob_client(
            container=config['ACTIVITIES_LAKE_CONTAINER_NAME'],
            blob=blob_name)
        downloader = blob_client.download_blob()
    except:
        db.resetUnfoundLakify(conn, file_id)
        raise SolrizeSourceError((
            "Could not download {} activity blob: {}"
            ", file hash: {}, iati-identifier: {}."
            " Sending back to Lakify."
            ).format(blob_type, blob_name, file_hash, fa['iati_identifier'])
        )

    try:
        return utils.get_text_from_blob(downloader, blob_name)
    except:
        raise SolrizeSourceError((
            "Could not identify charset for {} blob: {}"
            ", file hash: {}, iati-identifier: {}"
        ).format(blob_type, blob_name, file_hash, fa['iati_identifier']))


def escape_param_for_pysolr_delete(query_param: str):
    # escape is used to do XML escaping, since PySolr doesn't do it for delete
    # queries, and the \ and " escaping is to escape for a param for a Solr query
    # where the param will be in double quotes
    return escape(query_param).replace("\\", "\\\\").replace("\"", "\\\"")

def process_hash_list(document_datasets):
    """

    :param list document_datasets: A list of documents to be Solrized. Each item in the
        list is itself a list with the following elements: doc.hash, doc.id,
        doc.solr_api_error
    """

    conn = db.getDirectConnection()
    blob_service_client = BlobServiceClient.from_connection_string(
        config['STORAGE_CONNECTION_STR'])

    for file_data in document_datasets:
        try:
            file_hash = file_data[0]
            file_id = file_data[1]

            flattened_activities = db.getFlattenedActivitiesForDoc(conn, file_id)

            if flattened_activities is None or flattened_activities[0] is None:
                raise SolrizeSourceError(
                    'Flattened activities not found for hash: ' + file_hash + ' and id: ' + file_id)

            solr_cores['activity'] = addCore('activity')

            for core_name in explode_elements:
                solr_cores[core_name] = addCore(core_name)

            for core_name in solr_cores:
                try:
                    solr_cores[core_name].ping()
                except Exception as e:
                    e_message = e.args[0] if hasattr(e, 'args') else ''
                    raise SolrPingError('PINGING hash: ' + file_hash + ' and id: ' + file_id +
                                        ', from collection with name ' + core_name + ': ' + e_message)

            db.updateSolrizeStartDate(conn, file_id)

            # check whether existing DS data and new dataset contain only "good" data:
            # - flattened activities have any duplicated IATI identifiers in them?
            # - existing Solr-docs in solr for this dataset have repeated IDs
            new_identifiers = [fa['iati_identifier'] for fa in flattened_activities[0]]
            has_dupes = len(set(new_identifiers)) != len(new_identifiers) or \
                        solr_cores['activity'].search("id:" + file_id + "--*--1", rows=0).hits > 0

            identifiers_seen = []
            identifier_indices = {}

            # log to say which update method to be used, but outside the activity
            # processing loop to avoid thousands of log messages for large files
            if has_dupes:
                logger.info(('File with id: {} (hash: {}) had or has dupes, so removing all '
                             'activity, budget, transaction Solr-docs at start of processing '
                             'for each activity').format(file_id, file_hash))
            else:
                logger.info(('File with id: {} (hash: {}) had and has no dupes, so updating '
                             'activities in-place and removing budget, transaction Solr-docs '
                              'on a per-activity basis').format(file_id, file_hash))

            for fa in flattened_activities[0]:
                hashed_iati_identifier = utils.get_hash_for_identifier(fa['iati_identifier'])

                # if first time seeing identifier in dataset, delete docs from Solr
                if has_dupes and (fa['iati_identifier'] not in identifiers_seen):
                    delete_from_solr(solr_cores, file_id, file_hash, {
                                'q': ('iati_activities_document_id:"{}" AND '
                                      'iati_identifier_exact:"{}"').format(file_id,
                                                                           escape_param_for_pysolr_delete(fa['iati_identifier']))})

                    identifiers_seen.append(fa['iati_identifier'])

                # add the Activity XML/JSON blobs to the flattened activity
                fa['iati_xml'] = get_blob_data(blob_service_client, conn, file_id, file_hash,
                                               fa, hashed_iati_identifier, 'xml')
                fa['iati_json'] = get_blob_data(blob_service_client, conn, file_id, file_hash,
                                                fa, hashed_iati_identifier, 'json')

                # add id and hash
                fa['iati_activities_document_id'] = file_id
                fa['iati_activities_document_hash'] = file_hash

                # transform location_point_pos for default SOLR LatLonPointSpatialField
                try:
                    if isinstance(fa['location_point_pos'], str):
                        location_point_pos = [fa['location_point_pos']]
                    elif isinstance(fa['location_point_pos'], list):
                        location_point_pos = fa['location_point_pos']
                    location_point_latlon = [
                        validateLatLon(point_pos) for point_pos in location_point_pos if validateLatLon(point_pos) is not None
                    ]
                    if len(location_point_latlon) > 0:
                        fa['location_point_latlon'] = location_point_latlon
                except KeyError:
                    pass

                # Remove sub lists from flattened activity, saving data for use later
                sub_list_data = {}
                for element_name in explode_elements:
                    if isinstance(fa.get('@'+element_name), list):
                        sub_list_data[element_name] = fa['@'+element_name]
                        del fa['@'+element_name]

                identifier_indices[hashed_iati_identifier] = identifier_indices.get(hashed_iati_identifier, -1) + 1

                fa['id'] = "{}--{}--{}".format(file_id,
                                               hashed_iati_identifier,
                                               identifier_indices[hashed_iati_identifier])

                addToSolr('activity', [fa], file_hash, file_id)

                # don't index iati_xml or iati_json into exploded elements
                del fa['iati_xml']
                del fa['iati_json']

                # For budget, transaction cores, delete docs (if data had & has no dupes), then re-add
                for element_name, element_data in sub_list_data.items():
                    if not has_dupes:
                        delete_from_solr({element_name: solr_cores[element_name]}, file_id, file_hash, {
                                'q': ('iati_activities_document_id:"{}" AND '
                                      'iati_identifier_exact:"{}"').format(file_id,
                                                                           escape_param_for_pysolr_delete(fa['iati_identifier']))})
                    results = get_explode_element_data(element_name, element_data, fa)
                    addToSolr(element_name, results, file_hash, file_id)

            # now do cleanup delete for docs from activities that have been deleted
            logger.info(('Removing remaining old Solr-docs for file with id: {} '
                         'where the hash is not equal to current hash: {}').format(file_id, file_hash))
            delete_from_solr(solr_cores, file_id, file_hash, {
                        'q': ('iati_activities_document_id:"{}" AND '
                              'NOT(iati_activities_document_hash:"{}")').format(file_id,
                                                                                file_hash)})

            logger.info('Updating DB with successful Solrize for hash: ' +
                        file_hash + ' and id: ' + file_id)
            db.completeSolrize(conn, file_id)

        except (SolrizeSourceError) as e:
            logger.warning(e.message)
            db.updateSolrError(conn, file_id, e.message)
        except (SolrPingError) as e:
            logger.warning(e.message)
            db.updateSolrError(conn, file_id, e.message)
            if e.type == 'Server' or e.type == 'Timeout' or e.type == 'Connection':
                sleep_solr(file_hash, file_id, e.type)
        except (SolrError) as e:
            logger.warning(e.message)
            db.updateSolrError(conn, file_id, e.message)
            if e.type == 'Server' or e.type == 'Timeout' or e.type == 'Connection':
                sleep_solr(file_hash, file_id, e.type)
            # delete to keep atomic
            logger.warning('Removing docs after Solr Error for hash: ' +
                           file_hash + ' and id: ' + file_id + ' to keep things atomic')
            for core_name in solr_cores:
                try:
                    solr_cores[core_name].delete(
                        q='iati_activities_document_id:' + file_id)
                except:
                    logger.error('Failed to remove docs with hash: ' + file_hash +
                                 ' and id: ' + file_id + ', from collection with name ' + core_name)
        except Exception as e:
            message = 'Unidentified ERROR with Solrizing hash: ' + \
                file_hash + ' and id: ' + file_id
            logger.error(message)
            db.updateSolrError(conn, file_id, message)
            print(traceback.format_exc())
            if hasattr(e, 'args'):
                logger.error(e.args[0])
            if hasattr(e, 'message'):
                logger.error(e.message)
            if hasattr(e, 'msg'):
                logger.error(e.msg)
            try:
                logger.warning(e.args[0])
            except:
                pass

    conn.close()


def get_explode_element_data(element_name, element_data, activity_data):
    # copy
    starting_data = activity_data.copy()

    # remove any keys from main data
    for k in list(starting_data.keys()):
        if k.startswith(element_name + "_"):
            del starting_data[k]

    # now process
    out = []
    for idx, e_d in enumerate(element_data):
        this_data = starting_data.copy()
        this_data.update(e_d)
        # The id should include the idx so that 2 items that are exactly the same don't become 1 in the solr results https://github.com/IATI/refresher/issues/266
        this_data['id'] = utils.get_hash_for_identifier(json.dumps(this_data) + str(idx))
        out.append(this_data)

    # return
    return out


def addToSolr(core_name, batch, file_hash, file_id):
    """Code calling this should make sure an id element is already set in each doc."""

    clean_batch = []

    for doc in batch:
        cleanDoc = {}

        for key in doc:
            if doc[key] != '':
                cleanDoc[key] = doc[key]

        clean_batch.append(cleanDoc)

    batch = clean_batch
    del clean_batch

    try:
        chunk_length = config['SOLRIZE']['MAX_BATCH_LENGTH']
        if len(batch) > chunk_length:
            logger.info('Batch length of ' + str(len(batch)) + ' ' + core_name + ' greater than ' + str(
                config['SOLRIZE']['MAX_BATCH_LENGTH']) + ' for hash: ' + file_hash + ' id: ' + file_id)
            for i in range(0, len(batch), chunk_length):
                solr_cores[core_name].add(batch[i:i+chunk_length])
        else:
            solr_cores[core_name].add(batch)
    except Exception as e:
        e_message = ''
        if hasattr(e, 'args'):
            e_message = e.args[0]
        raise SolrError('ADDING hash: ' + file_hash + ' and id: ' + file_id + ' batch length: ' +
                        str(len(batch)) + ', from collection with name ' + core_name + ': ' + e_message)


def service_loop():
    logger.info('Start service loop')

    while True:
        main()
        time.sleep(60)


def main():
    logger.info('Starting to Solrize...')

    conn = db.getDirectConnection()

    logger.info('Got DB connection')

    file_hashes = db.getUnsolrizedDatasets(conn)

    logger.info('Got unsolrized datasets')

    if config['SOLRIZE']['PARALLEL_PROCESSES'] == 1:
        logger.info('Solrizing ' + str(len(file_hashes)) +
                    ' IATI docs in a a single process')
        process_hash_list(file_hashes)
    else:
        chunked_hash_lists = list(chunk_list(
            file_hashes, config['SOLRIZE']['PARALLEL_PROCESSES']))

        processes = []

        logger.info('Solrizing ' + str(len(file_hashes)) + ' IATI docs in a maximum of ' +
                    str(config['SOLRIZE']['PARALLEL_PROCESSES']) + ' parallel processes')

        for chunk in chunked_hash_lists:
            if len(chunk) == 0:
                continue
            process = Process(target=process_hash_list, args=(chunk,))
            process.start()
            processes.append(process)

        for process in processes:
            process.join()

    conn.close()
    logger.info('Finished.')
