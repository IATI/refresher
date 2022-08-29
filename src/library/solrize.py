import os, time, sys, traceback, copy, json
from multiprocessing import Process
from library.logger import getLogger
import datetime
import requests
from constants.config import config
from azure.storage.blob import BlobServiceClient, ContainerClient
from itertools import islice
from azure.core import exceptions as AzureExceptions
import psycopg2
import library.db as db
import pysolr
import library.utils as utils
import re

logger = getLogger()
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
            self.message = 'Solr ' + self.type + ' HTTP: ' + str(self.status_code) + ' Error ' + message
        else:
            self.message = 'Solr ' + self.type + ' Error ' + message

class SolrPingError(SolrError):
    pass

class SolrizeSourceError(Exception):
    def __init__(self, message):
        super(SolrizeSourceError, self).__init__(message)
        self.message = message

def sleep_solr(file_hash, file_id, err_type = ""):
    logger.warning('Sleeping for ' + config['SOLRIZE']['SOLR_500_SLEEP'] + ' seconds for hash: ' + file_hash + ' and id: ' + file_id)
    time.sleep(int(config['SOLRIZE']['SOLR_500_SLEEP'])) # give the thing time to come back up
    logger.warning('...and off we go again after ' + err_type + ' error for hash: ' + file_hash + ' and id: ' + file_id)

def chunk_list(l, n):
    for i in range(0, n):
        yield l[i::n]

def addCore(core_name):
    return pysolr.Solr(config['SOLRIZE']['SOLR_API_URL'] + core_name + '_solrize/', always_commit=False, auth=(config['SOLRIZE']['SOLR_USER'], config['SOLRIZE']['SOLR_PASSWORD']))

def process_hash_list(document_datasets):

    conn = db.getDirectConnection()
    blob_service_client = BlobServiceClient.from_connection_string(config['STORAGE_CONNECTION_STR'])   

    for file_data in document_datasets:
        try:
            file_hash = file_data[0]
            file_id = file_data[1]

            flattened_activities = db.getFlattenedActivitiesForDoc(conn, file_hash)

            if flattened_activities is None or flattened_activities[0] is None:
                raise SolrizeSourceError('Flattened activities not found for hash: ' + file_hash + ' and id: ' + file_id)

            solr_cores['activity'] = addCore('activity')

            for core_name in explode_elements:
                solr_cores[core_name] = addCore(core_name)
            
            for core_name in solr_cores:
                try:
                    solr_cores[core_name].ping()
                except Exception as e:
                    e_message = ''
                    if hasattr(e, 'args'):                     
                        e_message = e.args[0]
                    raise SolrPingError('PINGING hash: ' + file_hash + ' and id: ' + file_id + ', from collection with name ' + core_name + ': ' + e_message)

            db.updateSolrizeStartDate(conn, file_hash)

            logger.info('Removing all docs for doc with hash: ' + file_hash + ' and id: ' + file_id)

            for core_name in solr_cores:
                try:
                    solr_cores[core_name].delete(q='iati_activities_document_id:' + file_id)
                except Exception as e:
                    e_message = ''
                    if hasattr(e, 'args'):                     
                        e_message = e.args[0]
                    raise SolrError('DELETING hash: ' + file_hash + ' and id: ' + file_id + ', from collection with name ' + core_name + ': ' + e_message)
                               
                
            logger.info('Adding docs for hash: ' + file_hash + ' and id: ' + file_id)

            for fa in flattened_activities[0]:
                blob_name = '{}.xml'.format(utils.get_hash_for_identifier(fa['iati_identifier']))

                try:
                    blob_client = blob_service_client.get_blob_client(container=config['ACTIVITIES_LAKE_CONTAINER_NAME'], blob=blob_name)
                    downloader = blob_client.download_blob()
                except:
                    db.resetUnfoundLakify(conn, file_id)
                    raise SolrizeSourceError(
                        'Could not download XML activity blob: ' + blob_name +
                        ', file hash: ' + file_hash +
                        ', iati-identifier: ' + fa['iati_identifier'] +
                        '. Sending back to Lakify.'
                    )
                
                try:
                    fa['iati_xml'] = utils.get_text_from_blob(downloader, blob_name)
                except:
                    raise SolrizeSourceError('Could not identify charset for blob: ' + blob_name + ', file hash: ' + file_hash + ', iati-identifier: ' + fa['iati_identifier'])
             
                fa['iati_activities_document_id'] = file_id
                addToSolr('activity', [fa], file_hash, file_id)

                # don't index iati_xml into exploded elements
                del fa['iati_xml']

                # transform location_point_pos for default SOLR LatLonPointSpatialField
                location_latlon = []
                try:
                    for point_pos in fa['location_point_pos']:
                        try:
                            lat_str, lon_str = point_pos.split()
                            try:
                                lat = float(lat_str)
                                lon = float(lon_str)
                                if abs(lat) <= 90 and abs(lon) <= 180:
                                    location_latlon.append("{},{}".format(lat, lon))
                                else:
                                    location_latlon.append("")
                            except ValueError:
                                location_latlon.append("")
                        except (AttributeError, ValueError) as e:
                            location_latlon.append("")
                except KeyError:
                    pass
                fa['location_latlon'] = location_latlon

                for element_name in explode_elements:
                    res = explode_element(element_name, fa)
                    addToSolr(element_name, res, file_hash, file_id)

            logger.info('Updating DB with successful Solrize for hash: ' + file_hash + ' and id: ' + file_id) 
            db.completeSolrize(conn, file_hash)     

        except (SolrizeSourceError) as e:
            logger.warning(e.message)
            db.updateSolrError(conn, file_hash, e.message)
        except (SolrPingError) as e:
            logger.warning(e.message)
            db.updateSolrError(conn, file_hash, e.message)
            if e.type == 'Server' or e.type == 'Timeout' or e.type == 'Connection':
                sleep_solr(file_hash, file_id, e.type)
        except (SolrError) as e:
            logger.warning(e.message)
            db.updateSolrError(conn, file_hash, e.message)
            if e.type == 'Server' or e.type == 'Timeout' or e.type == 'Connection':
                sleep_solr(file_hash, file_id, e.type)
            # delete to keep atomic
            logger.warning('Removing docs after Solr Error for hash: ' + file_hash + ' and id: '+ file_id + ' to keep things atomic')
            for core_name in solr_cores:
                try:
                    solr_cores[core_name].delete(q='iati_activities_document_id:' + file_id)
                except:
                    logger.error('Failed to remove docs with hash: ' + file_hash + ' and id: '+ file_id + ', from collection with name ' + core_name)   
        except Exception as e:
            message = 'Unidentified ERROR with Solrizing hash: ' + file_hash + ' and id: ' + file_id
            logger.error(message)
            db.updateSolrError(conn, file_hash, message)
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

def explode_element(element_name, passed_fa):
    fa = copy.deepcopy(passed_fa)

    exploded_docs = []
    exploded_elements = {}
    single_value_elements = {}

    for key in fa:
        if key.startswith(element_name + '_'):
            if isinstance (fa[key], list):
                exploded_elements[key] = fa[key]
            else:
                single_value_elements[key] = fa[key]
    
    if not exploded_elements and not single_value_elements:
        return []
    
    if not exploded_elements:
        return [fa]
            
    for key in exploded_elements:
        del fa[key]    
    
    i=0
    
    for value in exploded_elements[list(exploded_elements)[0]]:
        exploded_doc = {}

        for key in exploded_elements:
            try:            
                exploded_doc[key] = exploded_elements[key][i]
            except:
                pass        

        exploded_docs.append({**exploded_doc, **single_value_elements, **fa})

        i = i + 1

    return exploded_docs

def addToSolr(core_name, batch, file_hash, file_id):
    
    clean_batch = []
   
    for doc in batch:
        cleanDoc = {}

        doc['id'] = utils.get_hash_for_identifier(json.dumps(doc))

        for key in doc:
            if doc[key] != '':
                cleanDoc[key] = doc[key]
        
        clean_batch.append(cleanDoc)

    batch = clean_batch
    del clean_batch

    try:
        chunk_length = config['SOLRIZE']['MAX_BATCH_LENGTH']
        if len(batch) > chunk_length:
            logger.info('Batch length of ' + str(len(batch)) + ' ' + core_name + ' greater than ' + str(config['SOLRIZE']['MAX_BATCH_LENGTH']) + ' for hash: ' + file_hash + ' id: ' + file_id)
            for i in range(0, len(batch), chunk_length):
                solr_cores[core_name].add(batch[i:i+chunk_length])  
        else:
           solr_cores[core_name].add(batch)                
    except Exception as e:
        e_message = ''
        if hasattr(e, 'args'):                     
            e_message = e.args[0]
        raise SolrError('ADDING hash: ' + file_hash + ' and id: ' + file_id +  ' batch length: ' + str(len(batch)) +', from collection with name ' + core_name + ': ' + e_message)

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
        logger.info('Solrizing ' + str(len(file_hashes)) + ' IATI docs in a a single process')
        process_hash_list(file_hashes)
    else:
        chunked_hash_lists = list(chunk_list(file_hashes, config['SOLRIZE']['PARALLEL_PROCESSES']))

        processes = []

        logger.info('Solrizing ' + str(len(file_hashes)) + ' IATI docs in a maximum of ' + str(config['SOLRIZE']['PARALLEL_PROCESSES']) + ' parallel processes')

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
    logger.info('Finished.')
    