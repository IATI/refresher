import os, time, sys, traceback
from multiprocessing import Process, Queue
from library.logger import getLogger
import datetime
from library.dds import IATI_db
from constants.config import config
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from itertools import islice
import psycopg2
import library.db as db

logger = getLogger()

def chunk_list(l, n):
    for i in range(0, n):
        yield l[i::n]

def process_hash(hash):
    dds = IATI_db()

    try:
        dds.create_from_iati_xml(hash) 
    except Exception as e:
        message = "unknown reason"
        print(traceback.format_exc())
        if hasattr(e, 'message'):
            message = e.message           
        if hasattr(e, 'msg'):                         
            message = e.msg
        try:
            message = e.args[0]
        except:
            pass

        logger.error('ERROR with ' + hash + ': ' + message)

        conn = db.getDirectConnection()                       
        logger.error(message)
        db.writeDatastoreBuildError(conn, hash, message)
        conn.close()

    dds.close()
   

def process_hash_list(hash_list):
    for file_hash in hash_list:

        try:
            process = Process(target=process_hash, args=(file_hash[0],))
            process.start()

            while process.is_alive():
                time.sleep(1)

        except Exception as e:
            logger.error('ERROR with ' + file_hash[0])
            db.writeDatastoreBuildError(conn, file_hash[0], message)
            print(traceback.format_exc())

            message = "unknown reason"

            if hasattr(e, 'message'):
                message = e.message           
            if hasattr(e, 'msg'):                         
                message = e.msg
            try:
                message = e.args[0]
            except:
                pass

            logger.error('ERROR with ' + file_hash[0] + ': ' + message)
            db.writeDatastoreBuildError(conn, file_hash[0], message)

            conn = db.getDirectConnection()                       
            logger.error(message)
            db.writeDatastoreBuildError(conn, file_hash[0], message)
            conn.close()
    


def service_loop():
    logger.info("Start service loop")
    while True:
        main()            
        time.sleep(10)

def main():
    logger.info("Starting build...")

    try:
        conn = db.getDirectConnection()
    except Exception:
        logger.error('Failed to connect to Postgres')
        sys.exit()

    logger.info("Resetting any unfinished document builds")
    db.resetUnfinishedDatasets(conn)

    file_hashes = db.getUnprocessedDatasets(conn, config['DDS']['BUILD_BATCH_LIMIT'])

    conn.close()

    if config['DDS']['PARALLEL_PROCESSES'] == 1:
        process_hash_list(file_hashes)        
    else:
        chunked_hash_lists = list(chunk_list(file_hashes, config['DDS']['PARALLEL_PROCESSES']))

        processes = []

        logger.info("Processing " + str(len(file_hashes)) + " IATI files in up to " + str(config['DDS']['PARALLEL_PROCESSES']) + " parallel processes")

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

    logger.info("Finished.")
    sys.exit()