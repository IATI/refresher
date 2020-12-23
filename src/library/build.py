import os, time, sys, traceback
from multiprocessing import Process
import logging
import datetime
from library.dds import IATI_db
from constants.config import config
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from itertools import islice
import psycopg2
import library.db as db


def chunk_list(l, n):
    for i in range(0, n):
        yield l[i::n]

def process_hash_list(hash_list):
    db = IATI_db()

    for file_hash in hash_list:
        try:
            db.create_from_iati_xml(file_hash[0]) 
        except Exception as e:
            logging.error('ERROR with ' + file_hash[0])
            print(traceback.format_exc())
            if hasattr(e, 'message'):                         
                logging.error(e.message)

            if hasattr(e, 'msg'):                         
                logging.error(e.msg)
            try:
                logging.warning(e.args[0])
            except:
                pass
    
    db.close()

def main():
    logging.info("Starting build...")

    try:
        conn = db.getDirectConnection()
        cur = conn.cursor()
    except Exception as e:
        logging.error('Failed to connect to Postgres')
        sys.exit()

    sql = "SELECT hash FROM refresher WHERE root_element_key is Null"

    cur.execute(sql)
    file_hashes = cur.fetchall()

    cur.close()
    conn.close()

    if config['DDS']['PARALLEL_PROCESSES'] == 1:
        process_hash_list(file_hashes)
    else:
        chunked_hash_lists = list(chunk_list(file_hashes, config['DDS']['PARALLEL_PROCESSES']))

        processes = []

        logging.info("Processing " + str(len(file_hashes)) + " IATI files in " + str(config['DDS']['PARALLEL_PROCESSES']) + " parallel processes")

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

    logging.info("Finished.")