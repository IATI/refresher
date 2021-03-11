import os, sys, time, re
from .logger import getLogger
import json
from datetime import datetime
import requests
from lxml import etree, objectify
import library.db as db
import hashlib
from constants.config import config
from azure.storage.blob import BlobServiceClient
import psycopg2

logging = getLogger()

class IATI_db:
    def __init__(self):
        self._parent_activity_hash = None
        
        try:
            self._conn = db.getDirectConnection()
            self._cur = self._conn.cursor()
        except Exception:
            logging.error('Failed to connect to Postgres')
            sys.exit()

        sql = "SELECT * FROM attribute_type"
        self._cur.execute(sql)

        self._attribute_types = {}

        for key, value in self._cur:
            self._attribute_types[key] = value

    def close(self):
        self._cur.close()
        self._conn.close()

    def upsert_child_elements_recursively(self, parent_el, parent_hash):
            children = parent_el.getchildren()

            if parent_el.tag == "iati-activity":
                self._parent_activity_hash = parent_hash 
            
            for child_el in children:

                child_hash = self.upsert_element(child_el)

                sql = """INSERT INTO public.element_to_child(
                        element_key, child_key)
                        VALUES (%s, %s);"""

                try:
                    self._cur.execute(sql, (parent_hash, child_hash))
                    self._conn.commit()
                except psycopg2.IntegrityError:
                    self._conn.rollback()

                sql = """INSERT INTO public.element_to_parent(
                        element_key, parent_key)
                        VALUES (%s, %s);"""
                
                try:
                    self._cur.execute(sql, (child_hash, parent_hash))
                    self._conn.commit()
                except psycopg2.IntegrityError:
                    self._conn.rollback()

                if self._parent_activity_hash is not None: 

                    sql = """INSERT INTO public.element_to_activity(
                        element_key, activity_key)
                        VALUES (%s, %s);"""                

                    try:
                        self._cur.execute(sql, (child_hash, self._parent_activity_hash))
                        self._conn.commit()
                    except psycopg2.IntegrityError:
                        self._conn.rollback()

                self.upsert_child_elements_recursively(child_el, child_hash)       
            

    def get_attribute_hash(self, key,value):
        return self.get_hash(key + value)
    
    def get_element_hash(self, el):
        return self.get_hash(etree.tostring(el))

    def get_hash(self, str_to_hash):
        return hashlib.md5(str(str_to_hash).encode('utf-8')).hexdigest()

    def upsert_attributes(self, attributes, el_hash):        

        for key, value in attributes:
            att_hash = self.get_attribute_hash(key, value)

            if not key in self._attribute_types:
                if "www.w3.org/XML" in key:
                    continue
                else:
                    logging.warning(key + " not present in attribute_types")
                    continue

            attribute_type = self._attribute_types[key]

            if attribute_type == 'date':
                sql = """INSERT INTO public.attribute(
                        md5_pk, name, date_value)
                        VALUES (%s, %s, %s);"""                
            elif attribute_type == 'numeric':
                sql = """INSERT INTO public.attribute(
                        md5_pk, name, numeric_value)
                        VALUES (%s, %s, %s);"""
            elif attribute_type == 'boolean':
                sql = """INSERT INTO public.attribute(
                        md5_pk, name, boolean_value)
                        VALUES (%s, %s, %s);"""
            else:
                sql = """INSERT INTO public.attribute(
                        md5_pk, name, string_value)
                        VALUES (%s, %s, %s);"""

            try:
                self._cur.execute(sql, (att_hash, key, str(value)))
                self._conn.commit()
            except psycopg2.IntegrityError as e:
                self._conn.rollback()
            except Exception as e:
                self._conn.rollback()
                logging.warning(e.args[0])


            sql = """INSERT INTO public.element_to_attribute(
                    element_key, attribute_key)
                    VALUES (%s, %s);"""
            try:
                self._cur.execute(sql, (el_hash, att_hash))
                self._conn.commit()
            except psycopg2.IntegrityError:
                logging.warning('Should never be getting to adding duplicate el to att relationships')
                self._conn.rollback()


    def upsert_element(self, el, is_root=False):
        if el.tag is etree.Comment:
            return
            
        el_hash = self.get_element_hash(el)

        if el.tag == 'iati-activity':
            self._parent_activity_hash = el_hash

        sql = """INSERT INTO public.element(
                md5_pk, name, text_raw, text_tokens, is_root)
                VALUES (%s, %s, %s, to_tsvector(%s), %s);"""
        
        try:
            self._cur.execute(sql, (el_hash, el.tag, el.text, el.text, str(is_root)))
        except psycopg2.IntegrityError:
            self._conn.rollback()
            return el_hash

        self._conn.commit()
        
        self.upsert_attributes(el.attrib.items(), el_hash)        

        return el_hash

    def create_from_iati_xml(self, file_hash):

        sql = "UPDATE refresher SET datastore_processing_start=%(dt)s WHERE hash = %(file_hash)s"
        date = datetime.now()

        data = {
            "file_hash": file_hash,
            "dt": date,
        }

        self._cur.execute(sql, data)
        self._conn.commit()

        blob_name = file_hash + '.xml'

        blob_service_client = BlobServiceClient.from_connection_string(config['STORAGE_CONNECTION_STR'])
        blob_client = blob_service_client.get_blob_client(container=config['SOURCE_CONTAINER_NAME'], blob=blob_name)

        downloader = blob_client.download_blob()

        try:
            root = etree.fromstring(downloader.content_as_text())
        except ValueError:
            xml = re.sub(r'\bencoding="[-\w]+"', '', downloader.content_as_text(), count=1)
            xml = re.sub(r'\bencoding=\'[-\w]+\'', '', xml, count=1)
            root = etree.fromstring(xml)

        if root.tag != "iati-activities" and root.tag != "iati-organisations":
            logging.warning('Neither activities nor organisations file - ' + blob_name)
            raise ValueError('Neither activities nor organisations file')
         
        root_hash = self.upsert_element(root, True)

        if root_hash == None:
            return

        self.upsert_child_elements_recursively(root, root_hash)

        sql = "UPDATE refresher SET datastore_root_element_key = %(root_hash)s, datastore_processing_end=%(dt)s WHERE hash = %(file_hash)s"

        date = datetime.now()

        data = {
            "root_hash": root_hash,
            "file_hash": file_hash,
            "dt": date,
        }

        try:
            self._cur.execute(sql, data)
            self._conn.commit()
        except psycopg2.IntegrityError:
            logging.warning('Integrity error on root el write where root el pk = ' + root_hash + ' and file hash = ' + file_hash)
            self._conn.rollback()
