import os, importlib, pathlib, sys
from constants.version import __version__
from constants.config import config
import psycopg2
from library.logger import getLogger
from datetime import datetime

logger = getLogger()

def getDirectConnection():
    return psycopg2.connect(database=config['DB_NAME'], user=config['DB_USER'], password=config['DB_PASS'], host=config['DB_HOST'], port=config['DB_PORT'])

def convert_migration_to_version(migration_rev):
    return migration_rev.replace('BR_', '').replace('_','.')

def convert_version_to_migration(version):
    return 'BR_' + version.replace('.', '_')

def isUpgrade(fromVersion, toVersion):
    fromSplit = fromVersion.split('.')
    toSplit = toVersion.split('.')

    if int(fromSplit[0]) < int(toSplit[0]):
        return True

    if int(fromSplit[1]) < int(toSplit[1]):
        return True

    if int(fromSplit[2]) < int(toSplit[2]):
        return True

    return False

def get_current_db_version(conn):
    sql = 'SELECT number, migration FROM version LIMIT 1'

    cursor = conn.cursor()

    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
    except psycopg2.errors.UndefinedTable:
        return None
    
    if len(result) != 1:
        return None
    else:
        return {'number': result[0][0], 'migration': result[0][1]}


def migrateIfRequired():
    conn = getDirectConnection()
    conn.set_session(autocommit=True)
    cursor = conn.cursor()
    current_db_version = get_current_db_version(conn)

    if current_db_version is None:
        current_db_version = {
            'number': '0.0.0',
            'migration': -1
        }

    if current_db_version['number'] == __version__['number']:
        logger.info('DB at correct version')
        return
    
    upgrade = isUpgrade(current_db_version['number'], __version__['number'])

    if upgrade:
        logger.info('DB upgrading to version ' + __version__['number'])
        step = 1
    else:
        logger.info('DB downgrading to version ' + __version__['number'])
        step = -1

    for i in range(current_db_version['migration'] + step, __version__['migration'] + step, step):
        if upgrade:
            mig_num = i
        else:
            mig_num = i + 1

        migration = 'mig_' + str(mig_num)

        parent = str(pathlib.Path(__file__).parent.absolute())
        spec = importlib.util.spec_from_file_location("migration", parent + "/../migrations/" + migration + ".py")
        mig = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mig)

        if upgrade:
            sql = mig.upgrade
        else:
            sql = mig.downgrade

        sql = sql.replace('\n', ' ')
        sql = sql.replace('\t', ' ')   

        cursor.execute(sql)

    if current_db_version['number'] != "0.0.0":
        sql = 'UPDATE version SET number = %s, migration = %s WHERE number = %s'
        cursor.execute(sql, (__version__['number'], __version__['migration'], current_db_version['number']))

    cursor.close()
    conn.close()

def getRefreshDataset(conn, retry_errors=False):
    cursor = conn.cursor()

    if retry_errors:
        sql = "SELECT id, hash, url FROM document WHERE downloaded is null"
    else:
        sql = "SELECT id, hash, url FROM document WHERE downloaded is null AND download_error is null"
    
    cursor.execute(sql)
    results = cursor.fetchall()
    cursor.close()
    return results


def getCursor(conn, itersize, sql):

    cursor = conn.cursor()
    cursor.itersize = itersize
    cursor.execute(sql)

    return cursor

def getUnvalidatedDatasets(conn):    
    cur = conn.cursor()
    sql = "SELECT hash, downloaded, id, url FROM document WHERE downloaded is not null AND validation is Null ORDER BY downloaded"
    cur.execute(sql)    
    results = cur.fetchall()
    cur.close()
    return results

def getUnprocessedDatasets(conn):    
    cur = conn.cursor()
    sql = """
        SELECT doc.hash FROM document AS doc
        LEFT JOIN validation AS val ON doc.validation = val.document_hash
        WHERE datastore_root_element_key is Null 
        AND doc.downloaded is not Null 
        AND doc.validation is not Null
        AND val.valid = true
    """    
    
    cur.execute(sql)    
    results = cur.fetchall()
    cur.close()
    return results

def updateValidationRequestDate(conn, filehash):
    cur = conn.cursor()
    sql = "UPDATE document SET validation_request=%(dt)s WHERE hash=%(hash)s"

    date = datetime.now()

    data = {
        "hash": filehash,
        "dt": date,
    }

    cur.execute(sql, data)
    conn.commit()
    cur.close()

def updateValidationError(conn, filehash, status):
    cur = conn.cursor()
    sql = "UPDATE document SET validation_api_error=%s WHERE hash=%s"

    data = (status, filehash)
    cur.execute(sql, data)
    conn.commit()
    cur.close()

def updateValidationState(conn, doc_id, doc_hash, doc_url, state, report):
    cur = conn.cursor()

    if state is None:
        sql = "UPDATE document SET validation=null WHERE hash=%s"
        data = (doc_hash)
        cur.execute(sql, data)
        conn.commit()
        cur.close()
        return
    
    sql = """
        INSERT INTO validation (document_id, document_hash, document_url, created, valid, report)  
        VALUES (%(doc_id)s, %(doc_hash)s, %(doc_url)s, %(created)s, %(valid)s, %(report)s)
        ON CONFLICT (document_hash) DO
            UPDATE SET report = %(report)s,
                valid = %(valid)s
            WHERE validation.document_hash=%(doc_hash)s;

        UPDATE document SET validation=%(doc_hash)s WHERE hash=%(doc_hash)s;
    """

    data = {
        "doc_id": doc_id,
        "doc_hash": doc_hash,
        "doc_url": doc_url,
        "created": datetime.now(),
        "valid": state,
        "report": report
    }

    cur.execute(sql, data)
    
    conn.commit()
    cur.close()

def updateFileAsDownloaded(conn, id):
    cur = conn.cursor()

    sql="UPDATE document SET downloaded = %(dt)s WHERE id = %(id)s"

    date = datetime.now()

    data = {
        "id": id,
        "dt": date,
    }

    cur.execute(sql, data)
    conn.commit()
    cur.close()

def updateFileAsNotDownloaded(conn, id):
    cur = conn.cursor()

    sql="UPDATE document SET downloaded = null WHERE id = %(id)s"

    data = {
        "id": id,
    }

    cur.execute(sql, data)
    conn.commit()
    cur.close()


def updateFileAsDownloadError(conn, id, status):
    cur = conn.cursor()

    sql="UPDATE document SET downloaded = %(dt)s, download_error = %(status)s WHERE id = %(id)s"

    data = {
        "id": id,
        "dt": datetime.now(),
        "status": status
    }

    cur.execute(sql, data)
    conn.commit()
    cur.close()

def insertOrUpdatePublisher(conn, organization, last_seen):
    cur = conn.cursor()

    sql = """
        INSERT INTO publisher (org_id, description, title, name, image_url, state, country_code, created, last_seen, package_count, iati_id)  
        VALUES (%(org_id)s, %(description)s, %(title)s, %(name)s, %(image_url)s, %(state)s, %(country_code)s, %(last_seen)s, %(last_seen)s, %(package_count)s, %(iati_id)s)
        ON CONFLICT (org_id) DO
            UPDATE SET title = %(title)s,
                state = %(state)s,
                image_url = %(image_url)s,
                description = %(description)s,
                last_seen = %(last_seen)s,
                package_count = %(package_count)s,
                iati_id = %(iati_id)s
            WHERE publisher.name=%(name)s
    """
    
    data = {
        "org_id": organization['id'],
        "description": organization['publisher_description'],
        "title": organization['title'],
        "name": organization['name'],
        "state": organization['state'],
        "country_code": organization['publisher_country'],
        "last_seen": last_seen,
        "package_count": organization['package_count'],
        "iati_id": organization['publisher_iati_id']
    }

    try:
        data["image_url"] = organization['image_url']
    except:
        data["image_url"] = None

    cur.execute(sql, data)
    conn.commit()
    cur.close()

def insertOrUpdateDocument(conn, id, hash, url, publisher_id, dt):
    cur = conn.cursor()

    sql1 = """
        INSERT INTO document (id, hash, url, first_seen, last_seen, publisher) 
        VALUES (%(id)s, %(hash)s, %(url)s, %(dt)s, %(dt)s, %(publisher_id)s)
        ON CONFLICT (id) DO 
            UPDATE SET hash = %(hash)s,
                url = %(url)s,
                modified = %(dt)s,
                downloaded = null,
                download_error = null,
                validation_request = null,
                validation_api_error = null,
                validation = null,
                datastore_processing_start = null,
                datastore_processing_end = null
            WHERE document.id=%(id)s and document.hash != %(hash)s;
    """

    sql2 = """
            UPDATE document SET
            last_seen = %(dt)s,
            publisher = %(publisher_id)s
            WHERE document.id=%(id)s;
    """

    data = {
        "id": id,
        "hash": hash,
        "url": url,
        "dt": dt,
        "publisher_id": publisher_id
    }

    cur.execute(sql1, data)
    cur.execute(sql2, data)
    conn.commit()
    cur.close()

def getFilesNotSeenAfter(conn, dt):
    cur = conn.cursor()

    sql = """
        SELECT id, hash, url FROM document WHERE last_seen < %s
    """

    data = (dt,)

    cur.execute(sql, data)
    results = cur.fetchall()
    cur.close()
    return results


def removeFilesNotSeenAfter(conn, dt):
    cur = conn.cursor()

    sql = """
        DELETE FROM document WHERE last_seen < %s
    """

    data = (dt,)

    cur.execute(sql, data)
    conn.commit()
    cur.close()

def removePublishersNotSeenAfter(conn, dt):
    cur = conn.cursor()

    sql = """
        DELETE FROM publisher WHERE last_seen < %s
    """

    data = (dt,)

    cur.execute(sql, data)
    conn.commit()
    cur.close() 
