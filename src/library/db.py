import os, importlib, pathlib, sys
from constants.version import __version__
from constants.config import config
import psycopg2
from library.logger import getLogger
from datetime import datetime
import time

logger = getLogger()

def getDirectConnection():
    return psycopg2.connect(database=config['DB_NAME'], user=config['DB_USER'], password=config['DB_PASS'], host=config['DB_HOST'], port=config['DB_PORT'])


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


def checkVersionMatch():
    conn = getDirectConnection()
    conn.set_session(autocommit=True)
    cursor = conn.cursor()
    current_db_version = get_current_db_version(conn)

    while current_db_version['number'] != __version__['number']:
        logger.info('DB version incorrect. Sleeping...')
        time.sleep(60)
        current_db_version = get_current_db_version(conn)

    return


def migrateIfRequired():
    check_conn = getDirectConnection()
    check_conn.set_session(autocommit=True)
    current_db_version = get_current_db_version(check_conn)
    check_conn.close()

    conn = getDirectConnection()
    conn.set_session(autocommit=False)
    cursor = conn.cursor()

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

    try:
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

            logmessage = "upgrade"

            if upgrade:
                sql = mig.upgrade
            else:
                sql = mig.downgrade
                logmessage = "downgrade"

            sql = sql.replace('\n', ' ')
            sql = sql.replace('\t', ' ')

            logger.info('Making schema ' + logmessage + ' in migration ' + str(mig_num))

            cursor.execute(sql)

        sql = 'UPDATE public.version SET number = %s, migration = %s'
        cursor.execute(sql, (__version__['number'], __version__['migration']))
        conn.commit()
    except Exception as e:
        logger.warning('Encountered unexpected exemption during migration... Rolling back...')
        conn.rollback()
        conn.close()
        raise e

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
    sql = """
    SELECT hash, downloaded, id, url, validation_api_error 
    FROM document 
    WHERE downloaded is not null AND (validation is Null OR regenerate_validation_report is True) 
    ORDER BY regenerate_validation_report DESC, downloaded
    """
    cur.execute(sql)    
    results = cur.fetchall()
    cur.close()
    return results

def getUnvalidatedAdhocDocs(conn):    
    cur = conn.cursor()
    sql = "SELECT hash, id, validation_api_error FROM adhoc_validation WHERE valid is null ORDER BY created"
    cur.execute(sql)    
    results = cur.fetchall()
    cur.close()
    return results

def getUnflattenedDatasets(conn):    
    cur = conn.cursor()
    sql = """
    SELECT hash, downloaded, id, url, flatten_api_error 
    FROM document as doc
    LEFT JOIN validation as val ON doc.validation = val.document_hash
    WHERE doc.downloaded is not null 
    AND doc.flatten_start is Null
    AND val.valid = true
    ORDER BY downloaded
    """
    cur.execute(sql)    
    results = cur.fetchall()
    cur.close()
    return results

def getFlattenedActivitiesForDoc(conn, hash):    
    cur = conn.cursor()
    sql = """
    SELECT flattened_activities
    FROM document as doc
    WHERE doc.hash = %(hash)s 
    """
    data = {"hash" : hash}
    
    cur.execute(sql, data)    
    results = cur.fetchall()
    cur.close()

    return results[0]

def getUnsolrizedDatasets(conn):
    cur = conn.cursor()
    sql = """
    SELECT doc.hash, doc.solr_api_error
    FROM document as doc
    LEFT JOIN validation as val ON doc.hash = val.document_hash
    WHERE downloaded is not null 
    AND doc.flatten_end is not null
    AND doc.solrize_end is null
	AND doc.hash != ''
    AND val.report ? 'iatiVersion' AND report->>'iatiVersion' != ''
    AND report->>'iatiVersion' NOT LIKE '1%'
    ORDER BY doc.downloaded
    """
    cur.execute(sql)    
    results = cur.fetchall()
    cur.close()
    return results

def resetUnfinishedFlattens(conn):
    cur = conn.cursor()
    sql = """
        UPDATE document
        SET flatten_start=null
        WHERE flatten_end is null
    """    
    
    cur.execute(sql)
    conn.commit()
    cur.close()

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

def updateSolrizeStartDate(conn, filehash):
    cur = conn.cursor()
    sql = "UPDATE document SET solrize_start=%(dt)s WHERE hash=%(hash)s"

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

def updateSolrError(conn, filehash, error):
    cur = conn.cursor()
    sql = "UPDATE document SET solr_api_error=%s, WHERE hash=%s"

    data = (error, filehash)
    cur.execute(sql, data)
    conn.commit()
    cur.close()

def startFlatten(conn, doc_id):
    cur = conn.cursor()

    sql = """
        UPDATE document
        SET flatten_start = %(now)s, flatten_api_error = null
        WHERE id = %(doc_id)s
    """

    data = {
        "doc_id": doc_id,
        "now": datetime.now(),
    }

    cur.execute(sql, data)
    
    conn.commit()
    cur.close()

def updateFlattenError(conn, doc_id, error):
    cur = conn.cursor()

    sql = """
        UPDATE document
        SET flatten_api_error = %(error)s
        WHERE id = %(doc_id)s
    """

    data = {
        "doc_id": doc_id,
        "error": error,
    }

    cur.execute(sql, data)
    
    conn.commit()
    cur.close()
   

def completeFlatten(conn, doc_id, flattened_activities):
    cur = conn.cursor()

    sql = """
        UPDATE document
        SET flatten_end = %(now)s, flattened_activities = %(flat_act)s, flatten_api_error = null
        WHERE id = %(doc_id)s
    """

    data = {
        "doc_id": doc_id,
        "now": datetime.now(),
        "flat_act": flattened_activities
    }

    cur.execute(sql, data)
    
    conn.commit()
    cur.close()

def completeSolrize(conn, doc_hash):
    cur = conn.cursor()

    sql = """
        UPDATE document
        SET solrize_end = %(now)s, solr_api_error = null
        WHERE hash = %(doc_hash)s
    """

    data = {
        "doc_hash": doc_hash,
        "now": datetime.now(),
    }

    cur.execute(sql, data)
    
    conn.commit()
    cur.close()

def resetFailedFlattens(conn):
    cur = conn.cursor()

    sql = """
        UPDATE document
        SET flatten_start = null
        WHERE flatten_end is null
        AND flatten_api_error != ANY(ARRAY[422, 413, 400])
    """

    cur.execute(sql)
    
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

def updateAdhocFileAsUnavailable(conn, hash, status):
    cur = conn.cursor()

    sql="UPDATE adhoc_validation SET downloaded = %(dt)s, download_error = %(status)s WHERE id = %(id)s"

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
        INSERT INTO publisher (org_id, description, title, name, image_url, state, country_code, created, last_seen, package_count, iati_id, type_code, contact, contact_email, first_publish_date)
        VALUES (%(org_id)s, %(description)s, %(title)s, %(name)s, %(image_url)s, %(state)s, %(country_code)s, %(last_seen)s, %(last_seen)s, %(package_count)s, %(iati_id)s, %(type_code)s, %(contact)s, %(contact_email)s, %(first_publish_date)s)
        ON CONFLICT (org_id) DO
            UPDATE SET title = %(title)s,
                state = %(state)s,
                image_url = %(image_url)s,
                description = %(description)s,
                last_seen = %(last_seen)s,
                package_count = %(package_count)s,
                iati_id = %(iati_id)s,
                type_code = %(type_code)s,
                contact = %(contact)s,
                contact_email = %(contact_email)s,
                first_publish_date = %(first_publish_date)s
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
        "iati_id": organization['publisher_iati_id'],
        "type_code": organization['publisher_organization_type'],
        "contact": organization['publisher_contact']
    }

    try:
        data["image_url"] = organization['image_url']
    except KeyError:
        data["image_url"] = None

    try:
        data["contact_email"] = organization['publisher_contact_email']
    except KeyError:
        data["contact_email"] = None

    if 'publisher_first_publish_date' in organization and organization['publisher_first_publish_date']:
        data["first_publish_date"] = organization['publisher_first_publish_date']
    else:
        data["first_publish_date"] = None

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
                validation = null
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
                valid = %(valid)s,
                created = %(created)s
            WHERE validation.document_hash=%(doc_hash)s;
        UPDATE document 
            SET validation=%(doc_hash)s,
                regenerate_validation_report = 'f'
            WHERE hash=%(doc_hash)s;
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

def updateAdhocValidationState(conn, doc_hash, state, report):

    cur = conn.cursor()

    if state is None:
        sql = "UPDATE adhoc_validation SET validated=null, report=null, validated=null WHERE hash=%s"
        data = (doc_hash)
        cur.execute(sql, data)
        conn.commit()
        cur.close()
        return

    sql = """
        UPDATE adhoc_validation (hash, valid, report)  
        SET valid=%(state)s, report=%(report)s, validated = %(validated)s
        WHERE hash=%(doc_hash)s;
        """

    data = {
        "doc_hash": doc_hash,
        "validated": datetime.now(),
        "valid": state,
        "report": report
    }

    cur.execute(sql, data)
    conn.commit()
