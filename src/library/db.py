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
    except psycopg2.errors.UndefinedTable as e:
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
        migration = 'mig_' + str(i)

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
        cursor.execute(sql, (number, migration, current_number))

    cursor.close()
    conn.close()

def getRefreshDataset(conn, retry_errors=False):
    cursor = conn.cursor()

    if retry_errors:
        sql = "SELECT id, hash, url FROM refresher WHERE downloaded is null"
    else:
        sql = "SELECT id, hash, url FROM refresher WHERE downloaded is null AND download_error is null"
    
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
    sql = "SELECT hash FROM refresher WHERE valid is Null"
    cur.execute(sql)    
    results = cur.fetchall()
    cur.close()
    return cur.fetchall()

def getUnprocessedDatasets(conn):    
    cur = conn.cursor()
    sql = "SELECT hash FROM refresher WHERE root_element_key is Null"
    cur.execute(sql)    
    results = cur.fetchall()
    cur.close()
    return cur.fetchall()


def updateValidationState(conn, filehash, state):
    cur = conn.cursor()
    sql = "UPDATE refresher SET valid=%s WHERE hash=%s"
    data = (state, filehash)
    cur.execute(sql, data)
    conn.commit()
    cur.close()

def updateFileAsDownloaded(conn, id):
    cur = conn.cursor()

    sql="UPDATE refresher SET downloaded = %(dt)s WHERE id = %(id)s"

    date = datetime.now()

    data = {
        "id": id,
        "dt": date,
    }

    cur.execute(sql, data)
    conn.commit()
    cur.close()

def updateFileAsDownloadError(conn, id, status):
    cur = conn.cursor()

    sql="UPDATE refresher SET downloaded = %(dt)s, download_error = %(status)s WHERE id = %(id)s"

    data = {
        "id": id,
        "dt": datetime.now(),
        "status": status
    }

    cur.execute(sql, data)
    conn.commit()
    cur.close()



def insertOrUpdateFile(conn, id, hash, url, dt):
    cur = conn.cursor()

    sql1 = """
        INSERT INTO refresher (id, hash, url, first_seen, last_seen) 
        VALUES (%(id)s, %(hash)s, %(url)s, %(dt)s, %(dt)s)
        ON CONFLICT (id) DO 
            UPDATE SET hash = %(hash)s,
                url = %(url)s,
                modified = %(dt)s,
                downloaded = null,
                download_error = null,
                validation_request = null,
                validation_api_error = null,
                valid = null,
                datastore_processing_start = null,
                datastore_processing_end = null
            WHERE refresher.id=%(id)s and refresher.hash != %(hash)s;
    """

    sql2 = """
            UPDATE refresher SET
            last_seen = %(dt)s
            WHERE refresher.id=%(id)s;
    """

    data = {
        "id": id,
        "hash": hash,
        "url": url,
        "dt": dt,
    }

    cur.execute(sql1, data)
    cur.execute(sql2, data)
    conn.commit()
    cur.close()

def getFilesNotSeenAfter(conn, dt):
    cur = conn.cursor()

    sql = """
        SELECT id, hash, url FROM refresher WHERE last_seen < %s
    """

    data = (dt,)

    cur.execute(sql, data)
    results = cur.fetchall()
    cur.close()
    return results


def removeFilesNotSeenAfter(conn, dt):
    cur = conn.cursor()

    sql = """
        DELETE FROM refresher WHERE last_seen < %s
    """

    data = (dt,)

    cur.execute(sql, data)
    conn.commit()
    cur.close()    
