import os, importlib, pathlib, sys
from sqlalchemy import create_engine, MetaData, Table, Column, String, and_, or_
from sqlalchemy.types import Boolean
from constants.version import __version__
from constants.config import config
import psycopg2
import logging

logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('refresher')
logger.setLevel(logging.INFO)

def getDirectConnection():
    return psycopg2.connect(database=config['DB_NAME'], user=config['DB_USER'], password=config['DB_PASS'], host=config['DB_HOST'], port=config['DB_PORT'])

def getDbEngine():
    return create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}?sslmode=require".format(config['DB_USER'], config['DB_PASS'], config['DB_HOST'], config['DB_PORT'], config['DB_NAME']))

def getMeta(engine):
    return MetaData(engine)

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
    except psycopg2.errors.UndefinedTable as e:
        return None
    
    if len(result) != 1:
        return None
    else:
        return {'number': result[0][0], 'migration': result[0][1]}

def set_current_db_version(number, migration, current_number):
    db.getUnvalidatedDatasets(conn)
    cursor = conn.cursor()

    if current_number == '0.0.0':
        sql = 'INSERT INTO version (number, migration) values (%s, %s)'
        cursor.execute(sql, (number, migration))
    else:
        sql = 'UPDATE version SET number = %s, migration = %s WHERE number = %s'
        cursor.execute(sql, (number, migration, current_number))    
    
    conn.commit()
    conn.close()


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

    cursor.close()
    conn.close()

    set_current_db_version(__version__['number'], __version__['migration'], current_db_version['number'])   


def getDatasets(engine):
    meta = getMeta(engine)
    meta.reflect()
    return Table(config['DATA_TABLENAME'], meta, schema=config['DATA_SCHEMA'], autoload=True)

def getUnvalidatedDatasets(conn):    
    cur = conn.cursor()

    sql = "SELECT hash FROM refresher WHERE valid is Null"

    cur.execute(sql)
    
    return cur.fetchall()

    cur.close()

def getUnprocessedDatasets(conn):    
    cur = conn.cursor()

    sql = "SELECT hash FROM refresher WHERE root_element_key is Null"

    cur.execute(sql)
    
    return cur.fetchall()

    cur.close()


def updateValidationState(conn, filehash, state):

    cur = conn.cursor()

    sql = "UPDATE refresher SET valid=%s WHERE hash=%s"

    data = (state, filehash)

    cur.execute(sql, data)

    conn.commit()        

    cur.close()
