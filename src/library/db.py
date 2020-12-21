import os
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, String, and_, or_
from sqlalchemy.types import Boolean
import alembic.config
from alembic.migration import MigrationContext
from constants.version import __version__
from constants.config import config
import psycopg2

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

def migrateIfRequired(conn):
    current_db_version = get_current_db_version(conn) #todo TUESDAY

    if current_db_version is None:
        current_db_version = {
            'name': '0.0.0'
            'migration': 0
        }

    if current_db_version['name'] == __version__['name']:
        return
        
    upgrade = isUpgrade(current_db_version['name'], __version__)

    if upgrade:
        step = 1
    else:
        step = -1

    for i in range(current_db_version['migration'], __version__['migration'] + step, step):
        migration = importlib.import_module('mig_' + str(i))

        if upgrade:
            sql = migration.upgrade
        else:
            sql = migration.downgrade
        
        with self.connection as cursor:
            cursor.execute(sql)


def getDatasets(engine):
    meta = getMeta(engine)
    meta.reflect()
    return Table(config['DATA_TABLENAME'], meta, schema=config['DATA_SCHEMA'], autoload=True)
