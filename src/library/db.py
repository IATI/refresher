import os
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, String, and_, or_
from sqlalchemy.types import Boolean
import alembic.config
from alembic.migration import MigrationContext
from constants.version import __version__
from constants.config import config

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
    context = MigrationContext.configure(conn)
    if context.get_current_revision() == None:
        current_migration_version = None
        upgrade = True
    else:
        current_migration_version = convert_migration_to_version(context.get_current_revision())
        upgrade = isUpgrade(current_migration_version, __version__)

    if current_migration_version != __version__:

        if upgrade:
            alembicArgs = [
            '--raiseerr',
            'upgrade', convert_version_to_migration(__version__),
            ]
        else:
            alembicArgs = [
            '--raiseerr',
            'downgrade', convert_version_to_migration(__version__),
            ]

        alembic.config.main(argv=alembicArgs)

def getDatasets(engine):
    meta = getMeta(engine)
    meta.reflect()
    return Table(config['DATA_TABLENAME'], meta, schema=config['DATA_SCHEMA'], autoload=True)
