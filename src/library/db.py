import importlib
import pathlib
import time
from datetime import datetime

import psycopg2

from constants.config import config
from constants.version import __version__
from library.logger import getLogger

logger = getLogger()


def getDirectConnection(retry_counter=0):
    try:
        connection = psycopg2.connect(
            dbname=config["DB_NAME"],
            user=config["DB_USER"],
            password=config["DB_PASS"],
            host=config["DB_HOST"],
            port=config["DB_PORT"],
            sslmode=config["DB_SSL_MODE"],
            connect_timeout=config["DB_CONN_TIMEOUT"],
            keepalives=1,
            keepalives_idle=config["DB_KEEPALIVE_IDLE"],
            keepalives_interval=config["DB_KEEPALIVE_INTERVAL"],
            keepalives_count=config["DB_KEEPALIVE_COUNT"],
        )
        retry_counter = 0
        return connection
    except psycopg2.OperationalError as e:
        if retry_counter >= config["DB_CONN_RETRY_LIMIT"]:
            raise e
        else:
            retry_counter += 1
            logger.warning(
                "Error connecting: psycopg2.OperationalError: {}. reconnecting {}".format(
                    str(e).strip(), retry_counter
                )
            )
            sleep_time = config["DB_CONN_SLEEP_START"] * retry_counter * retry_counter
            if sleep_time > config["DB_CONN_SLEEP_MAX"]:
                sleep_time = config["DB_CONN_SLEEP_MAX"]
            logger.info("Sleeping {}s".format(sleep_time))
            time.sleep(sleep_time)
            return getDirectConnection(retry_counter)
    except (Exception, psycopg2.Error) as e:
        logger.error("Error connecting: {}. reconnecting {}".format(str(e).strip(), retry_counter))
        raise e


def get_current_db_version(conn):
    sql = "SELECT number, migration FROM version LIMIT 1"

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
        return {"number": result[0][0], "migration": result[0][1]}


def checkVersionMatch():
    conn = getDirectConnection()
    conn.set_session(autocommit=True)
    current_db_version = get_current_db_version(conn)

    while current_db_version["migration"] != __version__["migration"]:
        logger.info("DB version incorrect. Sleeping...")
        time.sleep(60)
        current_db_version = get_current_db_version(conn)

    conn.close()
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
        current_db_version = {"number": "0.0.0", "migration": -1}

    if current_db_version["migration"] == __version__["migration"]:
        logger.info("DB at correct version ({})".format(current_db_version["migration"]))
        return

    upgrade = __version__["migration"] > current_db_version["migration"]

    if upgrade:
        logger.info("DB upgrading to version " + str(__version__["migration"]))
        step = 1
    else:
        logger.info("DB downgrading to version " + str(__version__["migration"]))
        step = -1

    try:
        for i in range(current_db_version["migration"] + step, __version__["migration"] + step, step):
            if upgrade:
                mig_num = i
            else:
                mig_num = i + 1

            migration = "mig_" + str(mig_num)

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

            sql = sql.replace("\n", " ")
            sql = sql.replace("\t", " ")

            logger.info("Making schema " + logmessage + " in migration " + str(mig_num))

            cursor.execute(sql)

        sql = "UPDATE public.version SET number = %s, migration = %s"
        cursor.execute(sql, (__version__["number"], __version__["migration"]))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Encountered unexpected exemption during migration... Rolling back...")
        conn.rollback()
        conn.close()
        raise e


def getRefreshDataset(conn, retry_errors=False):
    cursor = conn.cursor()

    if retry_errors:
        sql = (
            "SELECT id, hash, bds_cache_url FROM document WHERE downloaded is null "
            "AND (download_error != 3 OR download_error is null)"
        )
    else:
        sql = "SELECT id, hash, bds_cache_url FROM document WHERE downloaded is null AND download_error is null"

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
    SELECT document.hash, document.downloaded, document.id, document.url, document.publisher,
           publisher.name, document.file_schema_valid, publisher.black_flag
    FROM document
    LEFT JOIN publisher
        ON document.publisher = publisher.org_id
    WHERE downloaded is not null
    AND download_error is null
    AND hash != ''
    AND (validation is Null OR regenerate_validation_report is True)
    ORDER BY regenerate_validation_report DESC, downloaded
    """
    cur.execute(sql)
    results = cur.fetchall()
    cur.close()
    return results


def removeBlackFlag(conn, org_id):
    cur = conn.cursor()
    sql = """
    UPDATE publisher as pub
    SET black_flag = null, black_flag_notified = null
    WHERE pub.org_id = %(org_id)s
    """

    data = {
        "org_id": org_id,
    }

    cur.execute(sql, data)
    conn.commit()
    cur.close()


def blackFlagDubiousPublishers(conn, threshold, period_in_hours):
    cur = conn.cursor()
    sql = """
    UPDATE publisher
    SET black_flag = NOW()
    WHERE org_id IN (
        SELECT publisher
        FROM document
        WHERE file_schema_valid = false
        AND NOW() - downloaded < interval ' %(period_in_hours)s hours'
        GROUP BY publisher
        HAVING COUNT(*) >  %(threshold)s
    )
    """

    data = {
        "threshold": threshold,
        "period_in_hours": period_in_hours,
    }

    cur.execute(sql, data)
    conn.commit()
    cur.close()


def getUnnotifiedBlackFlags(conn):
    cur = conn.cursor()

    sql = """
    SELECT org_id
    FROM publisher
    WHERE black_flag_notified is null
    AND black_flag is not null
    """

    cur.execute(sql)
    results = cur.fetchall()
    cur.close()
    return results


def updateBlackFlagNotified(conn, org_id, notified=True):
    cur = conn.cursor()

    sql = """
    UPDATE publisher as pub
    SET black_flag_notified = %(notified)s
    WHERE org_id=%(org_id)s
    """

    data = {"org_id": org_id, "notified": notified}

    cur.execute(sql, data)
    conn.commit()
    cur.close()


def getValidActivitiesDocsToCopy(conn):
    sql = """
    SELECT doc.hash, doc.id
    FROM document as doc
    LEFT JOIN validation as val ON doc.validation = val.id
    WHERE
    doc.downloaded is not null
    AND doc.hash != ''
    AND doc.clean_start is null
    AND clean_end is null
    AND val.valid = true
    AND val.report ->> 'fileType' = 'iati-activities'
    """

    with conn.cursor() as curs:
        curs.execute(sql)
        return curs.fetchall()


def getInvalidActivitiesDocsToClean(conn):
    sql = """
    SELECT doc.hash, doc.id, val.report -> 'iati-activities' as valid_index
    FROM document as doc
    LEFT JOIN validation as val ON doc.validation = val.id
    WHERE
    doc.downloaded is not null
    AND doc.hash != ''
    AND doc.clean_start is null
    AND clean_end is null
    AND val.valid = false
    AND val.report ->> 'fileType' = 'iati-activities'
    AND val.report ? 'iatiVersion' AND report->>'iatiVersion' != ''
    AND report->>'iatiVersion' NOT LIKE '1%%'
    AND report->>'iati-activities' LIKE '%%"valid": true%%'
    """

    with conn.cursor() as curs:
        curs.execute(sql)
        return curs.fetchall()


def getUnflattenedDatasets(conn):
    sql = """
    SELECT hash, downloaded, doc.id, flatten_api_error
    FROM document as doc
    WHERE doc.downloaded is not Null
    AND doc.clean_end is not Null
    AND doc.flatten_start is Null
    ORDER BY downloaded
    """
    with conn.cursor() as curs:
        curs.execute(sql)
        return curs.fetchall()


def getFlattenedActivitiesForDoc(conn, id):
    cur = conn.cursor()
    sql = """
    SELECT flattened_activities
    FROM document as doc
    WHERE doc.id = %(id)s
    """
    data = {"id": id}

    cur.execute(sql, data)
    results = cur.fetchall()
    cur.close()

    try:
        return results[0]
    except Exception:
        return None


def getUnsolrizedDatasets(conn):
    cur = conn.cursor()
    sql = """
    SELECT doc.hash, doc.id, doc.solr_api_error
    FROM document as doc
    LEFT JOIN validation as val ON doc.validation = val.id
    WHERE downloaded is not null
    AND doc.flatten_end is not null
    AND doc.lakify_end is not null
    AND doc.hash != ''
    AND val.report ? 'iatiVersion' AND report->>'iatiVersion' != ''
    AND report->>'iatiVersion' NOT LIKE '1%'
    AND (doc.solrize_end is null OR doc.solrize_reindex is True)
    AND doc.flattened_activities != '[]'
    ORDER BY doc.downloaded
    """
    cur.execute(sql)
    results = cur.fetchall()
    cur.close()
    return results


def getUnlakifiedDatasets(conn):
    cur = conn.cursor()
    sql = """
    SELECT hash, downloaded, doc.id
    FROM document as doc
    LEFT JOIN validation as val ON doc.validation = val.id
    WHERE doc.downloaded is not null
    AND doc.clean_end is not Null
    AND doc.lakify_start is Null
    AND doc.lakify_error is Null
    ORDER BY downloaded
    """
    cur.execute(sql)
    results = cur.fetchall()
    cur.close()
    return results


def resetUnfinishedLakifies(conn):
    cur = conn.cursor()
    sql = """
        UPDATE document
        SET lakify_start=null
        WHERE lakify_end is null
        AND lakify_error is null
    """

    cur.execute(sql)
    conn.commit()
    cur.close()


def resetUnfoundLakify(conn, doc_id):
    cur = conn.cursor()
    sql = """
        UPDATE document
        SET lakify_start=null,
        lakify_end=null,
        lakify_error=null
        WHERE id=%(doc_id)s
    """

    data = {
        "doc_id": doc_id,
    }

    cur.execute(sql, data)
    conn.commit()
    cur.close()


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


def resetUnfinishedCleans(conn):
    sql = """
        UPDATE document
        SET clean_start = null
        WHERE clean_end is null
    """

    with conn.cursor() as curs:
        curs.execute(sql)
    conn.commit()


def updateDocumentSchemaValidationStatus(conn, id, valid):
    sql = "UPDATE document SET file_schema_valid=%(valid)s WHERE id=%(id)s"

    data = {
        "id": id,
        "valid": valid,
    }

    with conn.cursor() as curs:
        curs.execute(sql, data)
    conn.commit()


def updateValidationRequestDate(conn, id):
    cur = conn.cursor()
    sql = "UPDATE document SET validation_request=%(dt)s WHERE id=%(id)s"

    date = datetime.now()

    data = {
        "id": id,
        "dt": date,
    }

    cur.execute(sql, data)
    conn.commit()
    cur.close()


def updateSolrizeStartDate(conn, id):
    cur = conn.cursor()
    sql = "UPDATE document SET solrize_start=%(dt)s WHERE id=%(id)s"

    date = datetime.now()

    data = {
        "id": id,
        "dt": date,
    }

    cur.execute(sql, data)
    conn.commit()
    cur.close()


def updateValidationError(conn, id, status):
    cur = conn.cursor()
    sql = "UPDATE document SET validation_api_error=%s WHERE id=%s"

    data = (status, id)
    cur.execute(sql, data)
    conn.commit()
    cur.close()


def updateSolrError(conn, id, error):
    cur = conn.cursor()
    sql = "UPDATE document SET solr_api_error=%s WHERE id=%s"

    data = (error, id)
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


def startLakify(conn, doc_id):
    cur = conn.cursor()

    sql = """
        UPDATE document
        SET lakify_start = %(now)s, lakify_error = null
        WHERE id = %(doc_id)s
    """

    data = {
        "doc_id": doc_id,
        "now": datetime.now(),
    }

    cur.execute(sql, data)

    conn.commit()
    cur.close()


def startClean(conn, doc_id):
    sql = """
        UPDATE document
        SET clean_start = %(now)s, clean_error = null
        WHERE id = %(doc_id)s
    """

    data = {
        "doc_id": doc_id,
        "now": datetime.now(),
    }

    with conn.cursor() as curs:
        curs.execute(sql, data)
    conn.commit()


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


def updateCleanError(conn, doc_id, error):
    sql = """
        UPDATE document
        SET clean_error = %(error)s
        WHERE id = %(doc_id)s
    """

    data = {
        "doc_id": doc_id,
        "error": error,
    }

    with conn.cursor() as curs:
        curs.execute(sql, data)
    conn.commit()


def completeFlatten(conn, doc_id, flattened_activities):
    cur = conn.cursor()

    sql = """
        UPDATE document
        SET flatten_end = %(now)s, flattened_activities = %(flat_act)s, flatten_api_error = null
        WHERE id = %(doc_id)s
    """

    data = {"doc_id": doc_id, "now": datetime.now(), "flat_act": flattened_activities}

    cur.execute(sql, data)

    conn.commit()
    cur.close()


def completeClean(conn, doc_id):
    sql = """
        UPDATE document
        SET clean_end = %(now)s, clean_error = null
        WHERE id = %(doc_id)s
    """

    data = {
        "doc_id": doc_id,
        "now": datetime.now(),
    }

    with conn.cursor() as curs:
        curs.execute(sql, data)
    conn.commit()


def lakifyError(conn, doc_id, msg):
    cur = conn.cursor()

    sql = """
        UPDATE document
        SET lakify_error = %(msg)s
        WHERE id = %(doc_id)s
    """

    data = {"doc_id": doc_id, "msg": msg}

    cur.execute(sql, data)

    conn.commit()
    cur.close()


def sendLakifyErrorToClean(conn, doc_id):
    cur = conn.cursor()

    sql = """
        UPDATE document
        SET lakify_start=null,
        lakify_end=null,
        lakify_error=null,
        clean_start = null,
        clean_end = null,
        clean_error = null
        WHERE id = %(doc_id)s
    """

    data = {
        "doc_id": doc_id,
    }

    cur.execute(sql, data)

    conn.commit()
    cur.close()


def completeLakify(conn, doc_id):
    cur = conn.cursor()

    sql = """
        UPDATE document
        SET lakify_end = %(now)s, lakify_error = null
        WHERE id = %(doc_id)s
    """

    data = {
        "doc_id": doc_id,
        "now": datetime.now(),
    }

    cur.execute(sql, data)

    conn.commit()
    cur.close()


def completeSolrize(conn, id):
    cur = conn.cursor()

    sql = """
        UPDATE document
        SET solrize_end = %(now)s,
            last_solrize_end = %(now)s,
            solr_api_error = null,
            solrize_reindex = 'f'
        WHERE id = %(id)s
    """

    data = {
        "id": id,
        "now": datetime.now(),
    }

    cur.execute(sql, data)

    conn.commit()
    cur.close()


def updateFileAsDownloaded(conn, id):
    cur = conn.cursor()

    sql = """
        UPDATE document
        SET downloaded = %(dt)s, download_error = null
        WHERE id = %(id)s
    """

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

    sql = """
        UPDATE document
        SET downloaded = null,
        clean_start = null,
        clean_end = null,
        clean_error = null
        WHERE id = %(id)s
    """

    data = {
        "id": id,
    }

    cur.execute(sql, data)
    conn.commit()
    cur.close()


def updateFileAsDownloadError(conn, id, status):
    cur = conn.cursor()

    sql = """
        UPDATE document
        SET downloaded = null, download_error = %(status)s
        WHERE id = %(id)s
    """

    data = {"id": id, "status": status}

    cur.execute(sql, data)
    conn.commit()
    cur.close()


def insertOrUpdatePublisher(conn, organization, last_seen):
    sql = """
        INSERT INTO publisher (org_id, title, name,
            created, last_seen, package_count, iati_id)
        VALUES (%(org_id)s, %(title)s, %(name)s,
            %(last_seen)s, %(last_seen)s, %(package_count)s, %(iati_id)s)
        ON CONFLICT (org_id) DO
            UPDATE SET title = %(title)s,
                last_seen = %(last_seen)s,
                package_count = %(package_count)s,
                iati_id = %(iati_id)s
            WHERE publisher.name=%(name)s
    """

    data = {
        "org_id": organization["id"],
        "title": organization["human_readable_name"],
        "name": organization["short_name"],
        "last_seen": last_seen,
        "package_count": organization["dataset_count"],
        "iati_id": organization["iati_identifier"],
    }

    with conn.cursor() as curs:
        curs.execute(sql, data)
    conn.commit()


def updatePublisherAsSeen(conn, name, last_seen):
    sql = """
        UPDATE publisher
        SET last_seen = %(last_seen)s
        WHERE publisher.name = %(name)s
    """

    data = {"last_seen": last_seen, "name": name}

    with conn.cursor() as curs:
        curs.execute(sql, data)
    conn.commit()


def insertOrUpdateDocument(conn, dt: datetime, dataset: dict):
    sql1 = """
        INSERT INTO document (id, hash, url, first_seen, last_seen, publisher, name, bds_cache_url)
        VALUES (%(id)s, %(hash)s, %(url)s, %(dt)s, %(dt)s, %(publisher_id)s, %(name)s, %(bds_cache_url)s)
        ON CONFLICT (id) DO
            UPDATE SET hash = %(hash)s,
                url = %(url)s,
                name = %(name)s,
                modified = %(dt)s,
                bds_cache_url = %(bds_cache_url)s,
                downloaded = null,
                download_error = null,
                validation_request = null,
                validation_api_error = null,
                validation = null,
                file_schema_valid = null,
                lakify_start = null,
                lakify_end = null,
                lakify_error = null,
                flatten_start = null,
                flatten_end = null,
                flatten_api_error = null,
                solrize_start = null,
                solrize_end = null,
                solr_api_error = null,
                clean_start = null,
                clean_end = null,
                clean_error = null
            WHERE document.id=%(id)s and document.hash != %(hash)s;
    """

    sql2 = """
            UPDATE document SET
            last_seen = %(dt)s,
            publisher = %(publisher_id)s,
            name = %(name)s
            WHERE document.id=%(id)s;
    """

    data = {
        "id": dataset["id"],
        "hash": dataset["hash"],
        "url": dataset["url"],
        "bds_cache_url": dataset["bds_cache_url"],
        "publisher_id": dataset["publisher_id"],
        "name": dataset["name"],
        "dt": dt,
    }

    with conn.cursor() as curs:
        curs.execute(sql1, data)
        curs.execute(sql2, data)
    conn.commit()


def getFileWhereHashChanged(conn, id, hash):
    cur = conn.cursor()

    sql = """
        SELECT id, hash FROM document
        WHERE document.id=%(id)s and document.hash != %(hash)s;
    """

    data = {"id": id, "hash": hash}

    cur.execute(sql, data)
    results = cur.fetchone()
    cur.close()
    return results


def getFilesNotSeenAfter(conn, dt):
    cur = conn.cursor()

    sql = """
        SELECT id, hash FROM document WHERE last_seen < %s
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


def getFilesFromPublishersNotSeenAfter(conn, dt):
    cur = conn.cursor()

    sql = """
        SELECT document.id, document.hash FROM document
            LEFT JOIN publisher
            ON document.publisher = publisher.org_id
            WHERE publisher.last_seen < %s
    """

    data = (dt,)

    cur.execute(sql, data)
    results = cur.fetchall()
    cur.close()
    return results


def removePublishersNotSeenAfter(conn, dt):
    cur = conn.cursor()

    sql = """
        DELETE FROM publisher WHERE last_seen < %s
    """

    data = (dt,)

    cur.execute(sql, data)
    conn.commit()
    cur.close()


def updateValidationState(conn, doc_id, doc_hash, doc_url, publisher, state, report, publisher_name):

    cur = conn.cursor()

    if state is None:
        sql = "UPDATE document SET validation=null WHERE id=%s"
        data = (doc_id,)
        cur.execute(sql, data)
        conn.commit()
        cur.close()
        return

    sql = """
        WITH new_id AS (
            INSERT INTO validation (document_id, document_hash, document_url, created,
                valid, report, publisher, publisher_name)
            VALUES (%(doc_id)s, %(doc_hash)s, %(doc_url)s, %(created)s,
                %(valid)s, %(report)s, %(publisher)s, %(publisher_name)s)
            RETURNING id
        )
        UPDATE document
            SET validation = (SELECT id FROM new_id),
            regenerate_validation_report = 'f'
            WHERE document.id = %(doc_id)s;
        """

    data = {
        "doc_id": doc_id,
        "doc_hash": doc_hash,
        "doc_url": doc_url,
        "created": datetime.now(),
        "valid": state,
        "report": report,
        "publisher": publisher,
        "publisher_name": publisher_name,
    }

    cur.execute(sql, data)
    conn.commit()


def getNumPublishers(conn):

    cur = conn.cursor()

    sql = """
        SELECT COUNT(*) FROM publisher
        """

    cur.execute(sql)

    conn.commit()
    results = cur.fetchone()
    cur.close()
    return results[0]


def getNumDocuments(conn):

    cur = conn.cursor()

    sql = """
        SELECT COUNT(*) FROM document
        """

    cur.execute(sql)

    conn.commit()
    results = cur.fetchone()
    cur.close()
    return results[0]
