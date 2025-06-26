import json
import os
import tempfile
import time
import traceback
from multiprocessing import Process

import dateutil.parser
from azure.core import exceptions as AzureExceptions
from azure.storage.blob import BlobServiceClient
from lxml import etree

import library.db as db
import library.utils as utils
from constants.config import config
from library.logger import getLogger
from library.prometheus import set_prom_metric

logger = getLogger("flatten")
config_explode_elements = json.loads(config["SOLRIZE"]["EXPLODE_ELEMENTS"])  # type: ignore


class Flattener:

    def __init__(self, sub_list_elements=config_explode_elements):
        self.sub_list_elements = sub_list_elements

    def process(self, input_filename):
        # Check right type of XML file, get attributes from root
        large_parser = etree.XMLParser(huge_tree=True, recover=True, remove_comments=True)
        root = etree.parse(input_filename, parser=large_parser).getroot()

        if root.tag != "iati-activities":
            raise FlattenerException("Non-IATI XML")

        root_attributes = {}

        # Previous tool always added version, even if it was blank
        self._add_to_output("dataset_version", root.attrib.get("version", ""), root_attributes)
        if root.attrib.get("generated-datetime"):
            self._add_to_output("dataset_generated_datetime", root.attrib.get("generated-datetime"), root_attributes)
        if root.attrib.get("linked-data-default"):
            self._add_to_output("dataset_linked_data_default", root.attrib.get("linked-data-default"), root_attributes)

        del root

        # Start Output
        output = []

        # Process
        context = etree.iterparse(
            input_filename, tag="iati-activity", huge_tree=True, recover=True, remove_comments=True
        )
        for _, activity in context:
            nsmap = activity.nsmap
            activity_attribs = activity.attrib
            # Start
            activity_output = root_attributes.copy()
            # Activity Data
            self._process_tag(activity, activity_output, nsmap=nsmap, activity_attribs=activity_attribs)
            # Sub lists?
            for child_tag_name in self.sub_list_elements:
                child_tags = activity.findall(child_tag_name)
                if child_tags:
                    activity_output["@" + child_tag_name] = []
                    for child_tag in child_tags:
                        child_tag_data = {}
                        # TODO this isn't the most efficient as we are parsing the same tag twice
                        # (here & above for activity)
                        # But for now, we'll do this to prove functionality then look at speed.
                        self._process_tag(child_tag, child_tag_data, prefix=child_tag_name, nsmap=nsmap, activity_attribs=activity_attribs)
                        activity_output["@" + child_tag_name].append(child_tag_data)
            # We have output
            output.append(activity_output)

        # Return
        return output

    TAGS_THAT_SHOULD_USE_DEFAULT_CURRENCY = ["budget_value", "transaction_value", "planned_disbursement_value"]

    def _process_tag(self, xml_tag, output, prefix="", nsmap={}, activity_attribs={}):

        # Attributes
        for attrib_k, attrib_v in xml_tag.attrib.items():

            self._add_to_output(
                self._convert_name_to_canonical(attrib_k, prefix=prefix, nsmap=nsmap), attrib_v, output
            )
        # Anything missing to fill in from activity?
        if prefix in self.TAGS_THAT_SHOULD_USE_DEFAULT_CURRENCY and "currency" not in xml_tag.attrib.keys() and "default-currency" in activity_attribs.keys():
            self._add_to_output(
                self._convert_name_to_canonical("currency", prefix=prefix, nsmap=nsmap), activity_attribs["default-currency"], output
            )

        # Immediate text
        if xml_tag.text and xml_tag.text.strip() and prefix:
            self._add_to_output(prefix, xml_tag.text.strip(), output)

        # Child tags
        for child_xml_tag in xml_tag.getchildren():
            self._process_tag(
                child_xml_tag,
                output,
                prefix=self._convert_name_to_canonical(child_xml_tag.tag, prefix=prefix, nsmap=nsmap),
                nsmap=nsmap,
                activity_attribs=activity_attribs
            )

    CANONICAL_NAMES_WITH_DATE_TIMES = ["iso_date", "value_date", "extraction_date", "_datetime"]

    def _add_to_output(self, canonical_name, value, output):
        # Basic processing
        value = value.strip()

        # clean iati_identifier so hash matches lakifier
        if canonical_name == "iati_identifier":
            value = value.replace("\n", "").replace("\r", "")

        # Date time?
        if [x for x in self.CANONICAL_NAMES_WITH_DATE_TIMES if x in canonical_name]:
            try:
                dt_object = utils.parse_xsd_date_value(value) or dateutil.parser.parse(value)
                if dt_object:
                    # This mirrors output of old flaterrer system
                    value = dt_object.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                else:
                    # If can't parse, don't add as solr will error
                    return
            except (dateutil.parser.ParserError, dateutil.parser.UnknownTimezoneWarning):
                # If can't parse, don't add as solr will error
                return

        # Add to output
        if canonical_name in output:
            if isinstance(output[canonical_name], list):
                output[canonical_name].append(value)
            else:
                output[canonical_name] = [output[canonical_name], value]
        else:
            output[canonical_name] = value

    DEFAULT_NAMESPACES = {"xml": "http://www.w3.org/XML/1998/namespace"}

    def _convert_name_to_canonical(self, name, prefix="", nsmap={}):
        for ns, url in self.DEFAULT_NAMESPACES.items():
            if name.startswith("{" + url + "}"):
                name = ns + "_" + name[len(url) + 2 :]
        for ns, url in nsmap.items():
            if name.startswith("{" + url + "}"):
                name = ns + "_" + name[len(url) + 2 :]
        name = name.replace("-", "_")
        name = name.replace(":", "_")
        return prefix + "_" + name if prefix else name


class FlattenerException(Exception):
    pass


def process_hash_list(document_datasets):

    conn = db.getDirectConnection()
    flattener = Flattener()

    for file_data in document_datasets:
        tempfile_name = None
        try:
            file_hash = file_data[0]
            downloaded = file_data[1]
            doc_id = file_data[2]
            prior_error = file_data[3]
            tempfile_handle, tempfile_name = tempfile.mkstemp(prefix="flatten" + file_hash)

            # Explicit error codes returned from Flattener
            if prior_error == 422 or prior_error == 400 or prior_error == 413:
                logger.debug(
                    "Skipping file with hash {} doc id {}, downloaded at {}, due to prior {}".format(
                        file_hash, doc_id, downloaded.isoformat(), prior_error
                    )
                )
                continue

            db.startFlatten(conn, doc_id)

            logger.info(
                "Flattening file with hash {} doc id {}, downloaded at {}".format(
                    file_hash, doc_id, downloaded.isoformat()
                )
            )
            blob_name = file_hash + ".xml"

            blob_service_client = BlobServiceClient.from_connection_string(config["STORAGE_CONNECTION_STR"])
            blob_client = blob_service_client.get_blob_client(container=config["CLEAN_CONTAINER_NAME"], blob=blob_name)

            downloader = blob_client.download_blob()

            try:
                payload = utils.get_text_from_blob(downloader, file_hash)
            except:
                logger.warning("Can not identify charset for hash {} doc id {}".format(file_hash, doc_id))
                continue

            os.write(tempfile_handle, payload.encode("utf-8"))
            os.close(tempfile_handle)

            del payload

            flattenedObject = flattener.process(tempfile_name)

            db.completeFlatten(conn, doc_id, json.dumps(flattenedObject))

            os.remove(tempfile_name)

        except AzureExceptions.ResourceNotFoundError:
            logger.warning(
                "Blob not found for hash " + file_hash + " - updating as Not Downloaded for the refresher to pick up."
            )
            db.updateFileAsNotDownloaded(conn, doc_id)
        except (Exception, FlattenerException) as e:
            # Log to logs
            logger.error("ERROR with flattening " + file_hash)
            print(traceback.format_exc())
            if hasattr(e, "message"):
                logger.error("ERROR message: " + str(e.message))
            if hasattr(e, "msg"):
                logger.error("ERROR msg: " + str(e.msg))
            try:
                logger.warning("ERROR args: " + str(e.args[0]))
            except:
                pass
            # Log to DB
            db.updateFlattenError(conn, doc_id, 1)
            # Delete temp file
            if tempfile_name:
                try:
                    os.remove(tempfile_name)
                except:
                    pass

    conn.close()


def service_loop():
    logger.info("Start service loop")

    while True:
        main()
        time.sleep(60)


def main():
    logger.info("Starting to flatten...")

    conn = db.getDirectConnection()

    logger.info("Resetting those unfinished")

    db.resetUnfinishedFlattens(conn)

    file_hashes = db.getUnflattenedDatasets(conn)

    set_prom_metric("datasets_to_flatten", len(file_hashes))

    if config["FLATTEN"]["PARALLEL_PROCESSES"] == 1:
        logger.info("Flattening and storing " + str(len(file_hashes)) + " IATI files in a single process.")
        process_hash_list(file_hashes)
    else:
        chunked_hash_lists = list(utils.chunk_list(file_hashes, config["FLATTEN"]["PARALLEL_PROCESSES"]))

        processes = []

        logger.info(
            "Flattening and storing "
            + str(len(file_hashes))
            + " IATI files in a maximum of "
            + str(config["FLATTEN"]["PARALLEL_PROCESSES"])
            + " parallel processes."
        )

        for chunk in chunked_hash_lists:
            if len(chunk) == 0:
                continue
            process = Process(target=process_hash_list, args=(chunk,))
            process.start()
            processes.append(process)

        for process in processes:
            process.join()

    conn.close()
    logger.info("Finished.")
