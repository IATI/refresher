import datetime
import hashlib
import re

import chardet

from library.logger import getLogger

logger = getLogger("utils")


def get_text_from_blob(downloader, file_hash, with_encoding=False):
    # save off bytes if we need to detect charset later
    downloadBytes = downloader.content_as_bytes()
    try:
        if with_encoding:
            return (downloader.content_as_text(), "utf-8")
        return downloader.content_as_text()
    except UnicodeDecodeError:
        logger.info("File is not UTF-8, trying to detect encoding for file with hash " + file_hash)
        pass

    # If not UTF-8 try to detect charset and decode
    try:
        detect_result = chardet.detect(downloadBytes)
        charset = detect_result["encoding"]
        if charset:
            logger.info(
                "Charset detected: "
                + charset
                + " Confidence: "
                + str(detect_result["confidence"])
                + " Language: "
                + detect_result["language"]
                + " for file with hash "
                + file_hash
            )
            if with_encoding:
                return (downloader.content_as_text(encoding=charset), charset)
            return downloader.content_as_text(encoding=charset)
        logger.warning("No Charset detected for file with hash " + file_hash + ". Likely a non-text file.")
        raise
    except:
        logger.warning("Could not determine charset to decode for file with hash " + file_hash)
        raise


def get_hash_for_identifier(id):
    identifier_hash = hashlib.sha1()
    identifier_hash.update(id.encode())
    return identifier_hash.hexdigest()


def chunk_list(a_list, n):
    for i in range(0, n):
        yield a_list[i::n]


class TimeZoneFixedOffset(datetime.tzinfo):
    def __init__(self, hours, mins):
        self.hours = hours
        self.mins = mins

    def utcoffset(self, dt):
        if self.hours > 0:
            return datetime.timedelta(hours=self.hours, minutes=self.mins)
        else:
            return datetime.timedelta(hours=self.hours, minutes=(0 - self.mins))

    def tzname(self, dt):
        return "UTC{hours:+02d}:{mins:02d}".format(hours=self.hours, mins=self.mins)

    def dst(self, dt):
        return datetime.timedelta(0)


def parse_xsd_date_value(in_str):
    """
    Takes in a string that may be a valid xsd:date value

    Returns a datetime object if it is (None is if it is not)

    Years larger than 9999 should work but Python won't let us.
    See https://www.w3.org/TR/xmlschema-2/#date section 3.2.9.1 leading to section 3.2.7.1
    """
    # Date only
    try:
        v = datetime.datetime.strptime(in_str, "%Y-%m-%d")
        if v:
            return v
    except ValueError:
        pass
    # Date and Z time zone
    try:
        v = datetime.datetime.strptime(in_str, "%Y-%m-%dZ")
        if v:
            return v
    except ValueError:
        pass
    # Date and plus minus time zone
    # We can't use %z as that works with -/+0000
    # and https://www.w3.org/TR/xmlschema-2/#dateTime section 3.2.7.3 defines -/+00:00
    try:
        match = re.search(r"^(\d\d\d\d)-(\d\d)-(\d\d)(\-|\+)(\d\d):(\d\d)$", in_str)
        if match:
            tzinfo = TimeZoneFixedOffset(
                int(match.group(5)) if match.group(4) == "+" else 0 - int(match.group(5)), int(match.group(6))
            )
            dt = datetime.datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)), tzinfo=tzinfo)
            # We ignore the time zone part for now
            return dt
    except ValueError:
        pass
    # We fail
    return None
