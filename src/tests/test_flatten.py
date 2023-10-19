from library.flatten import Flattener, FlattenerException
import os.path
from lxml import etree
import json
import pytest


FLATTENER_FILES = [
    ('historic_activity_checks'),
    ('historic_sector_percentage_blanks'),
    ('historic_blank_narratives'),
    ('historic_date_format'),
    ('historic_date_format_empty_multivalue'),
    ('historic_date_format_no_datetimes'),
    ('historic_iati_identifier_checks'),
    ('historic_multiple_activity'),
    ('historic_multiple_transactions'),
    ('historic_multivalued_attribute_blank_string'),
    ('historic_nan_for_numeric_multivalued_blanks'),
    ('historic_sector_percentage_2_activities'),
    ('extension_on_activity'),
    ('extension_no_xmlns'),
    ('date_format_timezones'),
]

@pytest.mark.parametrize("filename", FLATTENER_FILES)
def test_flattener(filename):
    xml_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), "fixtures_flatten_flatterer", filename + ".input.xml")
    json_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), "fixtures_flatten_flatterer", filename + ".expected.json")

    # Get input, Process
    worker = Flattener()
    output = worker.process(xml_filename)

    #print(json.dumps(output, indent=2))

    # Get Expected
    with open(json_filename) as fp:
        expected = json.load(fp)

    # Compare
    assert expected == output

FLATTENER_EXCEPTION_FILES = [
    ('not-iati', 'Non-IATI XML')
]

@pytest.mark.parametrize("filename,expected_message", FLATTENER_EXCEPTION_FILES)
def test_flattener_exception(filename, expected_message):
    xml_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), "fixtures_flatten_flatterer", filename + ".input.xml")

    # Get input, Process
    worker = Flattener()
    with pytest.raises(FlattenerException) as excinfo:
        worker.process(xml_filename)

    # Compare
    assert expected_message in str(excinfo)
