from library.lakify import recursive_json_nest
import os.path
from lxml import etree
import json
import pytest


RECURSIVE_JSON_NEST_FILES = [
    ('basic'),
]

@pytest.mark.parametrize("filename", RECURSIVE_JSON_NEST_FILES)
def test_recursive_json_nest(filename):
    xml_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), "fixtures_lakify_recursive_json_nest", filename + ".input.xml")
    json_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), "fixtures_lakify_recursive_json_nest", filename + ".expected.json")

    # Get input, Process
    context = etree.iterparse(xml_filename, tag='iati-activity', huge_tree=True)
    _, activity = next(context)
    output = recursive_json_nest(activity, {})

    # Handy tip: to generate .expected.json file, use the below code and run the tests
    #with open(json_filename, "w") as fp:
    #    json.dump(output, fp, indent=2, sort_keys = True)
    # Note you must then check the diff very carefully to make sure it's what you expect!!!
    # This is not an easy way to just make your tests pass!
    # The diff in the expected data should be reviewed very carefully in pull requests!

    # Get Expected
    with open(json_filename) as fp:
        expected = json.load(fp)

    # Compare
    assert expected == output




