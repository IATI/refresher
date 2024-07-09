import pytest

from library.utils import get_hash_for_identifier, parse_xsd_date_value


def test_get_hash_for_identifier_1():
    assert "9d989e8d27dc9e0ec3389fc855f142c3d40f0c50" == get_hash_for_identifier("cat")

PARSE_XSD_DATE_VALUE = [
    # just nonsense
    ('cat', None),
    # dates only
    ('2023-11-15', '2023-11-15T00:00:00'),
    # dates only ... that aren't valid
    ('2023-13-15', None),
    ('2023-00-15', None),
    ('2023-01-32', None),
    ('2023-02-30', None),
    # dates and Z time zones
    ('2023-11-15Z', '2023-11-15T00:00:00'),
    # dates and Z time zones ... that aren't valid
    ('2023-13-15Z', None),
    ('2023-00-15Z', None),
    ('2023-01-32Z', None),
    ('2023-02-30Z', None),
    # dates and offset time zones
    ('2023-11-15+00:00', '2023-11-15T00:00:00+00:00'),
    ('2023-11-15+01:00', '2023-11-15T00:00:00+01:00'),
    ('2023-11-15+01:30', '2023-11-15T00:00:00+01:30'),
    ('2023-11-15-00:00', '2023-11-15T00:00:00+00:00'),
    ('2023-11-15-01:00', '2023-11-15T00:00:00-01:00'),
    ('2023-11-15-01:30', '2023-11-15T00:00:00-01:30'),
    # dates and offset time zones ... that aren't valid
    ('2023-13-15-00:00', None),
    ('2023-00-15-00:00', None),
    ('2023-01-32-00:00', None),
    ('2023-02-30-00:00', None),
    # This should be valid in xsd:date but python can't handle years bigger than 9999
    ('10000-01-01', None),
]

@pytest.mark.parametrize("in_value, expected_value", PARSE_XSD_DATE_VALUE)
def test_parse_xsd_date_value(in_value, expected_value):
    actual = parse_xsd_date_value(in_value)
    if expected_value:
        assert actual.isoformat() == expected_value
    else:
        assert actual is None
