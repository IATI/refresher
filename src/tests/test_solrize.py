from library.solrize import validateLatLon, get_explode_element_data
import pytest

def test_validateLatLon_pass_1():
    assert "1.0,1.0" == validateLatLon("1 1")


def test_validateLatLon_fail_1():
    assert validateLatLon("1 1000") is None


GET_EXPLODE_ELEMENT_DATA = [
    (
        'transaction',
        [
            {
                'transaction_value': 100,
                # Sector Code value is only on some transactions. In the output, we can check it has not been copied to all transactions.
                'transaction_sector_code': 1
            },
            {
                'transaction_value': 200,
            }
        ],
        {
            # These fields are a collection of all the transaction data flattened to an activity. These should be replaced in the output items.
            "transaction_sector_code": 1,
            "transaction_value": [ 100, 200],
            # These fields of the activity are nothing to do with transactions and should be copied to all output items.
            "iati_identifier": "ID",
        },
        [
            {
                'transaction_value': 100,
                "transaction_sector_code": 1,
                "iati_identifier": "ID",
            },
            {
                'transaction_value': 200,
                "iati_identifier": "ID",
            }
        ]
    ),
]

@pytest.mark.parametrize("element_name, element_data, activity_data, expected_output", GET_EXPLODE_ELEMENT_DATA)
def test_get_explode_element_data(element_name, element_data, activity_data, expected_output):
    out = get_explode_element_data(element_name, element_data, activity_data)
    assert out == expected_output

