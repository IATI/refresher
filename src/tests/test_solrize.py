import pytest

from library.solrize import get_explode_element_data, validateLatLon


def test_validateLatLon_pass_1():
    assert "1.0,1.0" == validateLatLon("1 1")


def test_validateLatLon_fail_1():
    assert validateLatLon("1 1000") is None


GET_EXPLODE_ELEMENT_DATA = [
    # General test
    (
        "transaction",
        [
            {
                "transaction_value": 100,
                # Sector Code value is only on some transactions. In the output, we can check it has not been copied to all transactions.
                "transaction_sector_code": 1,
            },
            {
                "transaction_value": 200,
            },
        ],
        {
            # These fields are a collection of all the transaction data flattened to an activity. These should be replaced in the output items.
            "transaction_sector_code": 1,
            "transaction_value": [100, 200],
            # These fields of the activity are nothing to do with transactions and should be copied to all output items.
            "iati_identifier": "ID",
        },
        [
            {
                "transaction_value": 100,
                "transaction_sector_code": 1,
                "iati_identifier": "ID",
                "id": "8c9c0d8144b6a0a68b85d0d672c10f52afe290fc",
            },
            {
                "transaction_value": 200,
                "iati_identifier": "ID",
                "id": "721c2cd56dedbf5b8e11e21464bc381084ef70fb",
            },
        ],
    ),
    # Test where 2 transactions are exactly the same - they should get different id's
    (
        "transaction",
        [
            {
                "transaction_value": 100,
            },
            {
                "transaction_value": 100,
            },
        ],
        {
            "transaction_value": [100, 100],
            "iati_identifier": "ID",
        },
        [
            {
                "transaction_value": 100,
                "iati_identifier": "ID",
                "id": "8deb2daf9d100142370b844168e08e312fe400aa",
            },
            {
                "transaction_value": 100,
                "iati_identifier": "ID",
                "id": "71f347cd714d5fc249367dfbe5979fa33f9c0e14",
            },
        ],
    ),
]


@pytest.mark.parametrize("element_name, element_data, activity_data, expected_output", GET_EXPLODE_ELEMENT_DATA)
def test_get_explode_element_data(element_name, element_data, activity_data, expected_output):
    out = get_explode_element_data(element_name, element_data, activity_data)
    assert out == expected_output
