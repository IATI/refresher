from library.solrize import validateLatLon

def test_validateLatLon_pass_1():
    assert "1.0,1.0" == validateLatLon("1 1")


def test_validateLatLon_fail_1():
    assert validateLatLon("1 1000") is None

