from library.utils import get_hash_for_identifier

def test_get_hash_for_identifier_1():
    assert "9d989e8d27dc9e0ec3389fc855f142c3d40f0c50" == get_hash_for_identifier("cat")

