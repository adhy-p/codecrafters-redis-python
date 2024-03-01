from app.rdb_parser import RdbParser


def test_parse():
    with open("dump.rdb", "rb") as f:
        rdb_file = f.read()
    kv_store, expiry_store = RdbParser.parse(rdb_file)
    assert kv_store == {
        b"good_key": b"OK",
        b"good_until_8mar": b"OK",
    }
    assert expiry_store == {b"good_until_8mar": 1709908810342}


test_parse()
