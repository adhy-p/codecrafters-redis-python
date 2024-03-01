from app.rdb_parser import RdbParser


def test_parse():
    with open("dump.rdb", "rb") as f:
        rdb_file = f.read()
    kv_store = RdbParser.parse(rdb_file)
    assert kv_store == {b"key": b"value", b"foo": b"bar"}


test_parse()
