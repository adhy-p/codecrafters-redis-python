import sys
from enum import Enum, auto
from typing import Any

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("rdb_parser")


class RdbValueType(Enum):
    STRING = 0
    LIST = 1
    SET = 2
    SORTEDSET = 3
    HASH = 4
    ZIPMAP = 9
    ZIPLIST = 10
    INTSET = 11
    SORTEDSET_ZIPLIST = 12
    HASHMAP_ZIPLIST = 13
    LIST_QUICKLIST = 14


class RdbStringType(Enum):
    LEN_PREFIX_STR = auto()
    INT_8_STR = auto()
    INT_16_STR = auto()
    INT_32_STR = auto()
    COMPRESSED_STR = auto()


class RdbParser:
    @staticmethod
    def parse(data: bytes) -> tuple[dict[bytes, bytes], dict[bytes, int]]:
        (ok, remain) = RdbParser._parse_magic(data)
        if not ok:
            return {}
        logger.info("magic ok")
        (_version, remain) = RdbParser._parse_version(remain)
        logger.info(f"version ok: {_version}")
        aux_data = {}
        kvstore = {}
        expirystore = {}
        while remain:
            # logger.info(f"remain: {remain!r}")
            # logger.info(f"matching data: {remain[:1]!r}")
            match remain[:1]:
                case b"\xff":
                    logger.info("ff. done!")
                    break
                case b"\xfe":
                    # database selector
                    logger.info("parsing db selector")
                    db_number, remain = RdbParser._parse_db_selector(remain)
                    logger.info(f"db number: {db_number}")
                case b"\xfd":
                    # expiry time in seconds
                    logger.info("parsing expiry time (seconds)")
                    expiry_s, remain = RdbParser._parse_expiry_s(remain)
                    logger.info("parsing key value pair")
                    key, value, remain = RdbParser._parse_key_value(remain)
                    kvstore[key] = value
                    expirystore[key] = expiry_s / 1000  # milliseconds
                case b"\xfc":
                    # expiry time in ms
                    logger.info("parsing expiry time (ms)")
                    expiry_ms, remain = RdbParser._parse_expiry_ms(remain)
                    logger.info("parsing key value pair")
                    key, value, remain = RdbParser._parse_key_value(remain)
                    kvstore[key] = value
                    expirystore[key] = expiry_ms
                case b"\xfb":
                    # resizedb field
                    logger.info("parsing resizedb")
                    db_size, expiry_db_size, remain = RdbParser._parse_resizedb(remain)
                    logger.info(f"db_size: {db_size}, expiry_db_size: {expiry_db_size}")
                case b"\xfa":
                    # auxiliary fields
                    logger.info("parsing auxiliary fields")
                    aux_kv, remain = RdbParser._parse_aux(remain)
                    aux_data.update(aux_kv)
                    logger.info(aux_data)
                case _:
                    logger.info("parsing key value pair")
                    key, value, remain = RdbParser._parse_key_value(remain)
                    kvstore[key] = value
        # todo:
        # assert compute_crc64(data) == remain
        return kvstore, expirystore

    def _parse_magic(data: bytes) -> tuple[bool, bytes]:
        is_correct_magic = data[:5] == b"REDIS"
        return (is_correct_magic, data[5:])

    def _parse_version(data: bytes) -> tuple[int, bytes]:
        version = int(data[:4])
        return (version, data[4:])

    def _parse_expiry_s(data: bytes) -> tuple[int, bytes]:
        # skip the opcode
        remain = data[1:]
        expiry_ms = int.from_bytes(remain[:4], byteorder=sys.byteorder)
        return expiry_ms, remain[4:]

    def _parse_expiry_ms(data: bytes) -> tuple[int, bytes]:
        # skip the opcode
        remain = data[1:]
        expiry_ms = int.from_bytes(remain[:8], byteorder=sys.byteorder)
        return expiry_ms, remain[8:]

    def _parse_key_value(data: bytes) -> tuple[bytes, Any, bytes]:
        value_type = RdbValueType(int.from_bytes(data[:1], byteorder=sys.byteorder))
        remain = data[1:]
        key, remain = RdbParser._parse_str(remain)
        match value_type:
            case RdbValueType.STRING:
                value, remain = RdbParser._parse_str(remain)
                return key, value, remain
            case _:
                logger.info("not implemented yet!")
                raise Exception

    def _rdb_int_str_to_int(
        length: int, str_type: RdbStringType, data: bytes
    ) -> tuple[int, bytes]:
        match str_type:
            case RdbStringType.LEN_PREFIX_STR:
                str_data = int.from_bytes(data[:length], byteorder=sys.byteorder)
                logger.info(f"len prefix str: {str_data!r}")
                remain = data[length:]
                return (str_data, remain)
            case RdbStringType.INT_8_STR:
                str_data = int.from_bytes(data[:1], byteorder=sys.byteorder)
                logger.info(f"int8 str: {str_data}")
                remain = data[1:]
                return (str_data, remain)
            case RdbStringType.INT_16_STR:
                str_data = int.from_bytes(data[:2], byteorder=sys.byteorder)
                remain = data[2:]
                logger.info(f"int16 str: {str_data}")
                return (str_data, remain)
            case RdbStringType.INT_32_STR:
                str_data = int.from_bytes(data[:4], byteorder=sys.byteorder)
                remain = data[4:]
                logger.info(f"int32 str: {str_data}")
                return (str_data, remain)
            case _:
                logger.info("invalid int rdb str")
                raise Exception

    def _parse_resizedb(data: bytes) -> tuple[int, int, bytes]:
        # skip the opcode
        remain = data[1:]
        db_size, _, remain = RdbParser._parse_length(remain)
        expiry_db_size, _, remain = RdbParser._parse_length(remain)
        return (db_size, expiry_db_size, remain)

    def _parse_db_selector(data: bytes) -> tuple[int, bytes]:
        # skip the opcode
        data = data[1:]
        length, _, remain = RdbParser._parse_length(data)
        return (length, remain)

    def _parse_aux(data: bytes) -> tuple[dict[str, str], bytes]:
        # skip the opcode
        data = data[1:]
        key, remain = RdbParser._parse_str(data)
        value, remain = RdbParser._parse_str(remain)
        return ({key: value}, remain)

    def _parse_str(data: bytes) -> tuple[bytes | int, bytes]:
        length, str_type, remain = RdbParser._parse_length(data)
        match str_type:
            case RdbStringType.LEN_PREFIX_STR:
                str_data = remain[:length]
                logger.info(f"len prefix str: {str_data!r}")
                remain = remain[length:]
                return (str_data, remain)
            case (
                RdbStringType.INT_8_STR
                | RdbStringType.INT_16_STR
                | RdbStringType.INT_32_STR
            ):
                return RdbParser._rdb_int_str_to_int(length, str_type, remain)
            case RdbStringType.COMPRESSED_STR:
                logger.info("not implemented yet!")
                raise Exception

    def _parse_length(data: bytes) -> tuple[int, RdbStringType, bytes]:
        first_byte = int.from_bytes(data[:1], byteorder=sys.byteorder)
        remain = data[1:]
        msb = first_byte >> 6
        match msb:
            case 0b00:
                # next six bits represent the length
                length = first_byte & 0b00111111
                return (length, RdbStringType.LEN_PREFIX_STR, remain)
            case 0b01:
                # read one additional byte. the combined 14 bits represents the length
                next_byte = int.from_bytes(data[:1], byteorder=sys.byteorder)
                remain = remain[1:]
                length = ((first_byte & 0b00111111) << 8) | next_byte
                return (length, RdbStringType.LEN_PREFIX_STR, remain)
            case 0b10:
                # discard the remaining 6 bits. the next 4 bytes represents the length
                length = int.from_bytes(data[:4], byteorder=sys.byteorder)
                remain = remain[4:]
                return (length, RdbStringType.LEN_PREFIX_STR, remain)
            case 0b11:
                # the next object is encoded in a special format
                # the remaining 6 bits indicate the format
                format = first_byte & 0b00111111
                match format:
                    case 0:
                        str_type = RdbStringType.INT_8_STR
                    case 1:
                        str_type = RdbStringType.INT_16_STR
                    case 2:
                        str_type = RdbStringType.INT_32_STR
                    case 3:
                        str_type = RdbStringType.COMPRESSED_STR
                return (0, str_type, remain)
