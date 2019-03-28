import decimal
import math
import datetime
import warnings
import json

import dateutil
import pyarrow

NoneType = type(None)

def value_to_integer(value, convert_none=False, byteorder='big', signed=False):
    if isinstance(value, bytes):
        return value.from_bytes(value, byteorder=byteorder, signed=signed)
    elif isinstance(value, NoneType):
        if convert_none:
            return 0
        else:
            return None
    elif isinstance(value, float) and math.isnan(value):
        if convert_none:
            return 0
        else:
            return None
    elif isinstance(value, str):
        if value.isnumeric():
            return int(value)
        else:
            f = float(value)
            if f.is_integer():
                return int(f)
            else:
                if math.isnan(f):
                    if convert_none:
                        return 0
                    else:
                        return None
                else:
                    return int(float(value))
    return int(value)

def value_to_float(value, convert_none=False):
    if isinstance(value, NoneType):
        if convert_none:
            return 0.0
        else:
            return float('nan')
    return float(value)

def value_to_numeric(value, convert_none=False):
    if isinstance(value, NoneType) or (isinstance(value, float) and math.isnan(value)):
        if convert_none:
            return decimal.Decimal('0.0')
        else:
            return None
    return decimal.Decimal(value)

def value_to_boolean(value, convert_none=False):
    if isinstance(value, NoneType) or (isinstance(value, float) and math.isnan(value)):
        if convert_none:
            return False
        else:
            return None
    elif isinstance(value, str):
        if value in ['', '0', '0.0', 'False']:
            return False
        elif value in ['None', 'nan']:
            return None
        else:
            return True

    return bool(value)

def value_to_string(value, convert_none=False, encoding='uft8', errors='strict'):
    if isinstance(value, bytes):
        return value.decode(encoding=encoding, errors=errors)
    elif isinstance(value, NoneType) or (isinstance(value, float) and math.isnan(value)):
        if convert_none:
            return ''
        else:
            return None
    if isinstance(value, str):
        if value in ['None', 'nan']:
            if convert_none:
                return ''
            else:
                return None
        else:
            return value.replace('\n', ' ')
    return str(value)

def value_to_bytes(value, convert_none=False, encoding='utf8', errors='strict', length=4, byteorder='big', signed=False):
    if isinstance(value, str):
        return value.encode(encoding=encoding, errors=errors)
    elif isinstance(value, int):
        return value.to_bytes(length=length, byteorder=byteorder, signed=signed)
    elif isinstance(value, NoneType) or (isinstance(value, float) and math.isnan(value)):
        if convert_none:
            return b''
        else:
            return None
    return bytes(value)

def value_to_date(value, convert_none=False):
    if isinstance(value, NoneType) or (isinstance(value, float) and math.isnan(value)):
        return None
    dt = value_to_datetime(value, convert_none=convert_none)
    return dt.date

def value_to_datetime(value, convert_none=False):
    if isinstance(value, str):
        dt = dateutil.parser.parse(value)
    elif isinstance(value, int):
        dt = datetime.datetime.fromtimestamp()
    elif isinstance(value, tuple):
        dt = datetime.datetime(*value)
    elif isinstance(value, dict):
        dt = datetime.datetime(**value)
    elif isinstance(value, NoneType) or (isinstance(value, float) and math.isnan(value)):
        return None
    return dt

def value_to_time(value, convert_none=False):
    if isinstance(value, NoneType) or (isinstance(value, float) and math.isnan(value)):
        return None
    return datetime.time(value)

def value_to_timestamp(value, convert_none=False):
    if isinstance(value, NoneType) or (isinstance(value, float) and math.isnan(value)):
        return None
    elif isinstance(value, int):
        return float(value)
    elif isinstance(value, float):
        return value
    elif isinstance(value, (datetime.datetime, datetime.date)):
        return value.timestamp()
    elif isinstance(value, str):
        return value_to_datetime(value).timestamp()
    
    warnings.warn('Dont know how to convert {} of type {} to TIMESTAMP.'.format(value, type(value)))
    return None

def value_to_struct(value, convert_none=False):
    if isinstance(value, NoneType) or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value

def value_to_geography(value, convert_none=False):
    if isinstance(value, NoneType) or (isinstance(value, float) and math.isnan(value)):
        return None
    return value

bq_field_types = {
    'INTEGER': value_to_integer,
    'FLOAT': value_to_float,
    'NUMERIC': value_to_numeric,
    'BOOLEAN': value_to_boolean,
    'STRING': value_to_string,
    'BYTES': value_to_bytes,
    'DATE': value_to_date,
    'DATETIME': value_to_datetime,
    'TIME': value_to_time,
    'TIMESTAMP': value_to_timestamp,
    'STRUCT': value_to_struct,
    'RECORD': value_to_struct,
    'GEOGRAPHY': value_to_geography
}

# pq_field_types = {
#     'INTEGER': pyarrow.int64(),
#     'FLOAT': pyarrow.float64(),
#     'NUMERIC': pyarrow.decimal128(precision=9),
#     'BOOLEAN': pyarrow.bool_(),
#     'STRING': pyarrow.string(),
#     'BYTES': pyarrow.binary(),
#     'DATE': pyarrow.date64(),
#     'DATETIME': pyarrow.date64(),
#     'TIME': pyarrow.time64('ns'),
#     'TIMESTAMP': pyarrow.timestamp('ns'),
#     'STRUCT': pyarrow.struct({}),
#     'RECORD': pyarrow.struct({}),
#     'GEOGRAPHY': pyarrow.string(),
# }