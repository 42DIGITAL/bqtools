import decimal
import math
import datetime
import logging
import json

import dateutil

NoneType = type(None)

def convert(column, field_type='STRING', mode='NULLABLE', infer_required=False):
    field_type = field_type.upper()
    mode = mode.upper()

    if field_type == 'INTEGER':
        converted_column = [
            to_integer(value, mode, infer_required) for value in column
        ]
    elif field_type == 'STRING':
        converted_column = [
            to_string(value, mode, infer_required) for value in column
        ]
    elif field_type == 'NUMERIC':
        converted_column = [
            to_numeric(value, mode, infer_required) for value in column
        ]
    elif field_type == 'FLOAT':
        converted_column = [
            to_float(value, mode, infer_required) for value in column
        ]
    elif field_type == 'BOOLEAN':
        converted_column = [
            to_boolean(value, mode, infer_required) for value in column
        ]
    elif field_type == 'BYTES':
        converted_column = [
            to_bytes(value, mode, infer_required) for value in column
        ]
    elif field_type == 'DATETIME':
        converted_column = [
            to_datetime(value, mode, infer_required) for value in column
        ]
    elif field_type == 'DATE':
        converted_column = [
            to_date(value, mode, infer_required) for value in column
        ]
    elif field_type == 'TIME':
        converted_column = [
            to_time(value, mode, infer_required) for value in column
        ]
    elif field_type == 'TIMESTAMP':
        converted_column = [
            to_timestamp(value, mode, infer_required) for value in column
        ]
    elif field_type in ['STRUCT', 'ARRAY', 'GEOGRAPHY']:
        raise NotImplementedError('Types STRUCT, ARRAY and GEOGRAPHY are not yet implemented.')
    else:
        raise ValueError('{} not a valid field_type.'.format(field_type))
    return converted_column

def to_integer(value, mode='NULLABLE', infer_required=False):
    def handle_none():
        if mode == 'REQUIRED':
            if infer_required:
                return 0
            else:
                raise ValueError('None is not allowed.')
        else:
            return None

    if isinstance(value, NoneType):
        int_value = handle_none()
    elif isinstance(value, float) and math.isnan(value):
        int_value = handle_none()
    elif isinstance(value, bytes):
        int_value = value.from_bytes(value, byteorder='big', signed=False)
    elif isinstance(value, str):
        if value.isnumeric():
            int_value = int(value)
        elif value == 'False':
            int_value = 0
        elif value in ['', 'None', 'nan']:
            int_value = handle_none()
        else:
            f = float(value)
            if f.is_integer():
                int_value = int(f)
            else:
                if math.isnan(f):
                    int_value = handle_none()
                else:
                    logging.warning('Converting Float to Int with loss.')
                    int_value = int(f)
    else:
        int_value = int(value)
    
    return int_value

def to_float(value, mode='NULLABLE', infer_required=False):
    def handle_none():
        if mode == 'REQUIRED':
            if infer_required:
                return 0.0
            else:
                raise ValueError('None is not allowed.')
        else:
            return float('nan')
    
    if isinstance(value, NoneType):
        float_value = handle_none()
    elif isinstance(value, str):
        if value == 'False':
            float_value = 0.0
        elif value in ['', 'None', 'nan']:
            float_value = handle_none()
        else:
            float_value = float(value)
    else:
        float_value = float(value)

    return float_value

def to_string(value, mode='NULLABLE', infer_required=False):
    def handle_none():
        if mode == 'REQUIRED':
            if infer_required:
                return ''
            else:
                raise ValueError('None is not allowed.')
        else:
            return None
    
    if isinstance(value, NoneType):
        str_value = handle_none()
    elif isinstance(value, float) and math.isnan(value):
        str_value = handle_none()
    elif isinstance(value, bytes):
        str_value = value.decode(encoding='utf8', errors='strict')
    elif isinstance(value, str):
        if value in ['', 'None', 'nan']:
            str_value = handle_none()
        else:
            return value.replace('\n', ' ')
    else:
        str_value = str(value)

    return str_value

def to_numeric(value, mode='NULLABLE', infer_required=False):
    def handle_none():
        if mode == 'REQUIRED':
            if infer_required:
                return decimal.Decimal('0.0')
            else:
                raise ValueError('None is not allowed.')
        else:
            return None

    if isinstance(value, NoneType):
        numeric_value = handle_none()
    elif isinstance(value, float) and math.isnan(value):
        numeric_value = handle_none()
    elif isinstance(value, str):
        if value == '':
            numeric_value = decimal.Decimal('0.0')
        elif value in ['', 'None', 'nan']:
            numeric_value = handle_none()
        else:
            numeric_value = decimal.Decimal(value)
    else:
        numeric_value = decimal.Decimal(value)

    return numeric_value

def to_boolean(value, mode='NULLABLE', infer_required=False):
    def handle_none():
        if mode == 'REQUIRED':
            if infer_required:
                return False
            else:
                raise ValueError('None is not allowed.')
        else:
            return None
    
    if isinstance(value, NoneType):
        bool_value = handle_none()
    elif isinstance(value, float) and math.isnan(value):
        bool_value = handle_none()
    elif isinstance(value, str):
        if value in ['', '0', '0.0', 'False']:
            bool_value = False
        elif value in ['None', 'nan']:
            bool_value = handle_none()
        else:
            bool_value = True
    else:
        bool_value = bool(value)

    return bool_value

def to_bytes(value, mode='NULLABLE', infer_required=False):
    def handle_none():
        if mode == 'REQUIRED':
            if infer_required:
                return b''
            else:
                raise ValueError('None is not allowed.')
        else:
            return None

    if isinstance(value, NoneType):
        bytes_value = handle_none()
    elif isinstance(value, float) and math.isnan(value):
        bytes_value = handle_none()
    elif isinstance(value, str):
        if value == 'False':
            bytes_value = b''
        elif value in ['', 'None', 'nan']:
            bytes_value = handle_none()
        else:
            bytes_value = value.encode(encoding='utf8', errors='strict')
    elif isinstance(value, int):
        bytes_value = value.to_bytes(length=4, byteorder='big', signed=False)
    else:
        bytes_value = bytes(value)

    return bytes_value

def to_date(value, mode='NULLABLE', infer_required=False):
    if isinstance(value, datetime.datetime):
        return value.date()
    elif isinstance(value, datetime.date):
        return value
    else:
        dt_value = to_datetime(value, mode=mode, infer_required=infer_required)
        return dt_value.date()

def to_datetime(value, mode='NULLABLE', infer_required=False):
    def handle_none():
        if mode == 'REQUIRED':
            if infer_required:
                return datetime.datetime.min
            else:
                raise ValueError('None is not allowed.')
        else:
            return None

    if isinstance(value, NoneType):
        dt_value = handle_none()
    elif isinstance(value, float) and math.isnan(value):
        dt_value = handle_none()
    elif isinstance(value, str):
        if value in ['', 'None', 'nan', 'False']:
            dt_value = handle_none()
        else:
            dt_value = dateutil.parser.parse(value)
    elif isinstance(value, (int, float)):
        dt_value = datetime.datetime.fromtimestamp(value)
    elif isinstance(value, (tuple, list)):
        dt_value = datetime.datetime(*value)
    elif isinstance(value, dict):
        dt_value = datetime.datetime(**value)
    elif isinstance(value, datetime.datetime):
        dt_value = value
    else:
        raise NotImplementedError('Cannot convert {} to Datetime'.format(type(value)))

    return dt_value

def to_time(value, mode='NULLABLE', infer_required=False):
    def handle_none():
        if mode == 'REQUIRED':
            if infer_required:
                return datetime.time.min
            else:
                raise ValueError('None is not allowed.')
        else:
            return None

    if isinstance(value, NoneType):
        time_value = handle_none()
    elif isinstance(value, float) and math.isnan(value):
        time_value = handle_none()
    elif isinstance(value, str):
        if value in ['', 'None', 'nan', 'False']:
            time_value = handle_none()
        else:
            time_value = to_datetime(value, mode=mode, infer_required=infer_required).time
    elif isinstance(value, (tuple, list)):
        time_value = datetime.time(*value)
    else:
        time_value = to_datetime(value, mode=mode, infer_required=infer_required).time

    return time_value

def to_timestamp(value, mode='NULLABLE', infer_required=False):
    if isinstance(value, (int, float)) and not math.isnan(value):
        ts_value = float(value)
    else:
        ts_value = to_datetime(value, mode=mode, infer_required=infer_required).timestamp()
    return ts_value

def to_struct(value, mode='NULLABLE', infer_required=False):
    raise NotImplementedError('Conversion to STRUCT is not implemented yet.')

def to_array(value, mode='NULLABLE', infer_required=False):
    raise NotImplementedError('Conversion to ARRAY is not implemented yet.')

def to_geograpy(value, mode='NULLABLE', infer_required=False):
    raise NotImplementedError('Conversion to GEOGRAPHY is not implemented yet.')

