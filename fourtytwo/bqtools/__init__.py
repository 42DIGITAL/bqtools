import csv
import os
import logging
import gzip
import pickle
import random

import pandas as pd
from google.cloud import bigquery

from fourtytwo.bqtools import conversions

DEBUG = False
if DEBUG:
    logging.basicConfig(level=logging.DEBUG)


def load(filename):
    if DEBUG:
        logging.debug('bqtools.load({})'.format(filename))

    with gzip.open(filename, 'rb') as f:
        table_data = pickle.load(f)
    for key, value in table_data.items():
        logging.debug(key)

    table = BQTable(schema=table_data['schema'], data=table_data['data'])
    return table

def read_bq(table_ref, credentials=None, limit=10, schema_only=False, columns=None):
    if DEBUG:
        logging.debug('bqtools.read_bq({})'.format(table_ref))
    
    table = BQTable()

    if credentials:
        client = bigquery.Client.from_service_account_json(credentials)
    else:
        client = bigquery.Client()
    
    if isinstance(table_ref, bigquery.TableReference):
        table_ref = '{}.{}.{}'.format(
            table_ref.project, table_ref.dataset_id, table_ref.table_id)
    
    schema = client.get_table(bigquery.Table(table_ref=table_ref)).schema
    table.schema = schema

    if not schema_only:
        selector = ','.join(columns) if columns else '*'
        query = 'select {} from `{}`'.format(selector, table_ref)
        if limit:
            query += ' limit {}'.format(limit)
        job = client.query(query)
        row_iterator = job.result()
        rows = [row.values() for row in row_iterator]
        columns = _rows_to_columns(rows=rows, schema=schema)
        table.data = columns
    return table

def _rows_to_columns(rows, schema):
    if DEBUG:
        logging.debug('bqtools._rows_to_columns()')

    schema_len = len(schema)
    columns = [[] for n in range(schema_len)]
    for row in rows:
        if isinstance(row, dict):
            row = [row.get(field.name) for field in schema]
        
        row_len = len(row)
        if row_len > schema_len:
            row = row[:schema_len]
        elif row_len < schema_len:
            row += [None] * (schema_len - row_len)

        for index, value in enumerate(row):
            columns[index].append(value)
    return columns

def _columns_to_rows(columns, schema, n=None, row_type='list'):
    if DEBUG:
        logging.debug('bqtools._columns_to_rows()')
    
    if not columns:
        return []
    rows = []
    max_col_len = max([len(c) for c in columns])
    if n:
        max_col_len = min(n, max_col_len)

    if row_type == 'list':
        for index in range(max_col_len):
            row = [c[index] for c in columns]
            rows.append(row)
    elif row_type == 'dict':
        for index in range(max_col_len):
            row = {s.name: columns[n][index] for n, s in enumerate(schema)}
            rows.append(row)
    return rows



class BQTable(object):
    def __init__(self, schema=None, data=None):
        if DEBUG:
            logging.debug('bqtools.BQTable.__init__')

        self.schema = schema if schema else []
        self.data = data if data else []
    
    #def __repr__(self):
    #    pass

    def __eq__(self, other):
        return self.schema == other.schema and self.data == other.data
    
    def __setattr__(self, name, value):
        if DEBUG:
            logging.debug('bqtools.BQTable.set {}'.format(name))

        if name == 'schema':
            self._set_schema(value)
        elif name == 'data':
            self._set_data(value)

    def __getattr__(self, name):
        if DEBUG:
            logging.debug('bqtools.BQTable.get {}'.format(name))
        
        if name == 'schema':
            return self._schema
        elif name == 'data':
            return self._data

    def _set_schema(self, schema):
        if DEBUG:
            logging.debug('bqtools.BQTable._set_schema()')

        new_schema = []
        for field in schema:
            if isinstance(field, bigquery.SchemaField):
                new_schema.append(field)
            elif isinstance(field, (tuple, list)):
                new_schema.append(bigquery.SchemaField(*field))
            elif isinstance(field, dict):
                new_schema.append(bigquery.SchemaField(**field))
        
        if self.schema and new_schema and new_schema != self.schema:
            data = self._move_columns(new_schema)
        else:
            data = self.data

        data = self._typecheck(schema=new_schema, data=data)
        object.__setattr__(self, '_schema', new_schema)
        object.__setattr__(self, '_data', data)

    def _set_data(self, data):
        if DEBUG:
            logging.debug('bqtools.BQTable._set_data()')
        
        if data and isinstance(data, list):
            if isinstance(data[0], dict):
                data = _rows_to_columns(data, self.schema)
        data = self._typecheck(data=data)
        object.__setattr__(self, '_data', data)
    
    def _move_columns(self, schema):
        if DEBUG:
            logging.debug('bqtools.BQTable._move_columns()')

        old_field_names = [field.name for field in self.schema]
        new_field_names = [field.name for field in schema]
        column_order = [old_field_names.index(name) for name in new_field_names]
        return [self.data[index] for index in column_order]

    def _rename_columns(self, mapping):
        if DEBUG:
            logging.debug('bqtools.BQTable._rename_columns()')

        new_schema = [field for field in self.schema]
        old_field_names = [field.name for field in self.schema]
        for old_field_name, new_field_name in mapping.items():
            index = old_field_names.index(old_field_name)
            field = new_schema[index]
            new_schema[index] = bigquery.SchemaField(
                name=new_field_name,
                field_type=field.field_type,
                mode=field.mode,
                description=field.description,
                fields=field.fields
            )
        self.schema = new_schema

    def _typecheck(self, schema=None, data=None):
        if DEBUG:
            logging.debug('bqtools.BQTable._typecheck()')
        
        schema = schema if schema else self.schema
        data = data if data else self.data
        
        if schema and data:
            typechecked_columns = []
            for index, field in enumerate(schema):
                typechecked_columns.append(
                    conversions.convert(
                        data[index], 
                        field.field_type,
                        field.mode
                    )
                )
            return typechecked_columns
        else:
            return data

    def rename(self, columns):
        if DEBUG:
            logging.debug('bqtools.BQTable.rename()')

        self._rename_columns(mapping=columns)
    
    def append(self, rows):
        append_columns = _rows_to_columns(rows, self.schema)
        data = self.data
        for index in range(len(data)):
            data[index] += append_columns[index]
        self.data = data

    def rows(self, n=None, row_type='list'):
        if DEBUG:
            logging.debug('bqtools.BQTable.rows()')

        rows = _columns_to_rows(
            columns=self.data, 
            schema=self.schema, 
            n=n, 
            row_type=row_type
        )
        return rows
    
    def save(self, filename):
        if DEBUG:
            logging.debug('bqtools.BQTable.save()')
        
        schema_dicts = [
            {
                'name': field.name,
                'field_type': field.field_type,
                'mode': field.mode,
                'description': field.description,
                'fields': field.fields,
            } for field in self.schema
        ]
        
        table_data = {
            'schema': schema_dicts,
            'data': self.data,
        }
        
        with gzip.open(filename, 'wb') as f:
            pickle.dump(table_data, f, pickle.HIGHEST_PROTOCOL)
    
    def to_df(self):
        if DEBUG:
            logging.debug('bqtools.BQTable.to_df()')
        
        data = {field.name: self.data[index] for index, field in enumerate(self.schema)}
        return pd.DataFrame(data)

    def to_bq(self, table_ref, credentials=None, mode='append'):
        if DEBUG:
            logging.debug('bqtools.BQTable.to_bq({})'.format(table_ref))

        if credentials:
            client = bigquery.Client.from_service_account_json(credentials)
        else:
            client = bigquery.Client()
        
        if isinstance(table_ref, str):
            table_ref = bigquery.TableReference.from_string(table_ref)

        tmpfile = 'tmpfile_{}.csv'.format(random.randint(1000,9999))
        self.to_csv(tmpfile, delimiter=',')

        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = False
        job_config.create_disposition = 'CREATE_IF_NEEDED'
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.write_disposition = 'WRITE_TRUNCATE' if mode =='overwrite' else 'WRITE_APPEND'
        job_config.schema = self.schema

        with open(tmpfile, 'rb') as csv_file:
            load_job = client.load_table_from_file(
                csv_file,
                table_ref,
                job_config=job_config,
                job_id_prefix='load_table_from_file'
            )
            load_job.result()
        os.remove(tmpfile)

    def to_csv(self, filename, delimiter=','):
        if DEBUG:
            logging.debug('bqtools.BQTable.to_csv({})'.format(filename))

        with open(filename, 'w', newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter=delimiter)
            writer.writerows(self.rows())

