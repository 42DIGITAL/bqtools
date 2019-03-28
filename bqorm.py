import datetime
import csv
import os
import warnings
import gzip
import pickle

import pandas as pd
import pyarrow
import pyarrow.parquet
from google.cloud import bigquery

import bqconvert


class BQTable(object):

    def __init__(self, load_file=None, table_ref=None, schema=None, rows=None, columns=None, 
            credentials=None, query_schema=True, query_data=True, limit=10, convert_none=False):
        print('init')
        if load_file:
            with gzip.open(load_file, 'rb') as f:
                table = pickle.load(f)
            self._table_ref = table.get('table_ref')
            schema = table.get('schema')
            self._schema = None
            self._data_columns = table.get('data_columns')
            credentials = table.get('credentials')
            self._credentials = None
            self._client = None
        else:
            self._table_ref = None # project_id, dataset_id, table_id
            self._schema = None # list of fields
            self._data_columns = None # list of columns
            self._credentials = None
            self._client = None

        if credentials:
            self._credentials = credentials
            self._client = bigquery.Client.from_service_account_json(credentials)        

        if table_ref:
            self.set_table_ref(table_ref=table_ref, query_schema=query_schema, query_data=query_data, limit=limit)

        if schema:
            self.set_schema(schema=schema)
        
        if rows:
            self.set_data(rows=rows)
        elif columns:
            self.set_data(columns=columns)
    
    def set_table_ref(self, table_ref, upload_data=False, upload_mode='append', query_schema=True, query_data=True, limit=10, convert_none=False):
        print('set_table_ref')
        if not self._client:
            self._client = bigquery.Client()
        
        self._project_id, self._dataset_id, self._table_id = table_ref.split('.')
        self._table_ref = table_ref

        if upload_data:
            schema = self.get_schema()
            columns = self.get_data_columns()

            if schema and columns[0]:

                csv_file = 'tmpfile.csv'
                with open(csv_file, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile, delimiter=',')
                    writer.writerows(self.get_data_rows())
                
                job_id_prefix = 'load_table_from_file'
                job_config = bigquery.LoadJobConfig()
                job_config.autodetect = False
                job_config.create_disposition = 'CREATE_IF_NEEDED'
                job_config.source_format = bigquery.SourceFormat.CSV
                job_config.write_disposition = 'WRITE_TRUNCATE' if upload_mode == 'overwrite' else 'WRITE_APPEND'
                job_config.schema = schema

                with open(csv_file, 'rb') as source_file:
                    load_job = self._client.load_table_from_file(
                        source_file, 
                        table_ref, 
                        job_config=job_config, 
                        job_id_prefix=job_id_prefix
                    )
                try:
                    load_job.result()
                except Exception as e:
                    print('Upload Failed:', e)
                    return load_job
                os.remove(csv_file)

        else:
            if query_schema:
                table = self.get_table()
                self.set_schema(table.schema)
            
            if query_data:
                query = 'select * from `{}`'.format(table_ref)
                if limit:
                    query += ' limit {}'.format(limit)
                job = self._client.query(query)
                row_iterator = job.result()
                rows = [row.values() for row in row_iterator]

                mode = 'append' if self.get_data_columns() else 'overwrite'
                self.set_data(rows=rows, mode=mode)
            else:
                columns = [[] for n in range(len(self.get_schema()))]
                self.set_data(columns=columns, convert_none=convert_none)

    def set_schema(self, schema):
        print('set_schema')
        schema_new = []
        for field in schema:
            if isinstance(field, bigquery.SchemaField):
                schema_new.append(field)
            else:
                if field['field_type'].upper() in bqconvert.bq_field_types.keys():
                    schema_new.append(
                        bigquery.SchemaField(
                            field['name'], 
                            field['field_type'], 
                            field.get('mode', 'NULLABLE'), 
                            field.get('description', None),
                            field.get('fields', ())
                        )
                    )
                else:
                    raise ValueError(
                        '{} is not a valid SchemaField.field_type. Must be in {}'.format(
                            field['field_type'], bqconvert.bq_field_types.keys())
                        )
        
        columns = self.get_data_columns()
        if columns:
            self.set_data(columns=columns, schema=schema_new)

        self._schema = schema_new
    
    def rename_schema_fields(self, mapping):
        print('rename_schema_fields')
        schema = self.get_schema()
        new_schema = [field for field in schema]
        field_names = [field.name for field in schema]
        for field_name_old, field_name_new in mapping.items():
            index = field_names.index(field_name_old)
            field = new_schema[index]
            new_schema[index] = bigquery.SchemaField(
                name=field_name_new,
                field_type=field.field_type,
                mode=field.mode,
                description=field.description,
                fields=field.fields
            )
        self._schema = new_schema

    def set_data(self, rows=None, columns=None, mode='overwrite', convert_none=False, schema=None):
        print('set_data')
        # table has schema AND set_data got schema parameter AND schemas are different
        # then first move column-order to align with new schema before typechecking
        current_schema = self.get_schema()
        if current_schema and schema and schema != current_schema:
            self._move_columns(schema=schema)
            current_schema = self.get_schema()
            columns = self.get_data_columns()
            schema = None

        # table has schema or new schema was provided
        # then proceed with typechecking
        schema = schema if schema else current_schema

        if schema:
            if rows and not columns:
                columns = self._rows_to_columns(rows=rows, schema=schema)

            if columns:
                max_len = max([len(column) for column in columns])
                for column in columns:
                    if len(column) < max_len:
                        column += [None] * (max_len - len(column))

            typechecked_columns = self._typecheck_columns(
                columns=columns, schema=schema, convert_none=convert_none
            )
            if mode == 'overwrite':
                self._data_columns = typechecked_columns
            elif mode == 'append':
                for index, typechecked_column in enumerate(typechecked_columns):
                    self._data_columns[index] += typechecked_column
            else:
                raise ValueError('mode must be "overwrite" or "append", not {}'.format(mode))
        else:
            raise ValueError('Requires BQTable.set_schema(schema) first.')
    
    def append_row(self, row, convert_none=False):
        print('append_row')
        if isinstance(row, dict):
            list_row = []
            schema_field_names = [field.name for field in self.get_schema()]
            for name in schema_field_names:
                list_row.append(row.get(name))
                # no warning if row items < schema items
        elif isinstance(row, list):
            list_row = row
        self.set_data(rows=[list_row], mode='append', convert_none=convert_none)

    def get_schema(self):
        print('get_schema')
        return self._schema
        
    def get_data_columns(self):
        print('get_data_columns')
        return self._data_columns

    def get_data_rows(self, n=None):
        print('get_data_rows')
        columns = self.get_data_columns()
        if columns:
            max_len = max([len(column) for column in columns])
            count = min(max_len, n) if n else max_len
            rows = []
            for index in range(count):
                row = [c[index] for c in columns]
                rows.append(row)
            return rows
        else:
            return [[] for n in range(len(self.get_schema()))]
    
    def get_data(self, orient='columns'):
        if orient == 'columns':
            return self.get_data_columns()
        else:
            return self.get_data_rows()

    def get_dataset_ref(self):
        print('get_dataset_ref')
        if self._table_ref:
            return bigquery.DatasetReference(project=self._project_id, dataset_id=self._dataset_id)
        else:
            raise ValueError('Requires BQTable.set_table_ref(set_table_ref) first.')
        
    def get_table_ref(self):
        print('get_table_ref')
        if self._table_ref:
            return bigquery.TableReference(dataset_ref=self.get_dataset_ref(), table_id=self._table_id)
        else:
            raise ValueError('Requires BQTable.set_table_ref(set_table_ref) first.')
    
    def get_table(self):
        print('get_table')
        if self._table_ref:
            table = bigquery.Table(table_ref=self.get_table_ref())
            return self._client.get_table(table)
        else:
            raise ValueError('Requires BQTable.set_table_ref(set_table_ref) first.')
    
    def to_df(self):
        data = dict(zip([field.name for field in self.get_schema()], self.get_data_columns()))
        return pd.DataFrame(data)

    def to_file(self, filename):
        schema_dicts = [
            {
                'name': field.name, 'field_type': field.field_type, 
                'mode': field.mode, 'description': field.description, 
                'fields': field.fields
            } for field in self.get_schema()
        ]
        table = {
            'schema': schema_dicts,
            'table_ref': self.get_table_ref(),
            'data_columns': self.get_data_columns(),
            'credentials': self._credentials,
        }
        with gzip.open(filename, 'wb') as f:
            pickle.dump(table, f, pickle.HIGHEST_PROTOCOL)
            

    def _move_columns(self, schema):
        print('_move_columns')
        field_names_old = [field.name for field in self.get_schema()]
        field_names_new = [field.name for field in schema]
        column_order = [field_names_old.index(name) for name in field_names_new]
        columns = self.get_data_columns()
        self._data_columns = [columns[index] for index in column_order]
        self._schema = schema

    def _typecheck_columns(self, columns, schema, convert_none=False):
        print('_typecheck_columns')
        typechecked_columns = []
        for index, column in enumerate(columns):
            field_type = schema[index].field_type
            typechecked_column = [
                bqconvert.bq_field_types[field_type](value, convert_none=convert_none) for value in column
            ]
            typechecked_columns.append(typechecked_column)
        return typechecked_columns

    def _rows_to_columns(self, rows, schema):
        print('_rows_to_columns')
        schema_field_count = len(schema)
        columns = [[] for n in range(schema_field_count)]
        for row_counter, row in enumerate(rows):
            if isinstance(row, dict):
                row = list(row.values())
            
            row_item_count = len(row)
            if row_item_count > schema_field_count:
                row = row[:schema_field_count]
                warnings.warn('Row {} contains more items than schema.'.format(row_counter))
            if row_item_count < schema_field_count:
                row += [None] * (schema_field_count - row_item_count)
                warnings.warn('Row {} contains less items than schema.'.format(row_counter))
            
            for value_counter, value in enumerate(row):
                columns[value_counter].append(value)
        return columns