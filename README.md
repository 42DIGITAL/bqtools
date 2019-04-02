[![Build Status](https://travis-ci.org/42DIGITAL/bqtools.svg?branch=master)](https://travis-ci.org/42DIGITAL/bqtools) [![PyPI version](https://badge.fury.io/py/bqtools.svg)](https://badge.fury.io/py/bqtools)

# Python Tools for BigQuery

### Why?
For data collection and data exploration, we like to work with [BigQuery](https://cloud.google.com/bigquery/). But we have not found a python library, to easily handle recurring tasks like adding new data (of potentially inconsistent schema) and schema migrations. So we took a couple of our solutions for those tasks and put them into this library.

### What?
`bqtools` provides a light-weight solution to explicit schema management with python-native types (unlike pandas dtype) and 
some convenient type checking, inference and conversions. Table-objects created by `bqtools` can be read from BigQuery, stored locally, read from a local file and written to BigQuery. Table schemas can be changed and data can be added or modified.

### Install
```bash
pip install --upgrade bqtools
```

## Examples:
### Create basic tables
```python
from fourtytwo import bqtools

schema = [
    {'name': 'number', 'field_type': 'INTEGER'},
    {'name': 'text', 'field_type': 'STRING'},
]
# valid BigQuery types see: 
# https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
# geo, struct and array are currently not/not fully supported

# data = columns of lists
table = bqtools.BQTable(
    schema=schema, 
    data=[[1, 2, 3, 4], ['a', 'b', 'c', 'd']]
)

# data = rows of dicts
table = bqtools.BQTable(
    schema=schema, 
    data=[
        {'number': 1, 'text': 'a'}, 
        {'number': 2, 'text': 'b'},
        ...
    ]
)
```

### View data
```python
print(table.data)       # list of all columns
print(table.rows(n=10)) # list of first n rows

# convert to pandas.DataFrame
df = table.to_df()               
# warning: pandas dtypes may be inconsistent 
# with BigQuery Schema field_types
```

### Append data
```python
rows = [{'number': 5, 'text': 'e'}]
table.append(rows)

row = [[6, 'f']]
table.append(rows)
```

### Load table from BigQquery
```python
# requires environment variable GOOGLE_APPLICATION_CREDENTIALS 
# or parameter credentials='path-to-credentials.json'
table = bqtools.read_bq(
    table_ref='project_id.dataset_id.new_table_id', 
    limit=10,           # limit query rows
    schema_only=False   # set True to only add data
)
```

### Modify table schema
```python
# change column order and field_type
new_schema = [
    {'name': 'text', 'field_type': 'STRING'},
    {'name': 'number', 'field_type': 'FLOAT'},
]
table.schema(new_schema)

# change column names
table.rename(columns={'number': 'decimal'})
```

### Write table to BigQuery
```python
# requires environment variable GOOGLE_APPLICATION_CREDENTIALS
# or parameter credentials='path-to-credentials.json'
table.to_bq(table_ref, mode='append')
```

### Persist tables locally
```python
# write to local file (compressed binary format)
table.save('local_table.bqt')

# load from local file
table = bqtools.load('local_table.bqt')
```
