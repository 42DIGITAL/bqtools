# Python Micro-ORM for BigQuery

### Why?
While [BigQuery](https://cloud.google.com/bigquery/) is not a database suited for transactional operations, data wrangling and regularly updating datasets can make ORM-style features very useful. 

### What?
`bqorm` provides a light-weight solution to explicit schema management with python-native types (unlike pandas dtype) and 
some convenient type checking, inference and conversions. Table-objects created by `bqorm` can be read from BigQuery, stored locally, read from a local file and written to BigQuery. Table schemas can be changed and data can be added or modified.

## Examples:
### Create basic tables
```python
import bqorm

schema = [
    {'name': 'number', 'field_type': 'INTEGER'},
    {'name': 'text', 'field_type': 'STRING'},
]
# valid BigQuery types see: 
# https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
# geo, struct and array are currently not/not fully supported

# data = columns of lists
table = bqorm.BQTable(
    schema=schema, 
    columns=[[1, 2, 3, 4], ['a', 'b', 'c', 'd']]
)

# data = rows of lists
table = bqorm.BQTable(
    schema=schema, 
    rows=[[1, 'a'], [2, 'b'], [3, 'c'], [4, 'd']]
)

# data = rows of dicts
table = bqorm.BQTable(
    schema=schema, 
    rows=[
        {'number': 1, 'text': 'a'}, 
        {'number': 2, 'text': 'b'},
        ...
    ]
)
```

### View data
```python
print(table.get_data_columns())  # list of all columns
print(table.get_data_rows(n=10)) # list of first n rows

# convert to pandas.DataFrame
df = table.to_df()               
# warning: pandas dtypes may be inconsistent 
# with BigQuery Schema field_types
```

### Append data
```python
row = {'number': 5, 'text': 'e'}
table.append_row(row)

row = [6, 'f']
table.append_row(row)
```

### Load table from BQ
```python
# requires environment variable GOOGLE_APPLICATION_CREDENTIALS 
# or parameter credentials='path-to-credentials.json'
table = bqorm.BQTable(
    table_ref='project_id.dataset_id.table_id',
    query_data=True,   # default, set to False to prevent setting data
    query_schema=True, # default, set to False to prevent setting schema
    limit=10           # default, set to None to request full table
)
```

### Modify table schema
```python
# change column order and field_type
new_schema = [
    {'name': 'text', 'field_type': 'STRING'},
    {'name': 'number', 'field_type': 'FLOAT'},
]
table.set_schema(new_schema)

# change column names
table.rename_schema_fields({'number': 'decimal'})
```

### Write table to BQ
```python
table.set_table_ref(
    table_ref='project_id.dataset_id.new_table_id', 
    upload_data=True, 
    upload_mode='append'
)
```

### Persist tables locally
```python
# write to local file (compressed binary format)
table.to_file('local_table.bqt')

# load from local file
table = bqorm.BQTable(load_file='local_table.bqt')
```
