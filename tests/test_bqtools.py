from fourtytwo import bqtools
from google.cloud import bigquery

def test_bqtools_construct_columns():
    schema = [
        {'name': 'number', 'field_type': 'INTEGER'},
        {'name': 'text', 'field_type': 'STRING'},
        {'name': 'struct1', 'field_type': 'RECORD', 'mode': 'NULLABLE', 'fields':[
            {'name': 'int_field', 'field_type': 'INTEGER'},
            {'name': 'str_field', 'field_type': 'STRING'}]},
        {'name': 'struct2', 'field_type': 'RECORD', 'mode': 'REPEATED', 'fields':[
            {'name': 'int_field', 'field_type': 'INTEGER'},
            {'name': 'str_field', 'field_type': 'STRING'}]}
    ]
    columns = [
        [1, 2, 3, 4], 
        ['a', 'b', 'c', 'd'], 
        [{'int_field':1, 'str_field':'a'}, {'int_field':2, 'str_field':'b'}, {'int_field':3, 'str_field':'c'}, {'int_field':4, 'str_field':'d'}],
        [[{'int_field':1, 'str_field':'a'}], [{'int_field':2, 'str_field':'b'}], [{'int_field':3, 'str_field':'c'}], [{'int_field':4, 'str_field':'d'}]]
    ]
    table = bqtools.BQTable(schema=schema, data=columns)
    
    assert len(table.schema) == 4
    assert len(table.data) == 4
    assert len(table.rows()) == 4

def test_bqtools_construct_rows_dicts():
    schema = [
        {'name': 'number', 'field_type': 'INTEGER'},
        {'name': 'text', 'field_type': 'STRING'},
        {'name': 'struct1', 'field_type': 'RECORD', 'mode': 'NULLABLE', 'fields':[
            {'name': 'int_field', 'field_type': 'INTEGER'},
            {'name': 'str_field', 'field_type': 'STRING'}]},
        {'name': 'struct2', 'field_type': 'RECORD', 'mode': 'REPEATED', 'fields':[
            {'name': 'int_field', 'field_type': 'INTEGER'},
            {'name': 'str_field', 'field_type': 'STRING'}]}
    ]
    rows = [
        {'number': 1, 'text': 'a', 'struct1':{'int_field':1, 'str_field':'a'}, 'struct2':[{'int_field':1, 'str_field':'a'}]},
        {'number': 2, 'text': 'b', 'struct1':{'int_field':2, 'str_field':'b'}, 'struct2':[{'int_field':2, 'str_field':'b'}]},
        {'number': 3, 'text': 'c', 'struct1':{'int_field':3, 'str_field':'c'}, 'struct2':[{'int_field':3, 'str_field':'c'}]},
        {'number': 4, 'text': 'd', 'struct1':{'int_field':4, 'str_field':'d'}, 'struct2':[{'int_field':4, 'str_field':'d'}]},
    ]
    table = bqtools.BQTable(schema=schema, data=rows)

    assert len(table.schema) == 4
    assert len(table.data) == 4
    assert len(table.rows()) == 4

def test_bqtools_append_list():
    schema = [
        {'name': 'number', 'field_type': 'INTEGER'},
        {'name': 'text', 'field_type': 'STRING'},
    ]
    table = bqtools.BQTable(schema=schema)
    table.append([[1, 'a'], [2, 'b']])
    assert len(table.data) == 2
    assert len(table.rows()) == 2

def test_bqtools_append_dict():
    schema = [
        {'name': 'number', 'field_type': 'INTEGER'},
        {'name': 'text', 'field_type': 'STRING'},
    ]
    table = bqtools.BQTable(schema=schema)
    table.append([
        {'number': 1, 'text': 'a'},
        {'number': 2, 'text': 'b'}
    ])
    assert len(table.data) == 2
    assert len(table.rows()) == 2

def test_bqtools_set_new_schema():
    schema = [
        {'name': 'number', 'field_type': 'INTEGER'},
        {'name': 'text', 'field_type': 'STRING'},
    ]
    rows = [
        {'number': 1, 'text': 'a'},
        {'number': 2, 'text': 'b'},
        {'number': 3, 'text': 'c'},
        {'number': 4, 'text': 'd'},
    ]
    table = bqtools.BQTable(schema=schema, data=rows)
    
    table.schema = table.schema + [{'name': 'test', 'field_type': 'STRING'}]
    assert len(table.schema) == 3
    assert len(table.data) == 3
    assert len(table.rows()) == 4

def test_bqtools_set_schema():
    schema = [
        {'name': 'number', 'field_type': 'INTEGER'},
        {'name': 'text', 'field_type': 'STRING'},
        {'name': 'struct', 'field_type': 'RECORD', 'mode': 'REPEATED', 'fields':[
            {'name': 'int_field', 'field_type': 'INTEGER'},
            {'name': 'str_field', 'field_type': 'STRING'}
        ]}
    ]
    table = bqtools.BQTable(schema=schema)

    assert all([isinstance(s, bigquery.SchemaField) for s in table.schema])
    assert all([isinstance(f, bigquery.SchemaField) for f in table.schema[2].fields])
# def test_bqtools_iadd_insert_schema():
#     schema = [
#         {'name': 'number', 'field_type': 'INTEGER'},
#         {'name': 'text', 'field_type': 'STRING'},
#     ]
#     rows = [
#         {'number': 1, 'text': 'a'},
#         {'number': 2, 'text': 'b'},
#         {'number': 3, 'text': 'c'},
#         {'number': 4, 'text': 'd'},
#     ]
#     table = bqtools.BQTable(schema=schema, data=rows)
    
#     table.schema += [{'name': 'test', 'field_type': 'STRING'}]
#     table.schema.insert(1, {'name': 'test', 'field_type': 'STRING'})
#     assert len(table.schema) == 3
#     assert len(table.data) == 3
#     assert len(table.rows()) == 4
    