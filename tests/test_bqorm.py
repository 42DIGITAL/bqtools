import bqorm

def test_bqorm_construct_columns():
    schema = [
        {'name': 'number', 'field_type': 'INTEGER'},
        {'name': 'text', 'field_type': 'STRING'},
    ]
    columns = [[1, 2, 3, 4], ['a', 'b', 'c', 'd']]
    table = bqorm.BQTable(schema=schema, data=columns)
    
    assert len(table.schema) == 2
    assert len(table.data) == 2
    assert len(table.rows()) == 4

def test_bqorm_construct_rows_dicts():
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
    table = bqorm.BQTable(schema=schema, data=rows)

    assert len(table.schema) == 2
    assert len(table.data) == 2
    assert len(table.rows()) == 4
