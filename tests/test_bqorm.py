import bqorm

def test_bqorm_construct_columns():
    schema = [
        {'name': 'number', 'field_type': 'INTEGER'},
        {'name': 'text', 'field_type': 'STRING'},
    ]
    columns = [[1, 2, 3, 4], ['a', 'b', 'c', 'd']]
    table = bqorm.BQTable(schema=schema, columns=columns)
    
    assert len(table.get_schema()) == 2
    assert len(table.get_data_columns()) == 2
    assert len(table.get_data_rows()) == 4

def test_bqorm_construct_rows_lists():
    schema = [
        {'name': 'number', 'field_type': 'INTEGER'},
        {'name': 'text', 'field_type': 'STRING'},
    ]
    rows = [[1, 'a'], [2, 'b'], [3, 'c'], [4, 'd']]
    table = bqorm.BQTable(schema=schema, rows=rows)

    assert len(table.get_schema()) == 2
    assert len(table.get_data_columns()) == 2
    assert len(table.get_data_rows()) == 4

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
    table = bqorm.BQTable(schema=schema, rows=rows)

    assert len(table.get_schema()) == 2
    assert len(table.get_data_columns()) == 2
    assert len(table.get_data_rows()) == 4
