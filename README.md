# Python Micro-ORM for BigQuery

Map Python object to BigQuery Table with Schema and Data through
+ Variables (name, type, mode='NULLABLE', description=None)
+ Type checking / inference

Supported workflows:
- Create Object with Schema and add Data with Type checks
- Upload Object to Table (create, overwrite, append), maybe streaming
- Download Table Schema (and opt. Data) to Object
- Modify Object Schema