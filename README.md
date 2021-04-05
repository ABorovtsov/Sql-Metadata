# Sql-Metadata

The tool grabs external files with sql code inside and collects metadata such as dependencies (tables, stored procedures) and attribures like SQL transactions, app_locks, dynamic SQL. The output format is compatible with the input of the https://drawio-app.com/import-from-csv-to-drawio/ so you can make a visualization in few clicks.

## Usage

```python
from meta_export import MetaExporter

path = '.' # a directory with sql scripts
include_scripts = ['one.sql', 'two.sql'] # an optional filter
MetaExporter(path, include_scripts).to_csv('result.csv')
```
