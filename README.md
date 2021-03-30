# Sql-Metadata

The tool grabs external files with sql code inside and collects metadata such as dependencies (tables, stored procedures) and attribures like SQL transactions, app_locks, dynamic SQL. The output format is compatible with the input of the https://drawio-app.com/import-from-csv-to-drawio/ so you can make a visulisation in few clicks.

## Usage

```python
from meta_export import MetaExporter

root_path = '.' # sql scripts root directory
include_scripts = ['one.sql', 'two.sql'] # optional filter
MetaExporter(root_path, include_scripts).to_csv('result.csv')
```
