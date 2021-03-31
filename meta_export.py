from sql_metadata import SQLMetadata
from pathlib import Path
import os
import pandas as pd

class MetaExporter:
    def __init__(self, root_script_directory, include_scripts = None, table_aliases = None):
        self.root_directory = root_script_directory
        self.include_scripts = include_scripts
        self.table_aliases = table_aliases

    def __get_sql_paths(self):
        sql_paths = list(Path(self.root_directory).rglob("*.[sS][qQ][lL]"))
        if self.include_scripts:
            sql_paths = list(filter(lambda x: os.path.basename(x) in self.include_scripts, sql_paths)) 
        
        return [str(path) for path in sql_paths]

    def to_csv(self, path):
        sql_paths = self.__get_sql_paths()
        metas = [SQLMetadata(path, self.table_aliases).as_json() for path in sql_paths]
        df = pd.DataFrame(metas)
        df.sort_values(by='name', inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.index.rename('idx', inplace=True)
        df.to_csv(path)
