from sql_metadata import SQLMetadata
from pathlib import Path
import os


class MetaProvider:
    def get(self, root_script_directory, include_scripts = None, table_aliases = None, overrides = {}, link_gen = None):
        sql_paths = self.__get_script_paths(root_script_directory, include_scripts)
        return [SQLMetadata(path, table_aliases, overrides.get(self.__get_name(path), {}), link_gen) for path in sql_paths]

    def __get_script_paths(self, root_script_directory, include_scripts):
        sql_paths = list(Path(root_script_directory).rglob("*.[sS][qQ][lL]"))
        if include_scripts:
            sql_paths = list(filter(lambda x: self.__get_name(x) in include_scripts, sql_paths)) 
        
        return [str(path) for path in sql_paths]

    def __get_name(self, full_script_path):
        return os.path.basename(full_script_path).replace('.sql', '')
