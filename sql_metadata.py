import re 
import json
import os


class SQLMetadata:
    def __init__(self, sql_path, table_aliases = None):
        # todo: validate SP exists and not empty
        self.table_aliases = table_aliases
        lines = []
        with open(sql_path, 'r') as file:
            lines = [line for line in file.readlines() if not re.match(r'^\s*(--|/\*)', line)] # remove the commented lines
        
        variables = [line.lower().split()[1:3] for line in lines if re.match(r'\s*declare\s+@', line, re.IGNORECASE)]
        self.variables = {variable[0]:variable[1] for variable in variables}
        self.sql = ' '.join(lines).replace('\n', ' ').replace('\r', ' ')
        self.name = os.path.basename(sql_path).replace('.sql', '')

    def __get_tables(self):
        pattern = r"(^from\s+|\s+from\s+|" + \
                  r"^join\s+|\s+join\s+|" + \
                  r"delete\s+from\s+|" + \
                  r"insert\s+into\s+|" + \
                  r"\s+update\s+top\s*\(\d+\)\s+|^update\s+|\s+update\s+|" + \
                  r"merge\s+into\s+)([\s\+']*)(\S+)"

        table_names = re.findall(pattern, self.sql, re.IGNORECASE)
        table_names = set([self.__clean_up(table_name[1] + table_name[2]).lower() for table_name in table_names])
        table_names = list(filter(lambda i: 
            not i.startswith('(') and 
            not (i.startswith('@') and 'table' in self.variables.get(i, '').lower()) and 
            not (i.startswith('@') and self.variables.get(i, '').lower().endswith('type')) and 
            not (i.startswith('@') and i not in self.variables) and 
            not i.startswith('#') and 
            not i.startswith('sys.') and 
            i.lower() != 'sysobjects' and
            not i.lower().startswith('cte') and
            not i.lower().endswith('cte') and
            not i.lower().endswith('cursor') and
            i.lower() != 'of', table_names))

        if self.table_aliases:
            for alias in self.table_aliases:
                table_names = [self.table_aliases[alias] if alias.lower() == name else name for name in table_names]
            
            table_names = list(set(table_names))

        table_names.sort()
        return table_names

    def __get_sps(self):
        pattern = r"\s+(exec\s+@[a-z]+\s*=\s*|exec\s+|execute\s+)(\S+)"
        sp_names = re.findall(pattern, self.sql, re.IGNORECASE)
        sp_names = set([self.__clean_up(sp_name[1]).lower() for sp_name in sp_names])
        sp_names = list(filter(lambda i: not i.startswith('sys.') and i.lower() != 'rethrowerror' , sp_names))
        sp_names.sort()
        return sp_names

    def has_app_lock(self):
        sps = self.__get_sps()
        return 'sp_getapplock' in sps

    def has_tx(self):
        return re.search(r'commit\s+tran', self.sql, re.IGNORECASE) != None

    def has_dynamic_sql(self):
        sps = self.__get_sps()
        return 'sp_executesql' in sps

    def as_json(self):
        return \
        {
            'name': self.name,
            'has_app_lock': self.has_app_lock(),
            'has_tx': self.has_tx(), 
            'has_dynamic_sql': self.has_dynamic_sql(),
            'tables': self.tables,
            'sps': self.sps,
        }

    def __clean_up(self, line):
        return line \
            .replace('[', '') \
            .replace(']', '') \
            .replace('dbo.', '') \
            .replace(';', '') \
            .replace("'", '') \
            .replace("+", '') \
            .strip()

    def __str__(self):
        return json.dumps(self.as_json(), indent=2)

    tables = property(__get_tables)
    sps = property(__get_sps)
