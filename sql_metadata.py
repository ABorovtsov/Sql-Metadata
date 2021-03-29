import re 

class SQLMetadata:
    def __init__(self, sql_path):
        # todo: validate SP exists and not empty
        with open(sql_path, 'r') as file:
            self.sql = file.read().replace('\n', ' ').replace('\r', ' ')

    def __get_tables(self):
        pattern = r"(from\s+|join\s+|delete\s+from\s+|insert\s+into\s+|update\s+|merge\s+into\s+)(\S+)"
        table_names = re.findall(pattern, self.sql, re.IGNORECASE)
        table_names = set([self.__clean_up(table_name[1]) for table_name in table_names])
        table_names = list(filter(lambda i: not i.startswith('@') and not i.startswith('#') and not i.startswith('sys.'), table_names))
        table_names.sort()
        return table_names

    def __get_sps(self):
        pattern = r"\s+(exec\s+@[a-z]+\s*=\s*|exec\s+|execute\s+)(\S+)"
        sp_names = re.findall(pattern, self.sql, re.IGNORECASE)
        sp_names = set([self.__clean_up(sp_name[1]) for sp_name in sp_names])
        sp_names = list(filter(lambda i: not i.startswith('sys.') and i.lower() != 'rethrowerror' , sp_names))
        sp_names.sort()
        return sp_names

    def __clean_up(self, line):
        return line \
            .replace('[', '') \
            .replace(']', '') \
            .replace('dbo.', '') \
            .replace(';', '')

    tables = property(__get_tables)
    sps = property(__get_sps)
