import posixpath
import re

class StashLinkGenerator:
    def __init__(self, repo_stash_url, repo_local_path, tables_script):
        if repo_stash_url[-1] != '/':
            repo_stash_url = repo_stash_url + '/'

        self.repo_stash_url = repo_stash_url.lower() + 'browse/'

        if repo_local_path[-2:] != '\\':
            repo_local_path = repo_local_path + '\\'

        self.repo_local_path = repo_local_path
        self.table_index = {}
        self.tables_script = tables_script

        with open(tables_script, 'r') as file:
            line_nom = 0
            while True:
                line = file.readline()
                line_nom += 1

                if not line:
                    break

                match  = re.search(r'(create table )(\S+)', line, re.IGNORECASE)
                if match:
                    table_name = self.__clean_up_table_name(match.group(2))
                    self.table_index[table_name] = line_nom

    def get(self, file_path):
        relative_link = self.__get_relative_link(file_path)
        return (self.repo_stash_url + relative_link + '?at=refs/heads/master')

    def __get_relative_link(self, file_path):
        relative_path = file_path \
            .replace(self.repo_local_path, '') \
            .replace('\\', '/') \
            .replace(' ', '%20')
        return relative_path

    def get_table_link(self, table_name):
        relative_link = self.__get_relative_link(self.tables_script)
        line_nom = str(self.table_index.get(table_name, ''))
        line_parameter = f"#{line_nom}" if len(line_nom) > 0 else ''
        return (self.repo_stash_url + relative_link + '?at=refs/heads/master' + line_parameter)

    def __clean_up_table_name(self, table_name: str) -> str:
        for r in ['[', ']', '(', ')', 'dbo.', ';', "'", "+"]:
            table_name = table_name.replace(r, '')
        return table_name.strip()
