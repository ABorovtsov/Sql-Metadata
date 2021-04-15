from textwrap import indent
import numpy as np
import pandas as pd
import pathlib


class MetaExporter:
    def __init__(self, link_generator = None):
        self.link_gen = link_generator

    def to_df(self, metas):
        df = pd.DataFrame([meta.as_json() for meta in metas])
        df.sort_values(by='name', inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.index.rename('idx', inplace=True)
        return df

    def to_csv(self, metas, path):
        df = self.to_df(metas)
        df.to_csv(path)

    def to_drawio(self, metas, path, exclude_dependencies = ['sp_executesql', 'sp_getapplock'], filter_mask = None, chunk_size = None, columns = None):
        df = self.to_df(metas)
        
        if filter_mask:
            df = df.loc[filter_mask(df)]

        dependencies = self.__get_dependencies(df, exclude_dependencies, unique=False)
        df = df.append(self.__get_dependencies(df, exclude_dependencies, unique=True), sort=False)
        df.reset_index(drop=True, inplace=True)
        df.index.rename('idx', inplace=True)
        df = df.replace({np.nan: None})

        df['refs'] = df.apply(lambda v: self.__indexer(df, v), axis=1)
        df['reused'] = df.name.apply(lambda v: v in list(dependencies.name))
        df['shape_type'] = df.apply(lambda row: self.__get_shape_type(row), axis=1) 
        if columns:
            df = df[columns]

        output_files = [path]
        df.to_csv(path)

        if chunk_size:
            reusable_df = df.loc[df.reused]
            part = 0
            for chunk in pd.read_csv(path,chunksize=chunk_size,dtype={'idx': int, 'refs': str}):
                chunk.set_index('idx')
                chunk_refs = []
                for refs in list(chunk.refs.astype(str)):
                    if refs and len(refs) > 0 and refs != 'nan':
                        chunk_refs.extend([int(ref) for ref in refs.split(',')])

                reusable_df_part = reusable_df.loc[(reusable_df.index.isin(chunk_refs)) & (~reusable_df.index.isin(chunk.index))]
                chunk = chunk.append(reusable_df_part, sort=False)
                chunk.index.rename('idx', inplace=True)

                if chunk.shape[0] > 0:
                    if columns:
                        chunk = chunk[columns]

                    output_path = path.replace('.csv', f'{part}.csv')
                    chunk.to_csv(output_path)
                    output_files.append(output_path)

                part += 1
        
        self.__add_drawio_header(output_files)

    def __add_drawio_header(self, output_files):
        header = ''
        with open(pathlib.Path(__file__).parent.joinpath('drawio_header.tpl').absolute(), 'r') as file:
            header = file.read()

        for output_file in output_files:
            content = ''
            with open(output_file, 'r') as file:
                content = file.read()

            with open(output_file, 'w') as file:
                file.write(header + content)


    def __get_dependencies(self, df, exclude, unique = True):
        dependencies_df = pd.DataFrame(columns=df.columns)
        dependencies_tables = self.__get_unique(df.tables)
        dependencies_tables = list(filter(lambda x: x not in exclude, dependencies_tables))
        dependencies_df['name']=pd.Series(dependencies_tables)
        dependencies_df.type = 'Table'
        if self.link_gen:
            dependencies_df.link = dependencies_df.name.apply(self.link_gen.get_table_link)

        dependencies_sps = self.__get_unique(df.sps)
        dependencies_sps = list(filter(lambda x: x not in exclude, dependencies_sps))

        if unique:
            dependencies_sps = list(filter(lambda x: x not in list(df.name), dependencies_sps))

        dependencies_sps_df = pd.DataFrame(columns=df.columns)
        dependencies_sps_df['name']=pd.Series(dependencies_sps)
        dependencies_sps_df.type = 'SP'

        dependencies_df = dependencies_df.append(dependencies_sps_df, sort=False)
        return dependencies_df

    def __get_shape_type(self, row):
        resource_type = row['type']
        reused = row['reused']
        status = row['status'] if 'status' in row else ''
        task = row['task'] if 'task' in row else ''

        if 'SP' in resource_type:
            if status and 'done' in status.lower():
                return 'doneblock' # SP with a task done
            if status and 'todo' in status.lower():
                if task and 'jira' in task.lower():
                    return 'blockwithtask' # SP with todo and a task
                return 'todoblock' # SP with a todo
            if reused:
                return 'blockhub' # reusable SP
            return 'block' # SP

        return 'table' # tables, etc

    def __get_unique(self, list_series):
        dependencies = []
        for i in list_series:
            dependencies.extend(i)

        dependencies = [i.replace(',', '') for i in dependencies]
        dependencies = list(set(dependencies))
        dependencies.sort()
        return dependencies

    def __indexer(self, resources_df, row):
        tables_to_index = row.tables
        sps_to_index = row.sps
        dependencies_to_index = []
        if tables_to_index:
            dependencies_to_index.extend(tables_to_index)
        if sps_to_index:
            dependencies_to_index.extend(sps_to_index)
            
        if not dependencies_to_index:
            return None

        i = [str(i) for i in list(resources_df.loc[resources_df.name.isin(dependencies_to_index)].index)]
        return ','.join(i)