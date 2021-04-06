from textwrap import indent
import numpy as np
import pandas as pd


class MetaExporter:
    ORDERED_COLUMNS = ['name','type','refs','reused','has_app_lock','has_tx','has_dynamic_sql',
            'shape','fill','stroke','link']

    def to_df(self, metas):
        df = pd.DataFrame([meta.as_json() for meta in metas])
        df.sort_values(by='name', inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.index.rename('idx', inplace=True)
        return df

    def to_csv(self, metas, path):
        df = self.to_df(metas)
        df.to_csv(path)

    def to_drawio(self, metas, path, exclude = ['sp_executesql', 'sp_getapplock'], chunk_size = None):
        df = self.to_df(metas)
        dependencies = self.__get_dependencies(df, exclude, unique=False)
        df = df.append(self.__get_dependencies(df, exclude, unique=True))
        df.reset_index(drop=True, inplace=True)
        df.index.rename('idx', inplace=True)
        df = df.replace({np.nan: None})

        df['refs'] = df.apply(lambda v: self.__indexer(df, v), axis=1)
        df['reused'] = df.name.apply(lambda v: v in list(dependencies.name))
        df['shape'] = df.type.apply(lambda t: 'mxgraph.basic.rect' if 'SP' in t else 'cylinder3') 
        df['fill'] = df.apply(lambda row: self.__get_fill_color(row['type'], row['reused']), axis=1)
        df['stroke'] = '#6C8EBF'
        df = df[MetaExporter.ORDERED_COLUMNS]

        df.to_csv(path)

        if chunk_size:
            reusable_df = df.loc[df.reused]
            # chunk_size = int(len(df.index) / partition_number)
            
            part = 0
            for chunk in pd.read_csv(path,chunksize=chunk_size,dtype={'idx': int, 'refs': str}):
                chunk.set_index('idx')
                chunk_refs = []
                for refs in list(chunk.refs.astype(str)):
                    if refs and len(refs) > 0 and refs != 'nan':
                        chunk_refs.extend([int(ref) for ref in refs.split(',')])

                reusable_df_part = reusable_df.loc[(reusable_df.index.isin(chunk_refs)) & (~reusable_df.index.isin(chunk.index))]
                chunk = chunk.append(reusable_df_part)
                chunk.index.rename('idx', inplace=True)

                if chunk.shape[0] > 0:
                    chunk[MetaExporter.ORDERED_COLUMNS].to_csv(path.replace('.csv', f'{part}.csv'))

                part += 1

    def __get_dependencies(self, df, exclude, unique = True):
        dependencies_df = pd.DataFrame(columns=df.columns)
        dependencies_tables = self.__get_unique(df.tables)
        dependencies_tables = list(filter(lambda x: x not in exclude, dependencies_tables))
        dependencies_df['name']=pd.Series(dependencies_tables)
        dependencies_df.type = 'Table'

        dependencies_sps = self.__get_unique(df.sps)
        dependencies_sps = list(filter(lambda x: x not in exclude, dependencies_sps))

        if unique:
            dependencies_sps = list(filter(lambda x: x not in list(df.name), dependencies_sps))

        dependencies_sps_df = pd.DataFrame(columns=df.columns)
        dependencies_sps_df['name']=pd.Series(dependencies_sps)
        dependencies_sps_df.type = 'SP'

        dependencies_df = dependencies_df.append(dependencies_sps_df)
        return dependencies_df

    def __get_fill_color(self, resource_type, reused):
        if 'SP' in resource_type:
            if reused:
                return '#B1CEFE' # reusable SP
            return '#DAE8FC' # SP

        return '#6AA9FF' # tables, etc

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