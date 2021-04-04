import numpy as np
import pandas as pd



class MetaExporter:
    def to_df(self, metas):
        df = pd.DataFrame([meta.as_json() for meta in metas])
        df.sort_values(by='name', inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.index.rename('idx', inplace=True)
        return df

    def to_csv(self, metas, path):
        df = self.to_df(metas)
        df.to_csv(path)

    def to_drawio(self, metas, path):
        df = self.to_df(metas)
        df = df.append(self.get_dependencies(df))
        df.reset_index(drop=True, inplace=True)
        df.index.rename('idx', inplace=True)
        df = df.replace({np.nan: None})

        df['refs'] = df.apply(lambda v: self.__indexer(df, v), axis=1)
        df.to_csv(path)

    def get_dependencies(self, df):
        dependencies_df = pd.DataFrame(columns=df.columns)
        dependencies_df['name']=pd.Series(self.__get_unique(df.tables))
        dependencies_df.type = 'Table'

        dependencies_sps = self.__get_unique(df.sps)
        dependencies_sps = list(filter(lambda x: x not in list(df.name) and x not in ['sp_executesql', 'sp_getapplock'], dependencies_sps))
        dependencies_sps_df = pd.DataFrame(columns=df.columns)
        dependencies_sps_df['name']=pd.Series(dependencies_sps)
        dependencies_sps_df.type = 'SP'

        dependencies_df = dependencies_df.append(dependencies_sps_df)
        return dependencies_df

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