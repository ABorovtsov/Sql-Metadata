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
        df.to_csv(path)
