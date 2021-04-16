import pandas as pd
import os
import glob
from shutil import copy

def get_user_data_from_csv(csv_path: str) -> dict:
    user_data_df = pd.read_csv(csv_path, keep_default_na=True)
    user_data_df.tables = user_data_df.tables.apply(lambda x: eval(x) if 'nan' != str(x) else x)
    user_data_df.sps = user_data_df.sps.apply(lambda x: eval(x) if 'nan' != str(x) else x)
    user_data_df.set_index('name',inplace=True)
    user_data_df.fillna('NaN', inplace=True)
    user_data_df = user_data_df.to_dict('index')

    user_data_dict = {}
    for name in user_data_df:
        user_data = user_data_df[name]
        user_data_dict[name] = {key:user_data[key] for key in user_data if user_data[key] != 'NaN' }

    return user_data_dict

def copy_scripts(metas, destination_path):
    paths = list(map(lambda x: x.sql_path, metas))
    old_files = glob.glob(f'{destination_path}/*')

    for old_file in old_files:
        os.remove(old_file)

    for path in paths:
        copy(path, destination_path)
