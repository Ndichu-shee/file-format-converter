import pandas as pd
import os
import json
import glob
import sys
from dotenv import load_dotenv

load_dotenv()


def get_all_columns(schema,data_set_name, sorting_key='column_position'):
    data_set = schema[data_set_name]
    sorted_data = sorted(data_set, key= lambda col:col[sorting_key])
    return [data['column_name']for data in sorted_data]

def read_data_from_csv(file,schema):
    file_extension = file.split('/')
    data_set_name = file_extension[2]
    all_columns = get_all_columns(schema,data_set_name)
    all_data_frame = pd.read_csv(file, names=all_columns) #convert a dict to a dataframe(rows and column) path, header, names(column names)
    return all_data_frame

def convert_to_json(all_data_frame,data_set_name,file_name,target_dir):
    file_path = f"{target_dir}{data_set_name}/{file_name}"
    os.makedirs(f"{target_dir}{data_set_name}",exist_ok=True) #creates a directory pathe, exists_ok(if True it does not throw an error)if it does not exists
    all_data_frame.to_json(file_path,orient='records',lines=True) #loads rows and column to json data/string path, orient, lines


def process_file_data(data_set_name,source_dir,target_dir):
    schema = json.load(open(f'{source_dir}schemas.json'))
    file_dirs = glob.glob(f'{source_dir}{data_set_name}/part-*')
    if len(file_dirs) == 0:
        raise NameError(f"file with name {data_set_name} does not exist")
    for file in file_dirs:
        all_data_frame = read_data_from_csv(file,schema)
        file_split = file.split('/')
        file_name = file_split[-1]
        convert_to_json(all_data_frame,data_set_name,file_name,target_dir)


def process_files(data_set_name=None):
    source_dir = os.getenv('SOURCE_DIR')
    target_dir =os.getenv('TARGET_DIR')
    schemas = json.load(open(f'{source_dir}schemas.json'))
    if not data_set_name:
        data_set_name = schemas.keys()
    for data_set in data_set_name:
        try:
            print(f'Processing {data_set}')
            process_file_data(data_set,source_dir,target_dir)
        except NameError as ne:
            print(f"{ne}")
            print(f"unable to process {data_set}")
            pass

if __name__ == '__main__':
    if len(sys.argv) == 2:
        ds_names = json.loads(sys.argv[1])
        process_files(ds_names)
    else:
        process_files()

