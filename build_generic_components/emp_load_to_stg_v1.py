import os
import sys
sys.path.append((os.getcwd()))
from generic_code import code_library
from schema import src_stg_schema
from config import connection_details
import json

# Read from config file.
config_snow_copy_file_name = 'copy_to_snow_stg.json'
config_snow_copy_file_path = os.getcwd()+'/config/'

config_snow_copy = open(config_snow_copy_file_path+config_snow_copy_file_name)
config_snow_copy = json.loads(config_snow_copy.read())
print(config_snow_copy)

# Read from connection_details.py
session = code_library.snow_connection(connection_details.connection_parameters)

print(src_stg_schema.emp_stg_schema)

copied_into_result, qid = code_library.copy_to_table(session,config_snow_copy,src_stg_schema.emp_stg_schema)

print(copied_into_result)
print(qid)

copied_into_result_df = session.create_dataframe(copied_into_result)
copied_into_result_df.show()