import os
import sys
sys.path.append((os.getcwd()))
from generic_code import code_library
from schema import src_stg_schema
from config import connection_details
import json

# Read from config file.
config_snow_copy_file_name = 'copy_to_snow_stg_avro.json'
config_snow_copy_file_path = os.getcwd()+'/config/'

config_snow_copy = open(config_snow_copy_file_path+config_snow_copy_file_name, "r")
config_snow_copy = json.loads(config_snow_copy.read())

# Read from connection_details.py
session = code_library.snow_connection(connection_details.connection_parameters)

copied_into_result, qid = code_library.copy_to_table_semi_struct_data(session,config_snow_copy,src_stg_schema.emp_details_avro_cls)
print(copied_into_result)