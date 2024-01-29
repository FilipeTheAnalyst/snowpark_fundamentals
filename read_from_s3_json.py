import os
from dotenv import load_dotenv
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import time

from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType

import pandas as pd

load_dotenv()

WAREHOUSE = os.getenv("WAREHOUSE")

connection_parameters = {
    "account": os.getenv("ACCOUNT"),
    "user": os.getenv("USER"),
    "password": os.getenv("PASSWORD"),
    "role": os.getenv("ROLE"),
    "warehouse": WAREHOUSE,
    "database": os.getenv("DATABASE"),
    "schema": os.getenv("SCHEMA")
}

session = Session.builder.configs(connection_parameters).create()

# Mention command to read data from '@my_s3_stage/json_folder/'
employee_s3_json = session.read.json('@my_s3_stage/json_folder/')
employee_s3_json.show()
employee_s3_json.cache_result()

employee_s3_json = employee_s3_json.select_expr("$1:author","$1:id","$1:cat")
employee_s3_json.show()
employee_s3_json.cache_result()

employee_s3_json.schema

