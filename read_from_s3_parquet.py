import os
from dotenv import load_dotenv
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import time

from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType, TimestampType,DoubleType

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

# Read data from s3 from the location, '@my_s3_stage/parquet_folder/' and create dataframe, employee_s3_parquet
employee_s3_parquet = session.read.parquet('@my_s3_stage/parquet_folder/')
employee_s3_parquet.show()

# Mention command to list all the columns in dataframe, employee_s3_parquet
employee_s3_parquet.columns

# Select only below columns from the dataframe, first_name , last_name, email, gender
employee_s3_parquet = employee_s3_parquet.select(col('"first_name"'),col('"last_name"'),col('"email"'),col('"gender"'))


# Mention command to write dataframe, employee_s3_parquet to snowflake.
employee_s3_parquet.write.mode("append").save_as_table("int_emp_details_parquet",column_order="name")

