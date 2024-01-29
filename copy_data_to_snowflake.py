# Copy data from s3 to snowflake.

# Create table in snowflake

# create or replace TABLE EMPLOYEE (
# 	FIRST_NAME VARCHAR(16777216),
# 	LAST_NAME VARCHAR(16777216),
# 	EMAIL VARCHAR(16777216),
# 	ADDRESS VARCHAR(16777216),
# 	CITY VARCHAR(16777216),
# 	DOJ DATE
# );

import os
from dotenv import load_dotenv

from snowflake.snowpark import Session
from snowflake.snowpark import QueryRecord
from snowflake.snowpark.functions import col
from snowflake.snowpark.functions import substr

from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType

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

# Create below Schema,

schema = StructType([StructField("FIRST_NAME", StringType()),
StructField("LAST_NAME", StringType()),
StructField("EMAIL", StringType()),
StructField("ADDRESS", StringType()),
StructField("CITY", StringType()),
 StructField("DOJ",DateType())])

# Use session.read.schema and session.read.csv and mention the command to read data from s3
employee_s3 = session.read.schema(schema).csv('@my_s3_stage/employee/')

# use copy_into_table method and try to copy the data from s3 to snowflake table, Employee. store the returned result in dataframe, copied_into_result
copied_into_result = employee_s3.copy_into_table("employee", target_columns=['FIRST_NAME','LAST_NAME','EMAIL','ADDRESS','CITY','DOJ'],on_error="CONTINUE")

copied_into_result
# The returned result will be a list. Convert this list to snowpark dataframe.
# Display the output
copied_into_result = session.createDataFrame(copied_into_result)
copied_into_result.show()


# Transform data and load to snowflake.

copied_into_result = employee_s3.copy_into_table("employee", \
    target_columns=['FIRST_NAME','LAST_NAME','EMAIL','ADDRESS','CITY','DOJ'], transformations=['$1','$2','$3',substr('$4',0,5),'$5','$6'],\
        force=True,on_error="CONTINUE")
