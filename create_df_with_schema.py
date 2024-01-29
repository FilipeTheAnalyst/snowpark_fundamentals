import os
from dotenv import load_dotenv
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import time

from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType

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
session.sql(f"USE WAREHOUSE {WAREHOUSE}").collect()

schema = StructType([StructField("one", IntegerType()),
StructField("two",  IntegerType()),
StructField("three",  IntegerType()),
StructField("four",  DateType())])

test = session.create_dataframe([[1, 2, 3, '2022-01-26'],[1, 2, 3, '2022-01-26'],[1, 2, 3, '2022-01-26'],[1, 2, 3, '2022-01-26']], schema=["a","b","c","d"])
test.show()

test = session.create_dataframe([[1, 2, 3, '2022-01-26'],[1, 2, 3, '2022-01-26'],[1, 2, 3, '2022-01-26'],[1, 2, 3, '2022-01-26']], schema=schema)
test.show()


