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

test = session.create_dataframe(pd.DataFrame([(1, 2, 3, 4,5)], columns=["a", "b", "c", "d","e"]))

test.show()

type(test)

# update to the temporary table name defined by Snowflake when executing the previous command
test2 = session.table("DEMO_DB.PUBLIC.SNOWPARK_TEMP_TABLE_6NU5I0127U")

test2.show()


