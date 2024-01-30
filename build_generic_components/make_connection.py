import os
from dotenv import load_dotenv
from snowflake.snowpark import Session
import sys
sys.path.append((os.getcwd()))
from generic_code import code_library

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

session = code_library.snow_connection(connection_parameters)