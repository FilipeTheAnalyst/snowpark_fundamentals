import os
from dotenv import load_dotenv
import sys
sys.path.append((os.getcwd()))
from generic_code import code_library
from snowflake.snowpark.types import StringType, StructField, StructType, DateType

load_dotenv()

account=os.getenv("ACCOUNT")
user=os.getenv("USER")
password=os.getenv("PASSWORD")
role=os.getenv("ROLE")
warehouse=os.getenv("WAREHOUSE")
database=os.getenv("DATABASE")
schema=os.getenv("SCHEMA")

connection_parameters = {
    "account":account,
    "user":user,
    "password":password,
    "role":role,
    "warehouse":warehouse,
    "database":database,
    "schema":schema
}

config_file = {"Database_name":database,\
"Schema_name":schema,
"Target_table":"EMPLOYEE",
"Reject_table":"EMPLOYEE_REJECTS",
"target_columns":["FIRST_NAME","LAST_NAME","EMAIL","ADDRESS","CITY","DOJ"],
"on_error":"CONTINUE",
"Source_location":"@my_s3_stage/employee/",
"Source_file_type":"csv"
}
    
# Declare schema for csv file and read data
schema = StructType([StructField("FIRST_NAME", StringType()),
StructField("LAST_NAME", StringType()),
StructField("EMAIL", StringType()),
StructField("ADDRESS", StringType()),
StructField("CITY", StringType()),
StructField("DOJ",DateType())])

session = code_library.snow_connection(connection_parameters)
copied_into_result, qid = code_library.copy_to_table(session,config_file,schema)

print(copied_into_result)
print(qid)

copied_into_result_df = session.create_dataframe(copied_into_result)
copied_into_result_df.show()