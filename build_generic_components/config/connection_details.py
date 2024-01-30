import os
from dotenv import load_dotenv

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
