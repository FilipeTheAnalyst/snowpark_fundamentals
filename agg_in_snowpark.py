import os
from dotenv import load_dotenv
from snowflake.snowpark import Session
from snowflake.snowpark import Window
from snowflake.snowpark.functions import col,min,rank,max,avg
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

employee = session.table("demo_db.public.employee")

employee_grp = employee.select("FIRST_NAME").group_by("FIRST_NAME")
employee_grp_cnt = employee_grp.count()
employee_grp_cnt.show()
employee_dups = employee_grp_cnt.where(col("COUNT")>1)
employee_dups.show()

employee_dups = employee.select("FIRST_NAME").group_by("FIRST_NAME").count().where(col("COUNT")>1)


###################################################################################

supplier = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER")

supplier_min = supplier.select(min("s_acctbal").as_("min_acc_bal"))
type(supplier_min)
type(supplier_min)

acct_bal = supplier_min.collect()
type(acct_bal)
min_bal = acct_bal[0]['MIN_ACC_BAL']
type(min_bal)

supplier_name = supplier.select("s_name","s_acctbal").where(col("s_acctbal")==min_bal)

supplier_name.show()

#########################################################################
# Window Functions ######################################################

supplier = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER")

supplier_rank = supplier.select("s_name","s_acctbal",rank().over(Window.order_by("s_acctbal")).\
    as_("RANK")).where(col("RANK")==1)
supplier_rank.show()

##########################################################################

supplier = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER")

suplier_agg = supplier.select(min("s_acctbal"),max("s_acctbal"),avg("s_acctbal"))
suplier_agg.show()

agg_input = [("s_acctbal","min"),("s_acctbal","max"),("s_acctbal","avg")]

suplier_agg = supplier.agg(agg_input)
suplier_agg.show()

suplier_agg = supplier.agg([{'s_acctbal':'min'}])


##########################################################################

