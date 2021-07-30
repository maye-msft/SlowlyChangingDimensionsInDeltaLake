# Databricks notebook source
# MAGIC %run ./SCD

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

df = spark.createDataFrame([('FA', 'Fiji Apple', 'Red', 3.5)
                           ,('BN', 'Banana', 'Yellow', 1.0)
                           ,('GG', 'Green Grape', 'Green', 2.0)
                           ,('RG', 'Red Grape', 'Red', 2.0)
                           ,('PC', 'Peach', 'Yellow', 3.0)
#                            ,('OG', 'Orange', 'Orange', 2.0)
#                            ,('GA', 'Green Apple', 'Green', 2.5)
                           ], 
                           
                           ['ShortName','Fruit', 'Color', 'Price'])


display(df)


# COMMAND ----------

import datetime
current_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
target_partition_keys = ['ShortName']
key_cols = "ShortName,Fruit"

# COMMAND ----------

target_table_name_scd1 = 'default.table_scd1'
SCD1(df, target_table_name_scd1, target_partition_keys, key_cols, current_time)
display(spark.sql(f"select * from {target_table_name_scd1}"))

# COMMAND ----------

target_table_name_scd2 = 'default.table_scd2'
SCD2(df, target_table_name_scd2, target_partition_keys, key_cols, current_time)
display(spark.sql(f"select * from {target_table_name_scd2}"))

# COMMAND ----------

target_table_name_scd3 = 'default.table_scd3'
SCD3(df, target_table_name_scd3, target_partition_keys, key_cols, current_time)
display(spark.sql(f"select * from {target_table_name_scd3}"))
