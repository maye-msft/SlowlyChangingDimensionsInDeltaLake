# Databricks notebook source
# MAGIC %pip install dbxscd

# COMMAND ----------

import dbxscd 
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

df = spark.createDataFrame([('FA', 'Fiji Apple', 'Red', 3.5)
                           ,('BN', 'Banana', 'Yellow', 1.0)
                           ,('GG', 'Green Grape', 'Green', 2.0)
                           ], 
                           ['ShortName','Fruit', 'Color', 'Price'])


display(df)


df2 = spark.createDataFrame([('FA', 'Fiji Apple', 'Red', 3.6)
                           ,('BN', 'Banana', 'Yellow', 1.0)
                           ,('GG', 'Green Grape', 'Green', 2.0)
                           ,('RG', 'Red Grape', 'Red', 2.0)
                           ], 
                           
                           ['ShortName','Fruit', 'Color', 'Price'])


display(df2)


# COMMAND ----------

import datetime
current_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
target_partition_keys = ['ShortName']
key_cols = "ShortName,Fruit"

# COMMAND ----------
%md
##SCD1

# COMMAND ----------

target_table_name_scd1 = 'default.table_scd1'
dbxscd.SCD1(spark, df, target_table_name_scd1, target_partition_keys, key_cols, current_time)
display(spark.sql(f"select * from {target_table_name_scd1}"))

# COMMAND ----------

dbxscd.SCD1(spark, df2, target_table_name_scd1, target_partition_keys, key_cols, current_time)
display(spark.sql(f"select * from {target_table_name_scd1}"))

# COMMAND ----------
%md
##SCD2

# COMMAND ----------

target_table_name_scd2 = 'default.table_scd2'
dbxscd.SCD2(spark, df, target_table_name_scd2, target_partition_keys, key_cols, current_time)
display(spark.sql(f"select * from {target_table_name_scd2}"))

# COMMAND ----------

dbxscd.SCD2(spark, df2, target_table_name_scd2, target_partition_keys, key_cols, current_time)
display(spark.sql(f"select * from {target_table_name_scd2}"))

# COMMAND ----------
%md
##SCD3

# COMMAND ----------
target_table_name_scd3 = 'default.table_scd3'
dbxscd.SCD3(spark, df, target_table_name_scd3, target_partition_keys, key_cols, current_time)
display(spark.sql(f"select * from {target_table_name_scd3}"))

# COMMAND ----------

dbxscd.SCD3(spark, df2, target_table_name_scd3, target_partition_keys, key_cols, current_time)
display(spark.sql(f"select * from {target_table_name_scd3}"))