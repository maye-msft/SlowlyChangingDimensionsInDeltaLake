# Databricks notebook source
def SCD1(spark, df, target_table_name, target_partition_keys, key_cols, current_time):
  # create a temp view
  input_view_name = 'INPUT_VIEW'
  df.createOrReplaceTempView(input_view_name)
  
  # deal with columns
  col = [x.upper() for x in df.columns]
  datahash_col = [x for x in col if not x in set(key_cols.upper().split(","))]
  keyhash_col = [x for x in col if x in set(key_cols.upper().split(","))]
  
  # deal with table name
  schema_name = target_table_name[:target_table_name.find('.')]
  tgt_tbl_name_without_schema = target_table_name[target_table_name.find('.')+1:] 
  
  # create a view with key_hash and data_hash 
  sql = f"""
  select a.*, 
    md5(concat(COALESCE( {",''), COALESCE(".join(keyhash_col)} ,''))) as key_hash,
    md5(concat(COALESCE( {",''), COALESCE(".join(datahash_col)} ,''))) as data_hash
    from {input_view_name} a
  """

  out_df = spark.sql(sql)
  output_view_name = 'OUTPUT_VIEW'
  out_df.createOrReplaceTempView(output_view_name);
  
  if not spark._jsparkSession.catalog().tableExists(schema_name, tgt_tbl_name_without_schema):
    sql = f"""
    CREATE TABLE {target_table_name} 
    USING DELTA 
    PARTITIONED BY ({",".join(target_partition_keys)}) 
    AS select a.*,
    cast('{current_time}' as timestamp)  as dw_insert_ts, 
    cast('{current_time}' as timestamp) as dw_update_ts 
    from {output_view_name} a """

    spark.sql(sql)
  else:
    # deal with new data
    insert_col = "dw_insert_ts, dw_update_ts, `"+("`,`".join(keyhash_col))+"`,`"+("`,`".join(datahash_col))+"`,`key_hash`, `data_hash`"
    insert_col_values = "`"+("`,`".join(keyhash_col))+"`,`"+("`,`".join(datahash_col))+"`"
    insert_values = f"""'{current_time}','{current_time}', {insert_col_values}, `key_hash`,`data_hash`"""
    
    # deal with changed data
    update_hash_values = f"{target_table_name}.key_hash = {output_view_name}.key_hash, \n{target_table_name}.data_hash = {output_view_name}.data_hash"
    update_key_values = ", \n".join([f"{target_table_name}.{set} = {output_view_name}.{set}" for set in list(dict.fromkeys(keyhash_col))])
    update_col_values = ", \n".join([f"{target_table_name}.{set} = {output_view_name}.{set}" for set in list(dict.fromkeys(datahash_col))])
    
    sql = f"""
    MERGE INTO {target_table_name}
    USING {output_view_name}
    ON {target_table_name}.key_hash <=> {output_view_name}.key_hash
    WHEN MATCHED AND {target_table_name}.data_hash != {output_view_name}.data_hash THEN
    UPDATE SET 
    dw_update_ts = '{current_time}', 
    {update_hash_values}, 
    {update_key_values}, 
    {update_col_values}
    WHEN NOT MATCHED THEN
    INSERT ( {insert_col} ) 
    VALUES ({insert_values})
    """

    spark.sql(sql)

# COMMAND ----------

def SCD2(spark, df, target_table_name, target_partition_keys, key_cols, current_time):
  # create a temp view
  input_view_name = 'INPUT_VIEW'
  df.createOrReplaceTempView(input_view_name)
  
  # deal with columns
  col = [x.upper() for x in df.columns]
  datahash_col = [x for x in col if not x in set(key_cols.upper().split(","))]
  keyhash_col = [x for x in col if x in set(key_cols.upper().split(","))]
  
  # deal with table name
  schema_name = target_table_name[:target_table_name.find('.')]
  tgt_tbl_name_without_schema = target_table_name[target_table_name.find('.')+1:]  
  
  # create a view with key_hash and data_hash 
  sql = f"""
  select a.*, 
    md5(concat(COALESCE( {",''), COALESCE(".join(keyhash_col)} ,''))) as key_hash,
    md5(concat(COALESCE( {",''), COALESCE(".join(datahash_col)} ,''))) as data_hash
    from {input_view_name} a
  """

  out_df = spark.sql(sql)
  output_view_name = 'OUTPUT_VIEW'
  out_df.createOrReplaceTempView(output_view_name);
  
  if not spark._jsparkSession.catalog().tableExists(schema_name, tgt_tbl_name_without_schema):
    sql = f"""
    CREATE TABLE {target_table_name} 
    USING DELTA 
    PARTITIONED BY ({",".join(target_partition_keys)}) 
    AS select a.*,
    cast('{current_time}' as timestamp)  as dw_insert_ts, 
    cast('{current_time}' as timestamp) as dw_update_ts,
    'Y' as row_latest_ind, 
    cast('{current_time}' as timestamp)  as row_valid_from_ts, 
    cast('9999-12-31 00:00:00.000000+00:00' as timestamp) as row_valid_to_ts
    from {output_view_name} a """

    spark.sql(sql)
  else:
    
    insert_col = "row_latest_ind, row_valid_from_ts, row_valid_to_ts, dw_insert_ts, dw_update_ts, `"+("`,`".join(keyhash_col))+"`,`"+("`,`".join(datahash_col))+"`,`key_hash`, `data_hash`"
    insert_col_values = "`"+("`,`".join(keyhash_col))+"`,`"+("`,`".join(datahash_col))+"`"
    insert_values = f"""'Y','{current_time}','9999-12-31 00:00:00.000000+00:00','{current_time}','{current_time}', {insert_col_values}, `key_hash`,`data_hash`"""
    
    staged_updates_1 = f"""
    SELECT {output_view_name}.key_hash as mergeKey0, {output_view_name}.*
	FROM {output_view_name}
    """
    
    staged_updates_2 = f"""
    SELECT NULL as mergeKey0, {output_view_name}.*
	FROM {output_view_name} JOIN {target_table_name} 
	ON 
		{output_view_name}.key_hash = {target_table_name}.key_hash
	 WHERE
		{target_table_name}.row_latest_ind = 'Y' AND NOT (
		{target_table_name}.data_hash <=> {output_view_name}.data_hash)
    """  
    
    staged_updates = f"""(
    {staged_updates_1}
    UNION ALL
    {staged_updates_2}
    ) staged_updates
    """
    
    
    sql = f"""
    MERGE INTO {target_table_name}
    USING {staged_updates}
    ON {target_table_name}.key_hash = mergeKey0 AND {target_table_name}.row_latest_ind = 'Y'
    WHEN MATCHED 
      AND {target_table_name}.row_latest_ind = 'Y' 
      AND NOT ({target_table_name}.data_hash <=> staged_updates.data_hash) 
    THEN 
      UPDATE SET row_latest_ind = 'N', row_valid_to_ts = '{current_time}', dw_update_ts = '{current_time}'
    WHEN NOT MATCHED 
    THEN
      INSERT ( {insert_col} ) 
      VALUES ({insert_values})
    """

    spark.sql(sql)

# COMMAND ----------

def SCD3(spark, df, target_table_name, target_partition_keys, key_cols, current_time):
  # create a temp view
  input_view_name = 'INPUT_VIEW'
  df.createOrReplaceTempView(input_view_name)
  
  # deal with columns
  col = [x.upper() for x in df.columns]
  datahash_col = [x for x in col if not x in set(key_cols.upper().split(","))]
  datahash_col_old = [x+'_OLD' for x in col if not x in set(key_cols.upper().split(","))]
  keyhash_col = [x for x in col if x in set(key_cols.upper().split(","))]
  
  # deal with table name
  schema_name = target_table_name[:target_table_name.find('.')]
  tgt_tbl_name_without_schema = target_table_name[target_table_name.find('.')+1:]  
  
  # create a view with key_hash and data_hash 
  sql = f"""
  select a.*, 
    md5(concat(COALESCE( {",''), COALESCE(".join(keyhash_col)} ,''))) as key_hash,
    md5(concat(COALESCE( {",''), COALESCE(".join(datahash_col)} ,''))) as data_hash
    from {input_view_name} a
  """

  out_df = spark.sql(sql)
  output_view_name = 'OUTPUT_VIEW'
  out_df.createOrReplaceTempView(output_view_name);

  # create the target table with query if the table not existing
  if not spark._jsparkSession.catalog().tableExists(schema_name, tgt_tbl_name_without_schema):
    sql = f"""
    CREATE TABLE {target_table_name} 
    USING DELTA 
    PARTITIONED BY ({",".join(target_partition_keys)}) 
    AS select a.*,
    {"'' AS " +", '' AS ".join(datahash_col_old)},
    'N' as HAS_OLD_VERSION,
    cast('{current_time}' as timestamp)  as dw_insert_ts, 
    cast('{current_time}' as timestamp) as dw_update_ts
    from {output_view_name} a """

    spark.sql(sql)
  # merge the new/changed data into target table
  else:
    # deal with new data
    insert_col = "dw_insert_ts, dw_update_ts, HAS_OLD_VERSION, `"+("`,`".join(keyhash_col))+"`,`"+("`,`".join(datahash_col))+"`,`"+("`,`".join(datahash_col_old))+"`"+",`key_hash`, `data_hash`"
    insert_col_values = "`"+("`,`".join(keyhash_col))+"`,`"+("`,`".join(datahash_col))+"`"
    insert_old_col_values = ", ".join(["''" for set in list(dict.fromkeys(datahash_col_old))])
    insert_values = f"""'{current_time}','{current_time}', 'N', {insert_col_values}, {insert_old_col_values}, `key_hash`,`data_hash`"""
    
    # deal with changed data
    update_hash_values = f"{target_table_name}.key_hash = {output_view_name}.key_hash, \n{target_table_name}.data_hash = {output_view_name}.data_hash"
    update_key_values = ", \n".join([f"{target_table_name}.{set} = {output_view_name}.{set}" for set in list(dict.fromkeys(keyhash_col))])
    update_col_values = ", \n".join([f"{target_table_name}.{set} = {output_view_name}.{set}" for set in list(dict.fromkeys(datahash_col))])
    update_old_col_values = ", \n".join([f"{target_table_name}.{set}_OLD = {target_table_name}.{set}" for set in list(dict.fromkeys(datahash_col))])


    sql = f"""
    MERGE INTO {target_table_name}
    USING {output_view_name}
    ON {target_table_name}.key_hash <=> {output_view_name}.key_hash
    WHEN MATCHED AND {target_table_name}.data_hash != {output_view_name}.data_hash THEN
    UPDATE SET 
    dw_update_ts = '{current_time}', 
    HAS_OLD_VERSION = 'Y',
    {update_hash_values}, 
    {update_key_values}, 
    {update_old_col_values},
    {update_col_values}
    WHEN NOT MATCHED THEN
    INSERT ( {insert_col} ) 
    VALUES ({insert_values})
    """

    spark.sql(sql)
