
create table app.cpa_sec_event_occur_daily (	-- HBase中不存在这个表，会自动创建
key              string, 
product_key      string,
product_name     string,
app_ver_code     string,
app_channel_id   string,
event_name       string,
event_cnt        bigint,
sum_du           decimal(20,2),
avg_du           decimal(20,2)
)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
with serdeproperties ("hbase.columns.mapping" = ":key, cf1:product_key, cf1:product_name, 
cf1:app_ver_code, cf1:app_channel_id, cf1:event_name, cf1:event_cnt, cf1:sum_du, cf1:avg_du")
tblproperties ("hbase.table.name" = "app.cpa_sec_event_occur_daily");

--------------------------------------------------------------------------------
create external table app.cpa_sec_event_occur_daily (	-- HBase中已存在这个表
key              string, 
product_key      string,
product_name     string,
app_ver_code     string,
app_channel_id   string,
event_name       string,
event_cnt        bigint,
sum_du           decimal(20,2),
avg_du           decimal(20,2)
)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
with serdeproperties ("hbase.columns.mapping" = "cf1:val")	-- ？？？？
tblproperties ("hbase.table.name" = "app.cpa_sec_event_occur_daily");



