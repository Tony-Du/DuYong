



create table temp.stg_fact_kesheng_sdk_new_device_hourly_01_dy
(
device_key bigint,
app_channel_id string,
product_key bigint,
app_ver_code string,
upload_unix_time bigint
)
partitioned by (src_file_day string, src_file_hour string)
row format delimited fields terminated by ' ';

alter table temp.stg_fact_kesheng_sdk_new_device_hourly_01_dy add if not exists partition(src_file_day='20170316', src_file_hour='20');


load data local inpath '/home/hadoop/temp/test_data_dy/channel_test.txt' overwrite into table temp.stg_fact_kesheng_sdk_new_device_hourly_01_dy 
partition( src_file_day = '20170316', src_file_hour = '20');


011 channel01 001 v1 12345678
012 channel01 001 v1 12345678
011 channel02 001 v1 12345679
011 channel03 001 v1 12345680
012 channel02 001 v1 12345644

-- 000
select
  device_key,
  app_channel_id,
  product_key,
  app_ver_code,
  upload_unix_time,
  row_number() over(partition by device_key order by upload_unix_time) row_num,
  src_file_day,
  src_file_hour,
  '000' grain_ind		
from temp.stg_fact_kesheng_sdk_new_device_hourly_01_dy; 

11	channel01	1	v1	12345678	1	20170316	20	000                               011 channel01 001 v1 12345678
11	channel02	1	v1	12345679	2	20170316	20	000                               012 channel01 001 v1 12345678
11	channel03	1	v1	12345680	3	20170316	20	000                               011 channel02 001 v1 12345679
12	channel02	1	v1	12345644	1	20170316	20	000                               011 channel03 001 v1 12345680
12	channel01	1	v1	12345678	2	20170316	20	000                               012 channel02 001 v1 12345644

-- 010
select
device_key,
app_channel_id,
product_key,
app_ver_code,
upload_unix_time,
row_number() over(partition by device_key, product_key order by upload_unix_time) row_num,
src_file_day,
src_file_hour,
'010' grain_ind
from temp.stg_fact_kesheng_sdk_new_device_hourly_01_dy;

11	channel01	1	v1	12345678	1	20170316	20	010
11	channel02	1	v1	12345679	2	20170316	20	010
11	channel03	1	v1	12345680	3	20170316	20	010
12	channel02	1	v1	12345644	1	20170316	20	010
12	channel01	1	v1	12345678	2	20170316	20	010

-- 011
select
device_key,
app_channel_id,
product_key,
app_ver_code,
upload_unix_time,
row_number() over(partition by device_key, product_key, app_ver_code order by upload_unix_time) row_num,
src_file_day,
src_file_hour,
'011' grain_ind
from temp.stg_fact_kesheng_sdk_new_device_hourly_01_dy;
11	channel01	1	v1	12345678	1	20170316	20	011
11	channel02	1	v1	12345679	2	20170316	20	011
11	channel03	1	v1	12345680	3	20170316	20	011
12	channel02	1	v1	12345644	1	20170316	20	011
12	channel01	1	v1	12345678	2	20170316	20	011


-- 100
select
  device_key,
  app_channel_id,
  product_key,
  app_ver_code,
  upload_unix_time,
  row_number() over(partition by device_key order by upload_unix_time) row_num,
  src_file_day,
  src_file_hour,
  '100' grain_ind
from temp.stg_fact_kesheng_sdk_new_device_hourly_01_dy;

11	channel01	1	v1	12345678	1	20170316	20	100
11	channel02	1	v1	12345679	2	20170316	20	100
11	channel03	1	v1	12345680	3	20170316	20	100
12	channel02	1	v1	12345644	1	20170316	20	100
12	channel01	1	v1	12345678	2	20170316	20	100


-- 110
select
  device_key,
  app_channel_id,
  product_key,
  app_ver_code,
  upload_unix_time,
  row_number() over(partition by device_key, app_channel_id, product_key order by upload_unix_time) row_num,
  src_file_day,
  src_file_hour,
  '110' grain_ind
from temp.stg_fact_kesheng_sdk_new_device_hourly_01_dy;

11	channel01	1	v1	12345678	1	20170316	20	110
11	channel02	1	v1	12345679	1	20170316	20	110
11	channel03	1	v1	12345680	1	20170316	20	110
12	channel01	1	v1	12345678	1	20170316	20	110
12	channel02	1	v1	12345644	1	20170316	20	110


-- 111
select
device_key,
app_channel_id,
product_key,
app_ver_code,
upload_unix_time,
row_number() over(partition by device_key, app_channel_id, product_key, app_ver_code order by upload_unix_time) row_num,
src_file_day,
src_file_hour,
'111' grain_ind
from temp.stg_fact_kesheng_sdk_new_device_hourly_01_dy;
11	channel01	1	v1	12345678	1	20170316	20	111
11	channel02	1	v1	12345679	1	20170316	20	111
11	channel03	1	v1	12345680	1	20170316	20	111
12	channel01	1	v1	12345678	1	20170316	20	111
12	channel02	1	v1	12345644	1	20170316	20	111