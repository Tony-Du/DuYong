

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
alter table temp.stg_fact_kesheng_sdk_new_device_hourly_01_dy add if not exists partition(src_file_day='20170316', src_file_hour='19');

load data local inpath '/home/hadoop/temp/test_data_dy/channel_test.txt' overwrite into table temp.stg_fact_kesheng_sdk_new_device_hourly_01_dy
partition( src_file_day = '20170316', src_file_hour = '20');


011 channel01 001 v1 12345678
012 channel01 001 v1 12345678
011 channel02 001 v1 12345679
011 channel03 001 v1 12345680
012 channel02 001 v1 12345644

------------------------------------------
create table temp.stg_fact_kesheng_sdk_new_device_hourly_02_dy(
device_key bigint,
app_channel_id string,
product_key bigint,
app_ver_code string,
upload_unix_time bigint,
row_num bigint,
src_file_day string,
src_file_hour string
)
partitioned by (grain_ind string);


-- 000
INSERT OVERWRITE TABLE temp.stg_fact_kesheng_sdk_new_device_hourly_02_dy PARTITION(grain_ind = '000')
select
device_key,
app_channel_id,
product_key,
app_ver_code,
upload_unix_time,
row_number() over(partition by device_key order by upload_unix_time) row_num,
src_file_day,
src_file_hour
-- '000' grain_ind
from temp.stg_fact_kesheng_sdk_new_device_hourly_01_dy;

+---------------+-------------------+----------------+-----------------+---------------------+------------+-----------------+------------------+--------------+--+
| t.device_key  | t.app_channel_id  | t.product_key  | t.app_ver_code  | t.upload_unix_time  | t.row_num  | t.src_file_day  | t.src_file_hour  | t.grain_ind  |
+---------------+-------------------+----------------+-----------------+---------------------+------------+-----------------+------------------+--------------+--+
| 11            | channel01         | 1              | v1              | 12345678            | 1          | 20170316        | 20               | 0            |
| 11            | channel02         | 1              | v1              | 12345679            | 2          | 20170316        | 20               | 0            |
| 11            | channel03         | 1              | v1              | 12345680            | 3          | 20170316        | 20               | 0            |
| 12            | channel02         | 1              | v1              | 12345644            | 1          | 20170316        | 20               | 0            |
| 12            | channel01         | 1              | v1              | 12345678            | 2          | 20170316        | 20               | 0            |
+---------------+-------------------+----------------+-----------------+---------------------+------------+-----------------+------------------+--------------+--+

-- 010
INSERT OVERWRITE TABLE temp.stg_fact_kesheng_sdk_new_device_hourly_02_dy PARTITION(grain_ind = '010')
select
device_key,
app_channel_id,
product_key,
app_ver_code,
upload_unix_time,
row_number() over(partition by device_key, product_key order by upload_unix_time) row_num,
src_file_day,
src_file_hour
-- '010' grain_ind
from temp.stg_fact_kesheng_sdk_new_device_hourly_01_dy;
+---------------+-------------------+----------------+-----------------+---------------------+------------+-----------------+------------------+--------------+--+
| t.device_key  | t.app_channel_id  | t.product_key  | t.app_ver_code  | t.upload_unix_time  | t.row_num  | t.src_file_day  | t.src_file_hour  | t.grain_ind  |
+---------------+-------------------+----------------+-----------------+---------------------+------------+-----------------+------------------+--------------+--+
| 11            | channel01         | 1              | v1              | 12345678            | 1          | 20170316        | 20               | 10           |
| 11            | channel02         | 1              | v1              | 12345679            | 2          | 20170316        | 20               | 10           |
| 11            | channel03         | 1              | v1              | 12345680            | 3          | 20170316        | 20               | 10           |
| 12            | channel02         | 1              | v1              | 12345644            | 1          | 20170316        | 20               | 10           |
| 12            | channel01         | 1              | v1              | 12345678            | 2          | 20170316        | 20               | 10           |
+---------------+-------------------+----------------+-----------------+---------------------+------------+-----------------+------------------+--------------+--+

11	channel01	1	v1	12345678	1	20170316	20	010
11	channel02	1	v1	12345679	2	20170316	20	010
11	channel03	1	v1	12345680	3	20170316	20	010
12	channel02	1	v1	12345644	1	20170316	20	010
12	channel01	1	v1	12345678	2	20170316	20	010

-- 011
INSERT OVERWRITE TABLE temp.stg_fact_kesheng_sdk_new_device_hourly_02_dy PARTITION(grain_ind='011')
select
device_key,
app_channel_id,
product_key,
app_ver_code,
upload_unix_time,
row_number() over(partition by device_key, product_key, app_ver_code order by upload_unix_time) row_num,
src_file_day,
src_file_hour
--'011' grain_ind
from temp.stg_fact_kesheng_sdk_new_device_hourly_01_dy;
+---------------+-------------------+----------------+-----------------+---------------------+------------+-----------------+------------------+--------------+--+
| t.device_key  | t.app_channel_id  | t.product_key  | t.app_ver_code  | t.upload_unix_time  | t.row_num  | t.src_file_day  | t.src_file_hour  | t.grain_ind  |
+---------------+-------------------+----------------+-----------------+---------------------+------------+-----------------+------------------+--------------+--+
| 11            | channel01         | 1              | v1              | 12345678            | 1          | 20170316        | 20               | 11           |
| 11            | channel02         | 1              | v1              | 12345679            | 2          | 20170316        | 20               | 11           |
| 11            | channel03         | 1              | v1              | 12345680            | 3          | 20170316        | 20               | 11           |
| 12            | channel02         | 1              | v1              | 12345644            | 1          | 20170316        | 20               | 11           |
| 12            | channel01         | 1              | v1              | 12345678            | 2          | 20170316        | 20               | 11           |
+---------------+-------------------+----------------+-----------------+---------------------+------------+-----------------+------------------+--------------+--+
11	channel01	1	v1	12345678	1	20170316	20	011
11	channel02	1	v1	12345679	2	20170316	20	011
11	channel03	1	v1	12345680	3	20170316	20	011
12	channel02	1	v1	12345644	1	20170316	20	011
12	channel01	1	v1	12345678	2	20170316	20	011


-- 100
INSERT OVERWRITE TABLE temp.stg_fact_kesheng_sdk_new_device_hourly_02_dy PARTITION(grain_ind = '100')
select
device_key,
app_channel_id,
product_key,
app_ver_code,
upload_unix_time,
row_number() over(partition by device_key order by upload_unix_time) row_num,
src_file_day,
src_file_hour
--'100' grain_ind
from temp.stg_fact_kesheng_sdk_new_device_hourly_01_dy;
+---------------+-------------------+----------------+-----------------+---------------------+------------+-----------------+------------------+--------------+--+
| t.device_key  | t.app_channel_id  | t.product_key  | t.app_ver_code  | t.upload_unix_time  | t.row_num  | t.src_file_day  | t.src_file_hour  | t.grain_ind  |
+---------------+-------------------+----------------+-----------------+---------------------+------------+-----------------+------------------+--------------+--+
| 11            | channel01         | 1              | v1              | 12345678            | 1          | 20170316        | 20               | 100          |
| 11            | channel02         | 1              | v1              | 12345679            | 2          | 20170316        | 20               | 100          |
| 11            | channel03         | 1              | v1              | 12345680            | 3          | 20170316        | 20               | 100          |
| 12            | channel02         | 1              | v1              | 12345644            | 1          | 20170316        | 20               | 100          |
| 12            | channel01         | 1              | v1              | 12345678            | 2          | 20170316        | 20               | 100          |
+---------------+-------------------+----------------+-----------------+---------------------+------------+-----------------+------------------+--------------+--+
11	channel01	1	v1	12345678	1	20170316	20	100
11	channel02	1	v1	12345679	2	20170316	20	100
11	channel03	1	v1	12345680	3	20170316	20	100
12	channel02	1	v1	12345644	1	20170316	20	100
12	channel01	1	v1	12345678	2	20170316	20	100


-- 110
INSERT OVERWRITE TABLE temp.stg_fact_kesheng_sdk_new_device_hourly_02_dy PARTITION(grain_ind = '110')
select
device_key,
app_channel_id,
product_key,
app_ver_code,
upload_unix_time,
row_number() over(partition by device_key, app_channel_id, product_key order by upload_unix_time) row_num,
src_file_day,
src_file_hour
--'110' grain_ind
from temp.stg_fact_kesheng_sdk_new_device_hourly_01_dy;
+---------------+-------------------+----------------+-----------------+---------------------+------------+-----------------+------------------+--------------+--+
| t.device_key  | t.app_channel_id  | t.product_key  | t.app_ver_code  | t.upload_unix_time  | t.row_num  | t.src_file_day  | t.src_file_hour  | t.grain_ind  |
+---------------+-------------------+----------------+-----------------+---------------------+------------+-----------------+------------------+--------------+--+
| 11            | channel01         | 1              | v1              | 12345678            | 1          | 20170316        | 20               | 110          |
| 11            | channel02         | 1              | v1              | 12345679            | 1          | 20170316        | 20               | 110          |
| 11            | channel03         | 1              | v1              | 12345680            | 1          | 20170316        | 20               | 110          |
| 12            | channel01         | 1              | v1              | 12345678            | 1          | 20170316        | 20               | 110          |
| 12            | channel02         | 1              | v1              | 12345644            | 1          | 20170316        | 20               | 110          |
+---------------+-------------------+----------------+-----------------+---------------------+------------+-----------------+------------------+--------------+--+
11	channel01	1	v1	12345678	1	20170316	20	110
11	channel02	1	v1	12345679	1	20170316	20	110
11	channel03	1	v1	12345680	1	20170316	20	110
12	channel01	1	v1	12345678	1	20170316	20	110
12	channel02	1	v1	12345644	1	20170316	20	110


-- 111
INSERT OVERWRITE TABLE temp.stg_fact_kesheng_sdk_new_device_hourly_02_dy PARTITION(grain_ind = '111')
select
device_key,
app_channel_id,
product_key,
app_ver_code,
upload_unix_time,
row_number() over(partition by device_key, app_channel_id, product_key, app_ver_code order by upload_unix_time) row_num,
src_file_day,
src_file_hour
--'111' grain_ind
from temp.stg_fact_kesheng_sdk_new_device_hourly_01_dy;
+---------------+-------------------+----------------+-----------------+---------------------+------------+-----------------+------------------+--------------+--+
| t.device_key  | t.app_channel_id  | t.product_key  | t.app_ver_code  | t.upload_unix_time  | t.row_num  | t.src_file_day  | t.src_file_hour  | t.grain_ind  |
+---------------+-------------------+----------------+-----------------+---------------------+------------+-----------------+------------------+--------------+--+
| 11            | channel01         | 1              | v1              | 12345678            | 1          | 20170316        | 20               | 111          |
| 11            | channel02         | 1              | v1              | 12345679            | 1          | 20170316        | 20               | 111          |
| 11            | channel03         | 1              | v1              | 12345680            | 1          | 20170316        | 20               | 111          |
| 12            | channel01         | 1              | v1              | 12345678            | 1          | 20170316        | 20               | 111          |
| 12            | channel02         | 1              | v1              | 12345644            | 1          | 20170316        | 20               | 111          |
+---------------+-------------------+----------------+-----------------+---------------------+------------+-----------------+------------------+--------------+--+
11	channel01	1	v1	12345678	1	20170316	20	111
11	channel02	1	v1	12345679	1	20170316	20	111
11	channel03	1	v1	12345680	1	20170316	20	111
12	channel01	1	v1	12345678	1	20170316	20	111
12	channel02	1	v1	12345644	1	20170316	20	111


----------------------------------------------------------------------------------

create table temp.rptdata_fact_kesheng_sdk_new_device_hourly_dy (
device_key bigint,
app_channel_id string,
product_key bigint,
app_ver_code string,
upload_unix_time bigint,
new_cnt bigint,
become_new_unix_time bigint,
become_new_dw_day string
)
partitioned by (src_file_day string, src_file_hour string, grain_ind string);



INSERT OVERWRITE TABLE temp.rptdata_fact_kesheng_sdk_new_device_hourly_dy PARTITION(src_file_day=20170316, src_file_hour=20, grain_ind = '000')
select
t1.device_key,
t1.app_channel_id,
t1.product_key,
t1.app_ver_code,
t1.upload_unix_time,
case when t2.device_key is null and row_num = 1 then 1 else 0 end new_cnt,
t2.become_new_unix_time,
case when t2.device_key is null
then concat_ws('-', substr('20170316', 1, 4), substr('20170316', 5, 2), substr('20170316', 7, 2))
else t2.become_new_dw_day end
from temp.stg_fact_kesheng_sdk_new_device_hourly_02_dy t1
left join (select device_key,
max(case when new_cnt = 1 then upload_unix_time else null end) become_new_unix_time,
max(become_new_dw_day) become_new_dw_day
from temp.rptdata_fact_kesheng_sdk_new_device_hourly_dy
where grain_ind = '000'
and (src_file_day >= translate(add_months(concat_ws('-', substr('20170316', 1, 4), substr('20170316', 5, 2), '01'), -6), '-', '')
and src_file_day < '20170316'
or src_file_day = '20170316' and src_file_hour < '20'
)
group by device_key) t2
on (t1.device_key = t2.device_key)
where t1.grain_ind = '000';
+---------------+-------------------+----------------+-----------------+---------------------+------------+-------------------------+----------------------+-----------------+------------------+--------------+--+
| t.device_key  | t.app_channel_id  | t.product_key  | t.app_ver_code  | t.upload_unix_time  | t.new_cnt  | t.become_new_unix_time  | t.become_new_dw_day  | t.src_file_day  | t.src_file_hour  | t.grain_ind  |
+---------------+-------------------+----------------+-----------------+---------------------+------------+-------------------------+----------------------+-----------------+------------------+--------------+--+
| 11            | channel01         | 1              | v1              | 12345678            | 1          | NULL                    | 2017-03-16           | 20170316        | 20               | 000          |
| 11            | channel02         | 1              | v1              | 12345679            | 0          | NULL                    | 2017-03-16           | 20170316        | 20               | 000          |
| 11            | channel03         | 1              | v1              | 12345680            | 0          | NULL                    | 2017-03-16           | 20170316        | 20               | 000          |
| 12            | channel02         | 1              | v1              | 12345644            | 1          | NULL                    | 2017-03-16           | 20170316        | 20               | 000          |
| 12            | channel01         | 1              | v1              | 12345678            | 0          | NULL                    | 2017-03-16           | 20170316        | 20               | 000          |
+---------------+-------------------+----------------+-----------------+---------------------+------------+-------------------------+----------------------+-----------------+------------------+--------------+--+

select device_key,
max(case when new_cnt = 1 then upload_unix_time else null end) become_new_unix_time,
max(become_new_dw_day) become_new_dw_day
from temp.rptdata_fact_kesheng_sdk_new_device_hourly_dy
where grain_ind = '000'
and (src_file_day >= translate(add_months(concat_ws('-', substr('20170316', 1, 4), substr('20170316', 5, 2), '01'), -6), '-', '')
and src_file_day < '20170316'
or src_file_day = '20170316' and src_file_hour < '20'
)
group by device_key;
+-------------+-----------------------+--------------------+--+
| device_key  | become_new_unix_time  | become_new_dw_day  |
+-------------+-----------------------+--------------------+--+
+-------------+-----------------------+--------------------+--+


INSERT OVERWRITE TABLE temp.rptdata_fact_kesheng_sdk_new_device_hourly_dy PARTITION(src_file_day=20170316, src_file_hour=20, grain_ind = '010')
select
t1.device_key,
t1.app_channel_id,
t1.product_key,
t1.app_ver_code,
t1.upload_unix_time,
case when t2.device_key is null and row_num = 1 then 1 else 0 end new_cnt,
t2.become_new_unix_time,
case when t2.device_key is null
then concat_ws('-', substr('20170316', 1, 4), substr('20170316', 5, 2), substr('20170316', 7, 2))
else t2.become_new_dw_day end
from temp.stg_fact_kesheng_sdk_new_device_hourly_02_dy t1
left join (select product_key, device_key,
max(case when new_cnt = 1 then upload_unix_time else null end) become_new_unix_time,
max(become_new_dw_day) become_new_dw_day
from temp.rptdata_fact_kesheng_sdk_new_device_hourly_dy
where grain_ind = '010'
and (src_file_day >= translate(add_months(concat_ws('-', substr('20170316', 1, 4), substr('20170316', 5, 2), '01'), -6), '-', '')
and src_file_day < '20170316'
or src_file_day = '20170316' and src_file_hour < '20'
)
group by device_key, product_key) t2
on (t1.device_key = t2.device_key and t1.product_key = t2.product_key)
where t1.grain_ind = '010';
+---------------+-------------------+----------------+-----------------+---------------------+------------+-------------------------+----------------------+-----------------+------------------+--------------+--+
| t.device_key  | t.app_channel_id  | t.product_key  | t.app_ver_code  | t.upload_unix_time  | t.new_cnt  | t.become_new_unix_time  | t.become_new_dw_day  | t.src_file_day  | t.src_file_hour  | t.grain_ind  |
+---------------+-------------------+----------------+-----------------+---------------------+------------+-------------------------+----------------------+-----------------+------------------+--------------+--+
| 11            | channel01         | 1              | v1              | 12345678            | 1          | NULL                    | 2017-03-16           | 20170316        | 20               | 010          |
| 11            | channel02         | 1              | v1              | 12345679            | 0          | NULL                    | 2017-03-16           | 20170316        | 20               | 010          |
| 11            | channel03         | 1              | v1              | 12345680            | 0          | NULL                    | 2017-03-16           | 20170316        | 20               | 010          |
| 12            | channel02         | 1              | v1              | 12345644            | 1          | NULL                    | 2017-03-16           | 20170316        | 20               | 010          |
| 12            | channel01         | 1              | v1              | 12345678            | 0          | NULL                    | 2017-03-16           | 20170316        | 20               | 010          |
+---------------+-------------------+----------------+-----------------+---------------------+------------+-------------------------+----------------------+-----------------+------------------+--------------+--+


INSERT OVERWRITE TABLE temp.rptdata_fact_kesheng_sdk_new_device_hourly_dy PARTITION(src_file_day ='20170316', src_file_hour='20', grain_ind='111')
select
t1.device_key,
t1.app_channel_id,
t1.product_key,
t1.app_ver_code,
t1.upload_unix_time,
case when t2.device_key is null and row_num = 1 then 1 else 0 end new_cnt,
t2.become_new_unix_time,
case when t2.device_key is null
then concat_ws('-', substr('20170316', 1, 4), substr('20170316', 5, 2), substr('20170316', 7, 2))
else t2.become_new_dw_day end
from temp.stg_fact_kesheng_sdk_new_device_hourly_02_dy t1
left join (select app_channel_id,
product_key,
app_ver_code,
device_key,
max(case when new_cnt = 1 then upload_unix_time else null end) become_new_unix_time,
max(become_new_dw_day) become_new_dw_day
from temp.rptdata_fact_kesheng_sdk_new_device_hourly_dy
where grain_ind = '111'
and (src_file_day >= translate(add_months(concat_ws('-', substr('20170316', 1, 4), substr('20170316', 5, 2), '01'), -6), '-', '')
and src_file_day < '20170316'
or src_file_day = '20170316' and src_file_hour < '20'
)
group by app_channel_id,
product_key,
app_ver_code,
device_key) t2
on (t1.device_key = t2.device_key and t1.app_channel_id = t2.app_channel_id and t1.product_key = t2.product_key and t1.app_ver_code = t2.app_ver_code)
where t1.grain_ind = '111';
+---------------+-------------------+----------------+-----------------+---------------------+------------+-------------------------+----------------------+-----------------+------------------+--------------+--+
| t.device_key  | t.app_channel_id  | t.product_key  | t.app_ver_code  | t.upload_unix_time  | t.new_cnt  | t.become_new_unix_time  | t.become_new_dw_day  | t.src_file_day  | t.src_file_hour  | t.grain_ind  |
+---------------+-------------------+----------------+-----------------+---------------------+------------+-------------------------+----------------------+-----------------+------------------+--------------+--+
| 11            | channel01         | 1              | v1              | 12345678            | 1          | NULL                    | 2017-03-16           | 20170316        | 20               | 111          |
| 11            | channel02         | 1              | v1              | 12345679            | 1          | NULL                    | 2017-03-16           | 20170316        | 20               | 111          |
| 11            | channel03         | 1              | v1              | 12345680            | 1          | NULL                    | 2017-03-16           | 20170316        | 20               | 111          |
| 12            | channel01         | 1              | v1              | 12345678            | 1          | NULL                    | 2017-03-16           | 20170316        | 20               | 111          |
| 12            | channel02         | 1              | v1              | 12345644            | 1          | NULL                    | 2017-03-16           | 20170316        | 20               | 111          |
+---------------+-------------------+----------------+-----------------+---------------------+------------+-------------------------+----------------------+-----------------+------------------+--------------+--+