create table stg.bangdanzui2
(
src_file_day         string,
app_pkg_name        string,
app_ver_code        string,
duration            string,
cc                   string
)

insert overwrite table stg.bangdanzui2
select 
src_file_day,
app_pkg_name,
app_ver_code,
sum(duration_ms)/1000 as duration,
count(*) as cc
from intdata.kesheng_sdk_session_end
where src_file_day between '20170501' and '20170518'
and duration_ms<15000000
group by
src_file_day,
app_pkg_name,
app_ver_code ;


hdfs dfs -getmerge hdfs://ns1/user/hive/warehouse/stg.db/bangdanzui2/*  /home/hadoop/test2/test


cat /home/hadoop/test2/test |sed "s/^/\"/g;s/$/\"/g;s/\x01/\"\,\"/g"> filename.csv





