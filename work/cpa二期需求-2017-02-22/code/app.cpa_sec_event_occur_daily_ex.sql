
drop table if exists app.cpa_sec_event_occur_daily_ex;

create external table app.cpa_sec_event_occur_daily_ex(
  rowkey string,
  idx map<string,double> )
stored by 
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
with serdeproperties ( 
  'hbase.columns.mapping'=':key,idx:')
tblproperties ('hbase.table.name'='cpa_sec_event_occur_daily');

/*----------

-- hbase对应创建表语句
create 'cpa_sec_event_occur_daily','idx'

-----------*/

-- ========================================================================== --
set mapreduce.job.name=app.cpa_sec_event_occur_daily_ex_${SRC_FILE_DAY};
set hive.merge.mapredfiles=true;


insert into app.cpa_sec_event_occur_daily_ex
select concat_ws(string(unhex('1F')), t1.src_file_day, t1.product_name
                ,t1.app_ver_code, t1.app_channel_id, t1.event_name) rowkey
      ,map('event_cnt', t1.event_cnt, 'sum_du', t1.sum_du, 'avg_du', t1.avg_du) as idx
  from app.cpa_sec_event_occur_daily t1
 where t1.src_file_day='${SRC_FILE_DAY}';


-- hive 默认数组分隔符ASCII 04

-- collect_set(param_info): 返回param_info元素剔重后的数组

-- hex：可以用HEX()函数将一个字符串或数字转换为十六进制格式的字符串

-- unhex：把十六进制格式的字符串转化为原来的格式
