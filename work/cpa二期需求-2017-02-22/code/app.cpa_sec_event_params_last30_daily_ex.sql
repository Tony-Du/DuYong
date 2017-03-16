

drop table if exists app.cpa_sec_event_params_last30_daily_ex;

create external table app.cpa_sec_event_params_last30_daily_ex(
  rowkey string , 
  param_info map<string,array<string>> )
stored by 
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
with serdeproperties ( 
  'hbase.columns.mapping'=':key,param_info:')
tblproperties ('hbase.table.name'='cpa_sec_event_params_last30_daily');

/*----------

-- hbase对应创建表语句
create 'cpa_sec_event_params_last30_daily','param_info'

-----------*/

-- =================================================================== --

set mapreduce.job.name=app.cpa_sec_event_params_last30_daily_ex_${SRC_FILE_DAY};
set hive.merge.mapredfiles=true;

insert into app.cpa_sec_event_params_last30_daily_ex
select t1.rowkey
      ,map('pset',collect_set(param_info)) as param_info
  from (select concat_ws(string(unhex('1F')), a.src_file_day, a.product_name
                        ,a.app_ver_code, a.app_channel_id, a.event_name) rowkey
              ,concat_ws(string(unhex('1F')), a.param_name, a.param_val
                        ,string(a.val_cnt), string(a.val_pct)) as param_info
          from app.cpa_sec_event_params_last30_daily a
         where a.src_file_day = '${SRC_FILE_DAY}' 
       ) t1
 group by t1.rowkey;