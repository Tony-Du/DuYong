
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

--  ========================================================================== --
set mapreduce.job.name=app.cpa_sec_event_occur_daily_ex_${SRC_FILE_DAY};
set hive.merge.mapredfiles=true;


insert into app.cpa_sec_event_occur_daily_ex
select concat_ws(string(unhex('1F')), t1.src_file_day, t1.product_name
                ,t1.app_ver_code, t1.app_channel_id, t1.event_name) rowkey
      ,map('event_cnt', t1.event_cnt, 'sum_du', t1.sum_du, 'avg_du', t1.avg_du) as idx
  from app.cpa_sec_event_occur_daily t1
 where t1.src_file_day='${SRC_FILE_DAY}';

select * from temp.cpa_sec_event_occur_daily_ex;

20|咪咕视频|1.3|1230001|en1	{"avg_du":5.3,"sum_du":2.43}
20|咪咕视频|1.3|1230001|en2	{"avg_du":2.08,"sum_du":8.4}
20|咪咕视频|1.3|1230002|en2	{"avg_du":0.98,"sum_du":4.66}
20|咪咕视频|1.3|2230001|en3	{"avg_du":43.8,"sum_du":12.9}
20|咪咕视频|1.3|2230002|en4	{"avg_du":93.8,"sum_du":23.8}

scan 'cpa_sec_event_occur_day'

 20|\xE5\x92\xAA\xE5\x92\x95\xE8\xA7\x86\xE9\xA2 column=idx:avg_du, timestamp=1489482390443, value=5.3                                                                                         
 \x91|1.3|1230001|en1                                                                                                                                                                          
 20|\xE5\x92\xAA\xE5\x92\x95\xE8\xA7\x86\xE9\xA2 column=idx:sum_du, timestamp=1489482390443, value=2.43                                                                                        
 \x91|1.3|1230001|en1                                                                                                                                                                          
 20|\xE5\x92\xAA\xE5\x92\x95\xE8\xA7\x86\xE9\xA2 column=idx:avg_du, timestamp=1489482390443, value=2.08                                                                                        
 \x91|1.3|1230001|en2                                                                                                                                                                          
 20|\xE5\x92\xAA\xE5\x92\x95\xE8\xA7\x86\xE9\xA2 column=idx:sum_du, timestamp=1489482390443, value=8.4                                                                                         
 \x91|1.3|1230001|en2                                                                                                                                                                          
 20|\xE5\x92\xAA\xE5\x92\x95\xE8\xA7\x86\xE9\xA2 column=idx:avg_du, timestamp=1489482390443, value=0.98                                                                                        
 \x91|1.3|1230002|en2                                                                                                                                                                          
 20|\xE5\x92\xAA\xE5\x92\x95\xE8\xA7\x86\xE9\xA2 column=idx:sum_du, timestamp=1489482390443, value=4.66                                                                                        
 \x91|1.3|1230002|en2                                                                                                                                                                          
 20|\xE5\x92\xAA\xE5\x92\x95\xE8\xA7\x86\xE9\xA2 column=idx:avg_du, timestamp=1489479960568, value=43.8                                                                                        
 \x91|1.3|2230001|en3                                                                                                                                                                          
 20|\xE5\x92\xAA\xE5\x92\x95\xE8\xA7\x86\xE9\xA2 column=idx:sum_du, timestamp=1489479878187, value=12.9                                                                                        
 \x91|1.3|2230001|en3                                                                                                                                                                          
 20|\xE5\x92\xAA\xE5\x92\x95\xE8\xA7\x86\xE9\xA2 column=idx:avg_du, timestamp=1489479975085, value=93.8                                                                                        
 \x91|1.3|2230002|en4                                                                                                                                                                          
 20|\xE5\x92\xAA\xE5\x92\x95\xE8\xA7\x86\xE9\xA2 column=idx:sum_du, timestamp=1489479967561, value=23.8                                                                                        
 \x91|1.3|2230002|en4   

-- ===================================================================================================== --
 
drop table if exists app.cpa_sec_event_params_daily_ex;

create external table app.cpa_sec_event_params_daily_ex(
  rowkey string , 
  param_info map<string,array<string>> )
stored by 
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
with serdeproperties ( 
  'hbase.columns.mapping'=':key,param_info:')
tblproperties ('hbase.table.name'='cpa_sec_event_params_daily');

/*----------

-- hbase对应创建表语句
create 'cpa_sec_event_params_daily','param_info'

-----------*/

-- =======================================================================
set mapreduce.job.name=app.cpa_sec_event_params_daily_ex_${SRC_FILE_DAY};
set hive.merge.mapredfiles=true;

insert into app.cpa_sec_event_params_daily_ex
select t1.rowkey
      ,map('pset',collect_set(param_info)) as param_info
  from (select concat_ws(string(unhex('1F')), a.src_file_day, a.product_name
                        ,a.app_ver_code, a.app_channel_id, a.event_name) rowkey
              ,concat_ws(string(unhex('1F')), a.param_name, a.param_val
                        ,string(a.val_cnt), string(a.val_pct)) as param_info
          from app.cpa_sec_event_params_daily a
         where a.src_file_day='${SRC_FILE_DAY}'
       ) t1
 group by t1.rowkey;

select * from temp.cpa_sec_event_params_daily_ex;

10|咪咕直播|1.3|1230001|en1	{"pset":["p2|15|33|8.3","p1|19|78|7.3","p1|20|56|3.3","p2|15|21|8.3","p1|19|76|4.3","p1|20|45|2.3","p1|20|46|3.3"]}
10|咪咕直播|1.3|1230003|en1	{"pset":["p1|20|47|4.3"]}
20|咪咕视频|1.3|1230001|en1	{"pset":["p1|14|56|2.3","p1|14|232|2.3"]}
20|咪咕视频|1.3|1230001|en2	{"pset":["p2|19|632|4.3","p2|19|36|4.3"]}
20|咪咕视频|1.3|1230002|en2	{"pset":["p2|15|78|2.3","p2|15|90|2.3","p2|15|932|2.3"]}


scan 'cpa_sec_event_params_daily'

 10|\xE5\x92\xAA\xE5\x92\x95\xE7\x9B\xB4\xE6\x92 column=param_info:pset, timestamp=1489485916008, value=p2|15|33|8.3\x04p1|19|78|7.3\x04p1|20|56|3.3\x04p2|15|21|8.3\x04p1|19|76|4.3\x04p1|20|4
 \xAD|1.3|1230001|en1                            5|2.3\x04p1|20|46|3.3                                                                                                                         
 10|\xE5\x92\xAA\xE5\x92\x95\xE7\x9B\xB4\xE6\x92 column=param_info:pset, timestamp=1489485916008, value=p1|20|47|4.3                                                                           
 \xAD|1.3|1230003|en1                                                                                                                                                                          
 20|\xE5\x92\xAA\xE5\x92\x95\xE8\xA7\x86\xE9\xA2 column=param_info:pset, timestamp=1489485916008, value=p1|14|56|2.3\x04p1|14|232|2.3                                                          
 \x91|1.3|1230001|en1                                                                                                                                                                          
 20|\xE5\x92\xAA\xE5\x92\x95\xE8\xA7\x86\xE9\xA2 column=param_info:pset, timestamp=1489485916008, value=p2|19|632|4.3\x04p2|19|36|4.3                                                          
 \x91|1.3|1230001|en2                                                                                                                                                                          
 20|\xE5\x92\xAA\xE5\x92\x95\xE8\xA7\x86\xE9\xA2 column=param_info:pset, timestamp=1489485916008, value=p2|15|78|2.3\x04p2|15|90|2.3\x04p2|15|932|2.3                                          
 \x91|1.3|1230002|en2 