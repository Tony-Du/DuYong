alter table test.probe_system_test_ex add if not exists partition (src_file_day='20171020');


==1==黑屏链接商场wifi========
 
select phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 11:45:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 11:35:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8'   ) --华为  
               ) t
         where t.time_stamp between '20171020 11:35:00' and '20171020 11:45:00' 
         group by t.phone_mac
       ) tt
;


==2==黑屏链接商场wifi========
 
select phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 11:58:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 11:47:32','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8'   ) --华为  
               ) t
         where t.time_stamp between '20171020 11:47:32' and '20171020 11:58:00' 
         group by t.phone_mac
       ) tt
;



==3==黑屏链接商场wifi========
 
select phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 12:10:31','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 12:01:03','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8'   ) --华为  
               ) t
         where t.time_stamp between '20171020 12:01:03' and '20171020 12:10:31' 
         group by t.phone_mac
       ) tt
;




==4==黑屏链接商场wifi========
 
select phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 12:22:24','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 12:12:12','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8'   ) --华为  
               ) t
         where t.time_stamp between '20171020 12:12:12' and '20171020 12:22:24' 
         group by t.phone_mac
       ) tt
;


==5==黑屏链接商场wifi========
 
select phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 12:35:28','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 12:25:03','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8'   ) --华为  
               ) t
         where t.time_stamp between '20171020 12:25:03' and '20171020 12:35:28' 
         group by t.phone_mac
       ) tt
;




==6==黑屏链接商场wifi========
 
select phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 12:49:11','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 12:38:36','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8'   ) --华为  
               ) t
         where t.time_stamp between '20171020 12:38:36' and '20171020 12:49:11' 
         group by t.phone_mac
       ) tt
;



==7==黑屏链接商场wifi========
 
select phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 13:01:57','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 12:51:20','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8'   ) --华为  
               ) t
         where t.time_stamp between '20171020 12:51:20' and '20171020 13:01:57' 
         group by t.phone_mac
       ) tt
;



==8==黑屏链接商场wifi========

 select phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 13:13:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 13:03:58','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8'   ) --华为  
               ) t
         where t.time_stamp between '20171020 13:03:58' and '20171020 13:13:00' 
         group by t.phone_mac
       ) tt
;







因为iphone mac 徐文娣给错了(给错的:c0ccf8ef75c0，现在正确的:c0ccf8ef75bf)，所以要重新算iphone mac 的探针测试情况（20171031）

====黑屏链接商场wifi========
 
select 1 as distance
      ,phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 11:45:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 11:35:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac = 'c0ccf8ef75bf'  
               ) t
         where t.time_stamp between '20171020 11:35:00' and '20171020 11:45:00' 
         group by t.phone_mac
       ) tt

union all
       
 select 2 as distance
      ,phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 11:58:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 11:47:32','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                  and phone_mac = 'c0ccf8ef75bf'  
               ) t
         where t.time_stamp between '20171020 11:47:32' and '20171020 11:58:00' 
         group by t.phone_mac
       ) tt

union all
       
 select 3 as distance
      ,phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 12:10:31','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 12:01:03','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                  and phone_mac = 'c0ccf8ef75bf'  
               ) t
         where t.time_stamp between '20171020 12:01:03' and '20171020 12:10:31' 
         group by t.phone_mac
       ) tt

union all
       
 select 4 as distance
      ,phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 12:22:24','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 12:12:12','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
               and phone_mac = 'c0ccf8ef75bf'  
               ) t
         where t.time_stamp between '20171020 12:12:12' and '20171020 12:22:24' 
         group by t.phone_mac
       ) tt

union all
       
 select 5 as distance
      ,phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 12:35:28','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 12:25:03','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
           and phone_mac = 'c0ccf8ef75bf'  
               ) t
         where t.time_stamp between '20171020 12:25:03' and '20171020 12:35:28' 
         group by t.phone_mac
       ) tt

union all
       
 select 6 as distance
      ,phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 12:49:11','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 12:38:36','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                and phone_mac = 'c0ccf8ef75bf'  
               ) t
         where t.time_stamp between '20171020 12:38:36' and '20171020 12:49:11' 
         group by t.phone_mac
       ) tt

union all
       
 select 7 as distance
      ,phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 13:01:57','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 12:51:20','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac = 'c0ccf8ef75bf'  
               ) t
         where t.time_stamp between '20171020 12:51:20' and '20171020 13:01:57' 
         group by t.phone_mac
       ) tt

union all
       
 select 8 as distance
      ,phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 13:13:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 13:03:58','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                 and phone_mac = 'c0ccf8ef75bf'  
               ) t
         where t.time_stamp between '20171020 13:03:58' and '20171020 13:13:00' 
         group by t.phone_mac
       ) tt
;














==1==黑屏不连接商场wifi也不连接其他wifi========

 select phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 13:42:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 13:32:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8'   ) --华为  
               ) t
         where t.time_stamp between '20171020 13:32:00' and '20171020 13:42:00' 
         group by t.phone_mac
       ) tt
;


==2==黑屏不连接商场wifi也不连接其他wifi========

 select phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 13:53:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 13:43:34','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8'   ) --华为  
               ) t
         where t.time_stamp between '20171020 13:43:34' and '20171020 13:53:00' 
         group by t.phone_mac
       ) tt
;


==3==黑屏不连接商场wifi也不连接其他wifi========

 select phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 14:05:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 13:55:19','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8'   ) --华为  
               ) t
         where t.time_stamp between '20171020 13:55:19' and '20171020 14:05:00' 
         group by t.phone_mac
       ) tt
;



==4==黑屏不连接商场wifi也不连接其他wifi========

 select phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 14:18:03','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 14:07:31','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8'   ) --华为  
               ) t
         where t.time_stamp between '20171020 14:07:31' and '20171020 14:18:03' 
         group by t.phone_mac
       ) tt
;



==5==黑屏不连接商场wifi也不连接其他wifi========

 select phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 14:29:58','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 14:19:33','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8'   ) --华为  
               ) t
         where t.time_stamp between '20171020 14:19:33' and '20171020 14:29:58' 
         group by t.phone_mac
       ) tt
;


==6==黑屏不连接商场wifi也不连接其他wifi========

 select phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 14:42:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 14:31:58','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8'   ) --华为  
               ) t
         where t.time_stamp between '20171020 14:31:58' and '20171020 14:42:00' 
         group by t.phone_mac
       ) tt
;


==7==黑屏不连接商场wifi也不连接其他wifi========

 select phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 14:54:28','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 14:44:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8'   ) --华为  
               ) t
         where t.time_stamp between '20171020 14:44:00' and '20171020 14:54:28' 
         group by t.phone_mac
       ) tt
;


==8==黑屏不连接商场wifi也不连接其他wifi========

 select phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 15:06:05','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 14:55:43','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8'   ) --华为  
               ) t
         where t.time_stamp between '20171020 14:55:43' and '20171020 15:06:05' 
         group by t.phone_mac
       ) tt
;





因为iphone mac 徐文娣给错了(给错的:c0ccf8ef75c0，现在正确的:c0ccf8ef75bf)，所以要重新算iphone mac 的探针测试情况（20171031）
===黑屏不连接商场wifi也不连接其他wifi========
       
select 1 as distance
      ,phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 13:42:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 13:32:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac= 'c0ccf8ef75bf'
               ) t
         where t.time_stamp between '20171020 13:32:00' and '20171020 13:42:00' 
         group by t.phone_mac
       ) tt

union all
       
 select 2 as distance
      ,phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 13:53:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 13:43:34','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac= 'c0ccf8ef75bf'
               ) t
         where t.time_stamp between '20171020 13:43:34' and '20171020 13:53:00' 
         group by t.phone_mac
       ) tt

union all
       
 select 3 as distance
      ,phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 14:05:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 13:55:19','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac= 'c0ccf8ef75bf'
               ) t
         where t.time_stamp between '20171020 13:55:19' and '20171020 14:05:00' 
         group by t.phone_mac
       ) tt

union all
       
 select 4 as distance
      ,phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 14:18:03','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 14:07:31','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac= 'c0ccf8ef75bf'
               ) t
         where t.time_stamp between '20171020 14:07:31' and '20171020 14:18:03' 
         group by t.phone_mac
       ) tt

union all
       
 select 5 as distance
      ,phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 14:29:58','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 14:19:33','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac= 'c0ccf8ef75bf'
               ) t
         where t.time_stamp between '20171020 14:19:33' and '20171020 14:29:58' 
         group by t.phone_mac
       ) tt

union all
       
 select 6 as distance
      ,phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 14:42:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 14:31:58','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac= 'c0ccf8ef75bf'
               ) t
         where t.time_stamp between '20171020 14:31:58' and '20171020 14:42:00' 
         group by t.phone_mac
       ) tt

union all
       
 select 7 as distance
      ,phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 14:54:28','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 14:44:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac= 'c0ccf8ef75bf'
               ) t
         where t.time_stamp between '20171020 14:44:00' and '20171020 14:54:28' 
         group by t.phone_mac
       ) tt

union all
       
 select 8 as distance
      ,phone_mac
      ,scan_cnt
      ,min_time
      ,max_time
      ,detect_duration
      ,actual_duration
      ,actual_duration - detect_duration as duration_error
      ,all_signal_strength/scan_cnt as avg_signal_strength
      ,scan_cnt/detect_duration as scan_frequency
  from ( 
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
              ,(unix_timestamp('20171020 15:06:05','yyyyMMdd HH:mm:ss')-unix_timestamp('20171020 14:55:43','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171020'
                   and phone_mac= 'c0ccf8ef75bf'
               ) t
         where t.time_stamp between '20171020 14:55:43' and '20171020 15:06:05' 
         group by t.phone_mac
       ) tt
;




