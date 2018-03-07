
alter table test.probe_system_test_ex add if not exists partition (src_file_day='20171023');



==1==黑屏不连接商场wifi链接其他wifi========

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
             ,(unix_timestamp('20171023 15:00:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 14:50:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171023 14:50:00' and '20171023 15:00:00' 
        group by t.phone_mac
      ) tt
;

==2==黑屏不连接商场wifi链接其他wifi========

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
             ,(unix_timestamp('20171023 15:12:09','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 15:01:41','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171023 15:01:41' and '20171023 15:12:09' 
        group by t.phone_mac
      ) tt
;


==3==黑屏不连接商场wifi链接其他wifi========

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
             ,(unix_timestamp('20171023 15:23:52','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 15:13:24','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171023 15:13:24' and '20171023 15:23:52' 
        group by t.phone_mac
      ) tt
;

==4==黑屏不连接商场wifi链接其他wifi========

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
             ,(unix_timestamp('20171023 15:35:46','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 15:25:13','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171023 15:25:13' and '20171023 15:35:46' 
        group by t.phone_mac
      ) tt
;



==5==黑屏不连接商场wifi链接其他wifi========

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
             ,(unix_timestamp('20171023 15:47:51','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 15:37:29','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171023 15:37:29' and '20171023 15:47:51' 
        group by t.phone_mac
      ) tt
;


==6==黑屏不连接商场wifi链接其他wifi========

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
             ,(unix_timestamp('20171023 16:00:13','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 15:49:51','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171023 15:49:51' and '20171023 16:00:13' 
        group by t.phone_mac
      ) tt
;

==7==黑屏不连接商场wifi链接其他wifi========

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
             ,(unix_timestamp('20171023 16:12:02','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 16:01:40','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171023 16:01:40' and '20171023 16:12:02' 
        group by t.phone_mac
      ) tt
;

==8==黑屏不连接商场wifi链接其他wifi========

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
             ,(unix_timestamp('20171023 16:24:06','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 16:13:43','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171023 16:13:43' and '20171023 16:24:06' 
        group by t.phone_mac
      ) tt
;






==iphone==黑屏不连接商场wifi链接其他wifi========
       
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
             ,(unix_timestamp('20171023 15:00:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 14:50:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                   and phone_mac= 'c0ccf8ef75bf'  
              ) t
        where t.time_stamp between '20171023 14:50:00' and '20171023 15:00:00' 
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
             ,(unix_timestamp('20171023 15:12:09','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 15:01:41','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                   and phone_mac= 'c0ccf8ef75bf' 
              ) t
        where t.time_stamp between '20171023 15:01:41' and '20171023 15:12:09' 
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
             ,(unix_timestamp('20171023 15:23:52','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 15:13:24','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                   and phone_mac= 'c0ccf8ef75bf'
              ) t
        where t.time_stamp between '20171023 15:13:24' and '20171023 15:23:52' 
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
             ,(unix_timestamp('20171023 15:35:46','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 15:25:13','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                   and phone_mac= 'c0ccf8ef75bf'
              ) t
        where t.time_stamp between '20171023 15:25:13' and '20171023 15:35:46' 
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
             ,(unix_timestamp('20171023 15:47:51','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 15:37:29','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                   and phone_mac= 'c0ccf8ef75bf'
              ) t
        where t.time_stamp between '20171023 15:37:29' and '20171023 15:47:51' 
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
             ,(unix_timestamp('20171023 16:00:13','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 15:49:51','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                   and phone_mac= 'c0ccf8ef75bf'
              ) t
        where t.time_stamp between '20171023 15:49:51' and '20171023 16:00:13' 
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
             ,(unix_timestamp('20171023 16:12:02','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 16:01:40','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                   and phone_mac= 'c0ccf8ef75bf' 
              ) t
        where t.time_stamp between '20171023 16:01:40' and '20171023 16:12:02' 
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
             ,(unix_timestamp('20171023 16:24:06','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 16:13:43','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                  and phone_mac ='c0ccf8ef75bf' 
              ) t
        where t.time_stamp between '20171023 16:13:43' and '20171023 16:24:06' 
        group by t.phone_mac
      ) tt
;






==1==黑屏打开wifi不连接商场wifi也不连接其他wifi========

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
             ,(unix_timestamp('20171023 16:40:34','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 16:30:23','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171023 16:30:23' and '20171023 16:40:34' 
        group by t.phone_mac
      ) tt
;

==2==黑屏打开wifi不连接商场wifi也不连接其他wifi========

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
             ,(unix_timestamp('20171023 16:52:04','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 16:41:38','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171023 16:41:38' and '20171023 16:52:04' 
        group by t.phone_mac
      ) tt
;


==3==黑屏打开wifi不连接商场wifi也不连接其他wifi========

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
             ,(unix_timestamp('20171023 17:03:52','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 16:53:30','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171023 16:53:30' and '20171023 17:03:52' 
        group by t.phone_mac
      ) tt
;

==4==黑屏打开wifi不连接商场wifi也不连接其他wifi========

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
             ,(unix_timestamp('20171023 17:15:25','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 17:05:04','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171023 17:05:04' and '20171023 17:15:25' 
        group by t.phone_mac
      ) tt
;



==5==黑屏打开wifi不连接商场wifi也不连接其他wifi========

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
             ,(unix_timestamp('20171023 17:27:09','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 17:16:44','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171023 17:16:44' and '20171023 17:27:09' 
        group by t.phone_mac
      ) tt
;


==6==黑屏打开wifi不连接商场wifi也不连接其他wifi========

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
             ,(unix_timestamp('20171023 17:39:08','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 17:28:36','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171023 17:28:36' and '20171023 17:39:08' 
        group by t.phone_mac
      ) tt
;

==7==黑屏打开wifi不连接商场wifi也不连接其他wifi========


==8==黑屏打开wifi不连接商场wifi也不连接其他wifi========








====黑屏打开wifi不连接商场wifi也不连接其他wifi========

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
             ,(unix_timestamp('20171023 16:40:34','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 16:30:23','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                   and phone_mac= 'c0ccf8ef75bf' 
              ) t
        where t.time_stamp between '20171023 16:30:23' and '20171023 16:40:34' 
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
             ,(unix_timestamp('20171023 16:52:04','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 16:41:38','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                   and phone_mac= 'c0ccf8ef75bf'
              ) t
        where t.time_stamp between '20171023 16:41:38' and '20171023 16:52:04' 
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
             ,(unix_timestamp('20171023 17:03:52','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 16:53:30','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                   and phone_mac= 'c0ccf8ef75bf'
              ) t
        where t.time_stamp between '20171023 16:53:30' and '20171023 17:03:52' 
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
             ,(unix_timestamp('20171023 17:15:25','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 17:05:04','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                   and phone_mac= 'c0ccf8ef75bf'
              ) t
        where t.time_stamp between '20171023 17:05:04' and '20171023 17:15:25' 
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
             ,(unix_timestamp('20171023 17:27:09','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 17:16:44','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                   and phone_mac= 'c0ccf8ef75bf'
              ) t
        where t.time_stamp between '20171023 17:16:44' and '20171023 17:27:09' 
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
             ,(unix_timestamp('20171023 17:39:08','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 17:28:36','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                   and phone_mac= 'c0ccf8ef75bf' 
              ) t
        where t.time_stamp between '20171023 17:28:36' and '20171023 17:39:08' 
        group by t.phone_mac
      ) tt
;
















==1==黑屏打开wifi不连接商场wifi也不连接其他wifi========

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
             ,(unix_timestamp('20171023 18:20:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 18:10:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171023 18:10:00' and '20171023 18:20:00' 
        group by t.phone_mac
      ) tt
;

==2==黑屏打开wifi不连接商场wifi也不连接其他wifi========

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
             ,(unix_timestamp('20171023 18:34:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 18:24:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171023 18:24:00' and '20171023 18:34:00' 
        group by t.phone_mac
      ) tt
;











==iphone==黑屏打开wifi不连接商场wifi也不连接其他wifi========

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
             ,(unix_timestamp('20171023 18:20:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 18:10:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                   and phone_mac= 'c0ccf8ef75bf'
              ) t
        where t.time_stamp between '20171023 18:10:00' and '20171023 18:20:00' 
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
             ,(unix_timestamp('20171023 18:34:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171023 18:24:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171023'
                   and phone_mac= 'c0ccf8ef75bf'  
              ) t
        where t.time_stamp between '20171023 18:24:00' and '20171023 18:34:00' 
        group by t.phone_mac
      ) tt
;


