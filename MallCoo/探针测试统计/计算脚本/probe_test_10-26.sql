alter table test.probe_system_test_ex add if not exists partition (src_file_day = '20171026');

=======亮屏，不连接商场wifi，连接其他wifi==========================

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
              ,(unix_timestamp('20171026 11:20:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 11:10:25','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171026'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
               ) t
         where t.time_stamp between '20171026 11:10:25' and '20171026 11:20:00' 
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
             ,(unix_timestamp('20171026 11:08:45','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 10:58:29','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )  
              ) t
        where t.time_stamp between '20171026 10:58:29' and '20171026 11:08:45' 
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
             ,(unix_timestamp('20171026 11:33:37','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 11:23:09','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )  
              ) t
        where t.time_stamp between '20171026 11:23:09' and '20171026 11:33:37' 
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
             ,(unix_timestamp('20171026 11:59:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 11:48:09','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )    
              ) t
        where t.time_stamp between '20171026 11:48:09' and '20171026 11:59:00' 
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
             ,(unix_timestamp('20171026 12:25:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 12:13:26','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )  
              ) t
        where t.time_stamp between '20171026 12:13:26' and '20171026 12:25:00' 
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
             ,(unix_timestamp('20171026 12:38:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 12:27:59','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )    
              ) t
        where t.time_stamp between '20171026 12:27:59' and '20171026 12:38:00' 
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
             ,(unix_timestamp('20171026 12:51:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 12:41:17','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a 
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )  
              ) t
        where t.time_stamp between '20171026 12:41:17' and '20171026 12:51:00' 
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
             ,(unix_timestamp('20171026 13:05:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 12:55:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )  
              ) t
        where t.time_stamp between '20171026 12:55:00' and '20171026 13:05:00' 
        group by t.phone_mac
      ) tt
      
union all

select 11 as distance
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
             ,(unix_timestamp('20171026 11:45:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 11:35:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171026 11:35:00' and '20171026 11:45:00' 
        group by t.phone_mac
      ) tt
;






=======亮屏，打开wifi，不连接商场wifi也不连接其他wifi==========================

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
              ,(unix_timestamp('20171026 13:56:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 13:46:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171026'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )    
               ) t
         where t.time_stamp between '20171026 13:46:00' and '20171026 13:56:00' 
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
             ,(unix_timestamp('20171026 13:44:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 13:33:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171026 13:33:00' and '20171026 13:44:00' 
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
             ,(unix_timestamp('20171026 14:09:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 13:59:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )      
              ) t
        where t.time_stamp between '20171026 13:59:00' and '20171026 14:09:00' 
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
             ,(unix_timestamp('20171026 14:35:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 14:24:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )       
              ) t
        where t.time_stamp between '20171026 14:24:00' and '20171026 14:35:00' 
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
             ,(unix_timestamp('20171026 14:22:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 14:12:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )    
              ) t
        where t.time_stamp between '20171026 14:12:00' and '20171026 14:22:00' 
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
             ,(unix_timestamp('20171026 14:59:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 14:49:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171026 14:49:00' and '20171026 14:59:00' 
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
             ,(unix_timestamp('20171026 14:47:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 14:37:58','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a 
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )    
              ) t
        where t.time_stamp between '20171026 14:37:58' and '20171026 14:47:00' 
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
             ,(unix_timestamp('20171026 15:11:23','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 15:01:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171026 15:01:00' and '20171026 15:11:23' 
        group by t.phone_mac
      ) tt;


=======黑屏不连接商场wifi链接其他wifi==========================

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
             ,(unix_timestamp('20171026 16:30:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 16:20:04','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171026 16:20:04' and '20171026 16:30:00' 
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
             ,(unix_timestamp('20171026 16:56:26','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 16:45:57','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )    
              ) t
        where t.time_stamp between '20171026 16:45:57' and '20171026 16:56:26' 
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
             ,(unix_timestamp('20171026 16:42:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 16:32:40','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171026 16:32:40' and '20171026 16:42:00' 
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
             ,(unix_timestamp('20171026 16:17:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 16:07:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )    
              ) t
        where t.time_stamp between '20171026 16:07:00' and '20171026 16:17:00' 
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
             ,(unix_timestamp('20171026 17:08:57','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 16:56:26','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a 
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171026 16:56:26' and '20171026 17:08:57' 
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
             ,(unix_timestamp('20171026 17:19:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 17:09:50','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171026 17:09:50' and '20171026 17:19:00' 
        group by t.phone_mac
      ) tt;


	  
	  
	  
	  
=======亮屏不连接商场wifi链接其他wifi==========================



union all

-----------------------------------





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
             ,(unix_timestamp('20171026 18:24:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 18:13:51','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171026 18:13:51' and '20171026 18:24:00' 
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
             ,(unix_timestamp('20171026 18:11:09','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 18:00:39','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )     
              ) t
        where t.time_stamp between '20171026 18:00:39' and '20171026 18:11:09' 
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
             ,(unix_timestamp('20171026 17:59:01','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 17:48:56','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )    
              ) t
        where t.time_stamp between '20171026 17:48:56' and '20171026 17:59:01' 
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
             ,(unix_timestamp('20171026 17:45:53','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 17:35:09','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a 
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )    
              ) t
        where t.time_stamp between '20171026 17:35:09' and '20171026 17:45:53' 
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
             ,(unix_timestamp('20171026 17:33:28','yyyyMMdd HH:mm:ss')-unix_timestamp('20171026 17:22:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171026'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171026 17:22:00' and '20171026 17:33:28' 
        group by t.phone_mac
      ) tt;






