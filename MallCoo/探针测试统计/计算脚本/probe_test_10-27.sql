alter table test.probe_system_test_ex add if not exists partition (src_file_day = '20171027');

===novachie====亮屏，打开wifi，不连接商场wifi也不连接其他wifi==========================

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
              ,(unix_timestamp('20171027 11:28:50','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 11:18:18','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171027 11:18:18' and '20171027 11:28:50' 
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
             ,(unix_timestamp('20171027 11:41:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 11:30:57','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )  
              ) t
        where t.time_stamp between '20171027 11:30:57' and '20171027 11:41:00' 
        group by t.phone_mac
      ) tt

--union all
--
--select 3 as distance
--     ,phone_mac
--     ,scan_cnt
--     ,min_time
--     ,max_time
--     ,detect_duration
--     ,actual_duration
--     ,actual_duration - detect_duration as duration_error
--     ,all_signal_strength/scan_cnt as avg_signal_strength
--     ,scan_cnt/detect_duration as scan_frequency
-- from ( 
--       select phone_mac
--             ,count(*) as scan_cnt
--             ,min(t.time_stamp) as min_time
--             ,max(t.time_stamp) as max_time
--             ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
--             ,(unix_timestamp('20171027 14:09:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 13:59:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
--             ,sum(t.signal_strength) as all_signal_strength
--         from (
--               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
--                     ,phone_mac
--                     ,signal_strength
--                     ,src_file_day
--                 from test.probe_system_test_ex a
--                where a.src_file_day = '20171027'
--                  and phone_mac in (   
--                  '3480b3fc5268',  --小米手机
--                  'ecdf3adcad7c',  --vivo
--                  'f025b796195c',  --三星
--                  '78f5fd6e5bf8',  --华为
--                  'c0ccf8ef75c0' )   
--              ) t
--        where t.time_stamp between '20171027 13:59:00' and '20171027 14:09:00' 
--        group by t.phone_mac
--      ) tt

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
             ,(unix_timestamp('20171027 12:06:47','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 11:55:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )  
              ) t
        where t.time_stamp between '20171027 11:55:00' and '20171027 12:06:47' 
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
             ,(unix_timestamp('20171027 12:39:15','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 12:28:52','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )     
              ) t
        where t.time_stamp between '20171027 12:28:52' and '20171027 12:39:15' 
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
             ,(unix_timestamp('20171027 12:51:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 12:41:22','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )  
              ) t
        where t.time_stamp between '20171027 12:41:22' and '20171027 12:51:00' 
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
             ,(unix_timestamp('20171027 13:05:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 12:54:33','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a 
                where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171027 12:54:33' and '20171027 13:05:00' 
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
             ,(unix_timestamp('20171027 13:17:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 13:07:23','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )  
              ) t
        where t.time_stamp between '20171027 13:07:23' and '20171027 13:17:00' 
        group by t.phone_mac
      ) tt;



	  
	  
===星怡会====亮屏，打开wifi，不连接商场wifi也不连接其他wifi==========================

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
              ,(unix_timestamp('20171027 17:43:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 17:33:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171027 17:33:00' and '20171027 17:43:00' 
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
             ,(unix_timestamp('20171027 14:32:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 14:22:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )  
              ) t
        where t.time_stamp between '20171027 14:22:00' and '20171027 14:32:00' 
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
             ,(unix_timestamp('20171027 14:44:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 14:34:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171027 14:34:00' and '20171027 14:44:00' 
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
             ,(unix_timestamp('20171027 14:57:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 14:47:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )    
              ) t
        where t.time_stamp between '20171027 14:47:00' and '20171027 14:57:00' 
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
             ,(unix_timestamp('20171027 15:09:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 14:59:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171027 14:59:00' and '20171027 15:09:00' 
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
             ,(unix_timestamp('20171027 15:22:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 15:12:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )  
              ) t
        where t.time_stamp between '20171027 15:12:00' and '20171027 15:22:00' 
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
             ,(unix_timestamp('20171027 15:34:56','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 15:24:34','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a 
                where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171027 15:24:34' and '20171027 15:34:56' 
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
             ,(unix_timestamp('20171027 15:45:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 15:35:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171027 15:35:00' and '20171027 15:45:00' 
        group by t.phone_mac
      ) tt;
	  
	  
	  
=======亮屏，打开WiFi，不连接商场wifi，连接其他wifi==========================

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
              ,(unix_timestamp('20171027 17:31:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 17:20:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
               ) t
         where t.time_stamp between '20171027 17:20:00' and '20171027 17:31:00' 
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
             ,(unix_timestamp('20171027 17:05:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 16:54:50','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )    
              ) t
        where t.time_stamp between '20171027 16:54:50' and '20171027 17:05:00' 
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
             ,(unix_timestamp('20171027 17:18:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 17:07:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )    
              ) t
        where t.time_stamp between '20171027 17:07:00' and '20171027 17:18:00' 
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
             ,(unix_timestamp('20171027 16:51:23','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 16:41:11','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171027 16:41:11' and '20171027 16:51:23' 
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
             ,(unix_timestamp('20171027 16:38:06','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 16:27:42','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )    
              ) t
        where t.time_stamp between '20171027 16:27:42' and '20171027 16:38:06' 
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
             ,(unix_timestamp('20171027 16:25:49','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 16:15:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )    
              ) t
        where t.time_stamp between '20171027 16:15:00' and '20171027 16:25:49' 
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
             ,(unix_timestamp('20171027 16:12:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 16:02:35','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a 
                where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171027 16:02:35' and '20171027 16:12:00' 
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
             ,(unix_timestamp('20171027 16:00:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 15:50:39','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )     
              ) t
        where t.time_stamp between '20171027 15:50:39' and '20171027 16:00:00' 
        group by t.phone_mac
      ) tt;
	  
	  
	  
	  
==novachie=====亮屏不连接商场wifi链接其他wifi===========

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
             ,(unix_timestamp('20171027 11:14:18','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 11:03:51','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )     
              ) t
        where t.time_stamp between '20171027 11:03:51' and '20171027 11:14:18' 
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
             ,(unix_timestamp('20171027 11:01:51','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 10:51:17','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )      
              ) t
        where t.time_stamp between '20171027 10:51:17' and '20171027 11:01:51' 
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
             ,(unix_timestamp('20171027 10:49:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171027 10:38:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171027'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )       
              ) t
        where t.time_stamp between '20171027 10:38:00' and '20171027 10:49:00' 
        group by t.phone_mac
      ) tt