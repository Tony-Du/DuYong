alter table test.probe_system_test_ex add if not exists partition (src_file_day = '20171024');



==novachic==黑屏连接商铺wifi也不连接其他wifi========

--select 1 as distance
--      ,phone_mac
--      ,scan_cnt
--      ,min_time
--      ,max_time
--      ,detect_duration
--      ,actual_duration
--      ,actual_duration - detect_duration as duration_error
--      ,all_signal_strength/scan_cnt as avg_signal_strength
--      ,scan_cnt/detect_duration as scan_frequency
--  from ( 
--        select phone_mac
--              ,count(*) as scan_cnt
--              ,min(t.time_stamp) as min_time
--              ,max(t.time_stamp) as max_time
--              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60 as detect_duration --单位min
--              ,(unix_timestamp('20171024 17:43:16','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 17:32:58','yyyyMMdd HH:mm:ss'))/60 as actual_duration
--              ,sum(t.signal_strength) as all_signal_strength
--          from (
--                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
--                      ,phone_mac
--                      ,signal_strength
--                      ,src_file_day
--                  from test.probe_system_test_ex a
--                 where a.src_file_day = '20171024'
--                   and phone_mac in (   
--                   '3480b3fc5268',  --小米手机
--                   'ecdf3adcad7c',  --vivo
--                   'f025b796195c',  --三星
--                   '78f5fd6e5bf8',  --华为
--                   'c0ccf8ef75c0' )   
--               ) t
--         where t.time_stamp between '20171024 17:32:58' and '20171024 17:43:16' 
--         group by t.phone_mac
--       ) tt
--
--union all
--
--select 2 as distance
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
--             ,(unix_timestamp('20171024 17:56:24','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 17:44:17','yyyyMMdd HH:mm:ss'))/60 as actual_duration
--             ,sum(t.signal_strength) as all_signal_strength
--         from (
--               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
--                     ,phone_mac
--                     ,signal_strength
--                     ,src_file_day
--                 from test.probe_system_test_ex a
--                where a.src_file_day = '20171024'
--                  and phone_mac in (   
--                  '3480b3fc5268',  --小米手机
--                  'ecdf3adcad7c',  --vivo
--                  'f025b796195c',  --三星
--                  '78f5fd6e5bf8',  --华为
--                  'c0ccf8ef75c0' )   
--              ) t
--        where t.time_stamp between '20171024 17:44:17' and '20171024 17:56:24' 
--        group by t.phone_mac
--      ) tt
--
--union all

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
             ,(unix_timestamp('20171024 11:42:01','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 11:31:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171024 11:31:00' and '20171024 11:42:01' 
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
             ,(unix_timestamp('20171024 11:53:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 11:43:44','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171024 11:43:44' and '20171024 11:53:00' 
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
             ,(unix_timestamp('20171024 12:07:11','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 11:56:39','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171024 11:56:39' and '20171024 12:07:11' 
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
             ,(unix_timestamp('20171024 12:20:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 12:10:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171024 12:10:00' and '20171024 12:20:00' 
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
             ,(unix_timestamp('20171024 12:33:38','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 12:22:27','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171024 12:22:27' and '20171024 12:33:38' 
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
             ,(unix_timestamp('20171024 12:45:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 12:34:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171024 12:34:00' and '20171024 12:45:00' 
        group by t.phone_mac
      ) tt
;



--补iphone数据


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
             ,(unix_timestamp('20171024 11:42:01','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 11:31:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                   and phone_mac ='c0ccf8ef75bf'
              ) t
        where t.time_stamp between '20171024 11:31:00' and '20171024 11:42:01' 
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
             ,(unix_timestamp('20171024 11:53:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 11:43:44','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                   and phone_mac ='c0ccf8ef75bf'
              ) t
        where t.time_stamp between '20171024 11:43:44' and '20171024 11:53:00' 
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
             ,(unix_timestamp('20171024 12:07:11','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 11:56:39','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                   and phone_mac ='c0ccf8ef75bf'  
              ) t
        where t.time_stamp between '20171024 11:56:39' and '20171024 12:07:11' 
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
             ,(unix_timestamp('20171024 12:20:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 12:10:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                   and phone_mac ='c0ccf8ef75bf' 
              ) t
        where t.time_stamp between '20171024 12:10:00' and '20171024 12:20:00' 
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
             ,(unix_timestamp('20171024 12:33:38','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 12:22:27','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                   and phone_mac ='c0ccf8ef75bf'
              ) t
        where t.time_stamp between '20171024 12:22:27' and '20171024 12:33:38' 
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
             ,(unix_timestamp('20171024 12:45:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 12:34:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                   and phone_mac ='c0ccf8ef75bf'
              ) t
        where t.time_stamp between '20171024 12:34:00' and '20171024 12:45:00' 
        group by t.phone_mac
      ) tt
;










==g-super==黑屏连接商铺wifi，不连接商铺wifi========

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
              ,(unix_timestamp('20171024 14:23:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 14:13:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171024'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8',  --华为
                   'c0ccf8ef75c0' )   
               ) t
         where t.time_stamp between '20171024 14:13:00' and '20171024 14:23:00' 
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
             ,(unix_timestamp('20171024 14:35:50','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 14:25:39','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171024 14:25:39' and '20171024 14:35:50' 
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
             ,(unix_timestamp('20171024 14:47:57','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 14:37:57','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171024 14:37:57' and '20171024 14:47:57' 
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
             ,(unix_timestamp('20171024 15:00:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 14:49:39','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171024 14:49:39' and '20171024 15:00:00' 
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
             ,(unix_timestamp('20171024 15:12:34','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 15:02:24','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171024 15:02:24' and '20171024 15:12:34' 
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
             ,(unix_timestamp('20171024 15:24:32','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 15:14:15','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171024 15:14:15' and '20171024 15:24:32' 
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
             ,(unix_timestamp('20171024 15:36:30','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 15:26:14','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171024 15:26:14' and '20171024 15:36:30' 
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
             ,(unix_timestamp('20171024 15:48:09','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 15:37:53','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171024 15:37:53' and '20171024 15:48:09' 
        group by t.phone_mac
      ) tt
;


--补充iphone数据

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
              ,(unix_timestamp('20171024 14:23:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 14:13:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171024'
                   and phone_mac ='c0ccf8ef75bf' 
               ) t
         where t.time_stamp between '20171024 14:13:00' and '20171024 14:23:00' 
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
             ,(unix_timestamp('20171024 14:35:50','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 14:25:39','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                   and phone_mac ='c0ccf8ef75bf'
              ) t
        where t.time_stamp between '20171024 14:25:39' and '20171024 14:35:50' 
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
             ,(unix_timestamp('20171024 14:47:57','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 14:37:57','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                   and phone_mac ='c0ccf8ef75bf'   
              ) t
        where t.time_stamp between '20171024 14:37:57' and '20171024 14:47:57' 
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
             ,(unix_timestamp('20171024 15:00:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 14:49:39','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                   and phone_mac ='c0ccf8ef75bf'
              ) t
        where t.time_stamp between '20171024 14:49:39' and '20171024 15:00:00' 
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
             ,(unix_timestamp('20171024 15:12:34','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 15:02:24','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                   and phone_mac ='c0ccf8ef75bf'  
              ) t
        where t.time_stamp between '20171024 15:02:24' and '20171024 15:12:34' 
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
             ,(unix_timestamp('20171024 15:24:32','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 15:14:15','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                   and phone_mac ='c0ccf8ef75bf' 
              ) t
        where t.time_stamp between '20171024 15:14:15' and '20171024 15:24:32' 
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
             ,(unix_timestamp('20171024 15:36:30','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 15:26:14','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                   and phone_mac ='c0ccf8ef75bf'
              ) t
        where t.time_stamp between '20171024 15:26:14' and '20171024 15:36:30' 
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
             ,(unix_timestamp('20171024 15:48:09','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 15:37:53','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                   and phone_mac ='c0ccf8ef75bf'
              ) t
        where t.time_stamp between '20171024 15:37:53' and '20171024 15:48:09' 
        group by t.phone_mac
      ) tt
;









==g-super==黑屏连接商场wifi========

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
              ,(unix_timestamp('20171024 16:03:58','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 15:53:43','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171024'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8',  --华为
                   'c0ccf8ef75c0' )   
               ) t
         where t.time_stamp between '20171024 15:53:43' and '20171024 16:03:58' 
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
             ,(unix_timestamp('20171024 16:29:46','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 16:19:40','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171024 16:19:40' and '20171024 16:29:46' 
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
             ,(unix_timestamp('20171024 16:41:20','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 16:31:11','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171024 16:31:11' and '20171024 16:41:20' 
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
             ,(unix_timestamp('20171024 16:17:19','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 16:07:11','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171024 16:07:11' and '20171024 16:17:19' 
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
             ,(unix_timestamp('20171024 16:53:17','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 16:43:02','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171024 16:43:02' and '20171024 16:53:17' 
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
             ,(unix_timestamp('20171024 17:06:05','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 16:56:03','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171024 16:56:03' and '20171024 17:06:05' 
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
             ,(unix_timestamp('20171024 17:18:41','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 17:08:31','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171024 17:08:31' and '20171024 17:18:41' 
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
             ,(unix_timestamp('20171024 17:30:26','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 17:20:09','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  '3480b3fc5268',  --小米手机
                  'ecdf3adcad7c',  --vivo
                  'f025b796195c',  --三星
                  '78f5fd6e5bf8',  --华为
                  'c0ccf8ef75c0' )   
              ) t
        where t.time_stamp between '20171024 17:20:09' and '20171024 17:30:26' 
        group by t.phone_mac
      ) tt
;


--补充iphone数据
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
              ,(unix_timestamp('20171024 16:03:58','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 15:53:43','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171024'
                   and phone_mac ='c0ccf8ef75bf'
               ) t
         where t.time_stamp between '20171024 15:53:43' and '20171024 16:03:58' 
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
             ,(unix_timestamp('20171024 16:29:46','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 16:19:40','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                   and phone_mac ='c0ccf8ef75bf'
              ) t
        where t.time_stamp between '20171024 16:19:40' and '20171024 16:29:46' 
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
             ,(unix_timestamp('20171024 16:41:20','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 16:31:11','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                   and phone_mac ='c0ccf8ef75bf' 
              ) t
        where t.time_stamp between '20171024 16:31:11' and '20171024 16:41:20' 
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
             ,(unix_timestamp('20171024 16:17:19','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 16:07:11','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                   and phone_mac ='c0ccf8ef75bf'
              ) t
        where t.time_stamp between '20171024 16:07:11' and '20171024 16:17:19' 
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
             ,(unix_timestamp('20171024 16:53:17','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 16:43:02','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                   and phone_mac ='c0ccf8ef75bf' 
              ) t
        where t.time_stamp between '20171024 16:43:02' and '20171024 16:53:17' 
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
             ,(unix_timestamp('20171024 17:06:05','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 16:56:03','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                   and phone_mac ='c0ccf8ef75bf'  
              ) t
        where t.time_stamp between '20171024 16:56:03' and '20171024 17:06:05' 
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
             ,(unix_timestamp('20171024 17:18:41','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 17:08:31','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                   and phone_mac ='c0ccf8ef75bf'  
              ) t
        where t.time_stamp between '20171024 17:08:31' and '20171024 17:18:41' 
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
             ,(unix_timestamp('20171024 17:30:26','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 17:20:09','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                   and phone_mac ='c0ccf8ef75bf'   
              ) t
        where t.time_stamp between '20171024 17:20:09' and '20171024 17:30:26' 
        group by t.phone_mac
      ) tt
;






==g-super==亮屏连接商场wifi========

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
              ,(unix_timestamp('20171024 17:43:16','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 17:32:58','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171024'
                   and phone_mac in (   
                   --'3480b3fc5268',  --小米手机
                   --'ecdf3adcad7c',  --vivo
                   --'f025b796195c',  --三星
                   --'78f5fd6e5bf8',  --华为
                   'c0ccf8ef75bf' )   
               ) t
         where t.time_stamp between '20171024 17:32:58' and '20171024 17:43:16' 
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
             ,(unix_timestamp('20171024 17:56:24','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 17:44:17','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171024 17:44:17' and '20171024 17:56:24' 
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
             ,(unix_timestamp('20171024 18:07:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 17:56:50','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                 --'3480b3fc5268',  --小米手机
                 --'ecdf3adcad7c',  --vivo
                 --'f025b796195c',  --三星
                 --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171024 17:56:50' and '20171024 18:07:00' 
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
             ,(unix_timestamp('20171024 18:20:51','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 18:10:30','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171024 18:10:30' and '20171024 18:20:51' 
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
             ,(unix_timestamp('20171024 18:32:40','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 18:22:26','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171024 18:22:26' and '20171024 18:32:40' 
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
             ,(unix_timestamp('20171024 18:45:07','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 18:34:42','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171024 18:34:42' and '20171024 18:45:07' 
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
             ,(unix_timestamp('20171024 18:57:02','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 18:46:37','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171024 18:46:37' and '20171024 18:57:02' 
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
             ,(unix_timestamp('20171024 19:08:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171024 18:58:17','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171024'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171024 18:58:17' and '20171024 19:08:00' 
        group by t.phone_mac
      ) tt
;






