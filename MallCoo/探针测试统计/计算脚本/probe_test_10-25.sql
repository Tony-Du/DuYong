alter table test.probe_system_test_ex add if not exists partition (src_file_day = '20171025');

====黑屏链接商铺wifi不连接商场wifi====
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
              ,(unix_timestamp('20171025 11:10:37','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 11:00:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171025'
                   and phone_mac in (   
                   --'3480b3fc5268',  --小米手机
                   --'ecdf3adcad7c',  --vivo
                   --'f025b796195c',  --三星
                   --'78f5fd6e5bf8',  --华为
                   'c0ccf8ef75bf' )   
               ) t
         where t.time_stamp between '20171025 11:00:00' and '20171025 11:10:37' 
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
             ,(unix_timestamp('20171025 11:34:38','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 11:24:18','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171025'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171025 11:24:18' and '20171025 11:34:38' 
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
             ,(unix_timestamp('20171025 10:56:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 10:45:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171025'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171025 10:45:00' and '20171025 10:56:00' 
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
             ,(unix_timestamp('20171025 11:22:48','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 11:12:48','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171025'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171025 11:12:48' and '20171025 11:22:48' 
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
             ,(unix_timestamp('20171025 11:46:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 11:36:51','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171025'
                  and phone_mac in (   
                 --'3480b3fc5268',  --小米手机
                 --'ecdf3adcad7c',  --vivo
                 --'f025b796195c',  --三星
                 --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171025 11:36:51' and '20171025 11:46:00' 
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
             ,(unix_timestamp('20171025 12:16:24','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 12:06:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171025'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171025 12:06:00' and '20171025 12:16:24' 
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
             ,(unix_timestamp('20171025 12:39:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 12:29:51','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171025'
                  and phone_mac in (   
                 --'3480b3fc5268',  --小米手机
                 --'ecdf3adcad7c',  --vivo
                 --'f025b796195c',  --三星
                 --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171025 12:29:51' and '20171025 12:39:00' 
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
             ,(unix_timestamp('20171025 12:28:19','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 12:18:04','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171025'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171025 12:18:04' and '20171025 12:28:19' 
        group by t.phone_mac
      ) tt
;



====黑屏打开wifi不连接商场wifi也不连接其他wifi====
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
              ,(unix_timestamp('20171025 13:21:09','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 13:11:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171025'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
               ) t
         where t.time_stamp between '20171025 13:11:00' and '20171025 13:21:09' 
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
             ,(unix_timestamp('20171025 13:33:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 13:22:51','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171025'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )    
              ) t
        where t.time_stamp between '20171025 13:22:51' and '20171025 13:33:00' 
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
             ,(unix_timestamp('20171025 13:57:49','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 13:47:35','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171025'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )    
              ) t
        where t.time_stamp between '20171025 13:47:35' and '20171025 13:57:49' 
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
             ,(unix_timestamp('20171025 15:06:28','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 14:56:13','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171025'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171025 14:56:13' and '20171025 15:06:28' 
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
             ,(unix_timestamp('20171025 13:45:42','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 13:34:55','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171025'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )    
              ) t
        where t.time_stamp between '20171025 13:34:55' and '20171025 13:45:42' 
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
             ,(unix_timestamp('20171025 14:10:53','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 14:00:39','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171025'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171025 14:00:39' and '20171025 14:10:53' 
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
             ,(unix_timestamp('20171025 14:22:39','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 14:12:26','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a 
                where a.src_file_day = '20171025'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171025 14:12:26' and '20171025 14:22:39' 
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
             ,(unix_timestamp('20171025 14:53:31','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 14:43:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171025'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171025 14:43:00' and '20171025 14:53:31' 
        group by t.phone_mac
      ) tt
;





=======亮屏不连接商场wifi也不连接其他wifi==========================

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
              ,(unix_timestamp('20171025 16:03:54','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 15:53:34','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171025'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
               ) t
         where t.time_stamp between '20171025 15:53:34' and '20171025 16:03:54' 
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
             ,(unix_timestamp('20171025 16:16:12','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 16:06:07','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171025'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171025 16:06:07' and '20171025 16:16:12' 
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
             ,(unix_timestamp('20171025 15:37:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 15:21:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171025'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171025 15:21:00' and '20171025 15:37:00' 
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
             ,(unix_timestamp('20171025 15:18:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 15:08:19','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171025'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171025 15:08:19' and '20171025 15:18:00' 
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
             ,(unix_timestamp('20171025 16:29:29','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 16:19:19','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171025'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )    
              ) t
        where t.time_stamp between '20171025 16:19:19' and '20171025 16:29:29' 
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
             ,(unix_timestamp('20171025 16:57:34','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 16:47:18','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171025'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )    
              ) t
        where t.time_stamp between '20171025 16:47:18' and '20171025 16:57:34' 
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
             ,(unix_timestamp('20171025 16:42:30','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 16:32:03','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a 
                where a.src_file_day = '20171025'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171025 16:32:03' and '20171025 16:42:30' 
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
             ,(unix_timestamp('20171025 15:49:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171025 15:39:35','yyyyMMdd HH:mm:ss'))/60 as actual_duration
             ,sum(t.signal_strength) as all_signal_strength
         from (
               select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                     ,phone_mac
                     ,signal_strength
                     ,src_file_day
                 from test.probe_system_test_ex a
                where a.src_file_day = '20171025'
                  and phone_mac in (   
                  --'3480b3fc5268',  --小米手机
                  --'ecdf3adcad7c',  --vivo
                  --'f025b796195c',  --三星
                  --'78f5fd6e5bf8',  --华为
                  'c0ccf8ef75bf' )   
              ) t
        where t.time_stamp between '20171025 15:39:35' and '20171025 15:49:00' 
        group by t.phone_mac
      ) tt
;