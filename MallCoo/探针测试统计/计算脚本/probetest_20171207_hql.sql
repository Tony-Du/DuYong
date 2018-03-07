alter table test.probe_system_test_ex add if not exists partition (src_file_day = '20171207');
alter table test.probe_system_test_ex add if not exists partition (src_file_day = '20171208');


==== 八吉岛mo站 =========
select 25 as distance
      ,phone_mac
      ,case when phone_mac = '3480b3fc5268' then '小米'
            when phone_mac = 'ecdf3adcad7c' then 'vivo'
            when phone_mac = 'f025b796195c' then '三星'
            when phone_mac = '78f5fd6e5bf8' then '华为'
            when phone_mac = 'c0ccf8ef75bf' then '苹果' end as phone_brand
      ,scan_cnt
      ,max_rssi
      ,min_rssi
      ,all_rssi/scan_cnt as avg_rssi
      ,detect_duration
      ,case when detect_duration = 0 then 0 else scan_cnt/detect_duration end as scan_freq_detect
      ,10 as actual_duration
      ,scan_cnt/10 as scan_freq_actual
      ,min_time
      ,max_time
  from (
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,max(signal_strength) as max_rssi
              ,min(signal_strength) as min_rssi
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60.0 as detect_duration --单位min
              ,sum(t.signal_strength) as all_rssi
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,cast(signal_strength as int) as signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171207'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171207 13:29:00' and '20171207 13:39:00' 
         group by t.phone_mac
       ) tt
       
 union all       
       
select 30 as distance
      ,phone_mac
      ,case when phone_mac = '3480b3fc5268' then '小米'
            when phone_mac = 'ecdf3adcad7c' then 'vivo'
            when phone_mac = 'f025b796195c' then '三星'
            when phone_mac = '78f5fd6e5bf8' then '华为'
            when phone_mac = 'c0ccf8ef75bf' then '苹果' end as phone_brand
      ,scan_cnt
      ,max_rssi
      ,min_rssi
      ,all_rssi/scan_cnt as avg_rssi
      ,detect_duration
      ,case when detect_duration = 0 then 0 else scan_cnt/detect_duration end as scan_freq_detect
      ,10 as actual_duration
      ,scan_cnt/10 as scan_freq_actual
      ,min_time
      ,max_time
  from (
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,max(signal_strength) as max_rssi
              ,min(signal_strength) as min_rssi
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60.0 as detect_duration --单位min
              ,sum(t.signal_strength) as all_rssi
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,cast(signal_strength as int) as signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171207'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171207 13:40:00' and '20171207 13:50:00' 
         group by t.phone_mac
       ) tt
       
       


==== 南3飞天梯 =========
       
select 5 as distance
      ,phone_mac
      ,case when phone_mac = '3480b3fc5268' then '小米'
            when phone_mac = 'ecdf3adcad7c' then 'vivo'
            when phone_mac = 'f025b796195c' then '三星'
            when phone_mac = '78f5fd6e5bf8' then '华为'
            when phone_mac = 'c0ccf8ef75bf' then '苹果' end as phone_brand
      ,scan_cnt
      ,max_rssi
      ,min_rssi
      ,all_rssi/scan_cnt as avg_rssi
      ,detect_duration
      ,case when detect_duration = 0 then 0 else scan_cnt/detect_duration end as scan_freq_detect
      ,10 as actual_duration
      ,scan_cnt/10 as scan_freq_actual
      ,min_time
      ,max_time
  from (
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,max(signal_strength) as max_rssi
              ,min(signal_strength) as min_rssi
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60.0 as detect_duration --单位min
              ,sum(t.signal_strength) as all_rssi
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,cast(signal_strength as int) as signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171207'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171207 15:05:00' and '20171207 15:15:00' 
         group by t.phone_mac
       ) tt
       
 union all       
       
select 10 as distance
      ,phone_mac
      ,case when phone_mac = '3480b3fc5268' then '小米'
            when phone_mac = 'ecdf3adcad7c' then 'vivo'
            when phone_mac = 'f025b796195c' then '三星'
            when phone_mac = '78f5fd6e5bf8' then '华为'
            when phone_mac = 'c0ccf8ef75bf' then '苹果' end as phone_brand
      ,scan_cnt
      ,max_rssi
      ,min_rssi
      ,all_rssi/scan_cnt as avg_rssi
      ,detect_duration
      ,case when detect_duration = 0 then 0 else scan_cnt/detect_duration end as scan_freq_detect
      ,10 as actual_duration
      ,scan_cnt/10 as scan_freq_actual
      ,min_time
      ,max_time
  from (
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,max(signal_strength) as max_rssi
              ,min(signal_strength) as min_rssi
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60.0 as detect_duration --单位min
              ,sum(t.signal_strength) as all_rssi
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,cast(signal_strength as int) as signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171207'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171207 15:20:00' and '20171207 15:30:00' 
         group by t.phone_mac
       ) tt
              
 union all       
       
select 15 as distance
      ,phone_mac
      ,case when phone_mac = '3480b3fc5268' then '小米'
            when phone_mac = 'ecdf3adcad7c' then 'vivo'
            when phone_mac = 'f025b796195c' then '三星'
            when phone_mac = '78f5fd6e5bf8' then '华为'
            when phone_mac = 'c0ccf8ef75bf' then '苹果' end as phone_brand
      ,scan_cnt
      ,max_rssi
      ,min_rssi
      ,all_rssi/scan_cnt as avg_rssi
      ,detect_duration
      ,case when detect_duration = 0 then 0 else scan_cnt/detect_duration end as scan_freq_detect
      ,10 as actual_duration
      ,scan_cnt/10 as scan_freq_actual
      ,min_time
      ,max_time
  from (
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,max(signal_strength) as max_rssi
              ,min(signal_strength) as min_rssi
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60.0 as detect_duration --单位min
              ,sum(t.signal_strength) as all_rssi
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,cast(signal_strength as int) as signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171207'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171207 14:22:00' and '20171207 14:32:00' 
         group by t.phone_mac
       ) tt
                  
 union all       
       
select 20 as distance
      ,phone_mac
      ,case when phone_mac = '3480b3fc5268' then '小米'
            when phone_mac = 'ecdf3adcad7c' then 'vivo'
            when phone_mac = 'f025b796195c' then '三星'
            when phone_mac = '78f5fd6e5bf8' then '华为'
            when phone_mac = 'c0ccf8ef75bf' then '苹果' end as phone_brand
      ,scan_cnt
      ,max_rssi
      ,min_rssi
      ,all_rssi/scan_cnt as avg_rssi
      ,detect_duration
      ,case when detect_duration = 0 then 0 else scan_cnt/detect_duration end as scan_freq_detect
      ,10 as actual_duration
      ,scan_cnt/10 as scan_freq_actual
      ,min_time
      ,max_time
  from (
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,max(signal_strength) as max_rssi
              ,min(signal_strength) as min_rssi
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60.0 as detect_duration --单位min
              ,sum(t.signal_strength) as all_rssi
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,cast(signal_strength as int) as signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171207'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171207 14:54:00' and '20171207 15:04:00' 
         group by t.phone_mac
       ) tt       
       
 
==== 南1探针 (1)============
select 5 as distance
      ,phone_mac
      ,case when phone_mac = '3480b3fc5268' then '小米'
            when phone_mac = 'ecdf3adcad7c' then 'vivo'
            when phone_mac = 'f025b796195c' then '三星'
            when phone_mac = '78f5fd6e5bf8' then '华为'
            when phone_mac = 'c0ccf8ef75bf' then '苹果' end as phone_brand
      ,scan_cnt
      ,max_rssi
      ,min_rssi
      ,all_rssi/scan_cnt as avg_rssi
      ,detect_duration
      ,case when detect_duration = 0 then 0 else scan_cnt/detect_duration end as scan_freq_detect
      ,10 as actual_duration
      ,scan_cnt/10 as scan_freq_actual
      ,min_time
      ,max_time
  from (
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,max(signal_strength) as max_rssi
              ,min(signal_strength) as min_rssi
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60.0 as detect_duration --单位min
              ,sum(t.signal_strength) as all_rssi
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,cast(signal_strength as int) as signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171207'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171207 15:48:00' and '20171207 15:58:00' 
         group by t.phone_mac
       ) tt
       
 union all       
       
select 10 as distance
      ,phone_mac
      ,case when phone_mac = '3480b3fc5268' then '小米'
            when phone_mac = 'ecdf3adcad7c' then 'vivo'
            when phone_mac = 'f025b796195c' then '三星'
            when phone_mac = '78f5fd6e5bf8' then '华为'
            when phone_mac = 'c0ccf8ef75bf' then '苹果' end as phone_brand
      ,scan_cnt
      ,max_rssi
      ,min_rssi
      ,all_rssi/scan_cnt as avg_rssi
      ,detect_duration
      ,case when detect_duration = 0 then 0 else scan_cnt/detect_duration end as scan_freq_detect
      ,10 as actual_duration
      ,scan_cnt/10 as scan_freq_actual
      ,min_time
      ,max_time
  from (
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,max(signal_strength) as max_rssi
              ,min(signal_strength) as min_rssi
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60.0 as detect_duration --单位min
              ,sum(t.signal_strength) as all_rssi
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,cast(signal_strength as int) as signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171207'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171207 15:36:00' and '20171207 15:46:00' 
         group by t.phone_mac
       ) tt
              
 union all       
       
select 15 as distance
      ,phone_mac
      ,case when phone_mac = '3480b3fc5268' then '小米'
            when phone_mac = 'ecdf3adcad7c' then 'vivo'
            when phone_mac = 'f025b796195c' then '三星'
            when phone_mac = '78f5fd6e5bf8' then '华为'
            when phone_mac = 'c0ccf8ef75bf' then '苹果' end as phone_brand
      ,scan_cnt
      ,max_rssi
      ,min_rssi
      ,all_rssi/scan_cnt as avg_rssi
      ,detect_duration
      ,case when detect_duration = 0 then 0 else scan_cnt/detect_duration end as scan_freq_detect
      ,10 as actual_duration
      ,scan_cnt/10 as scan_freq_actual
      ,min_time
      ,max_time
  from (
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,max(signal_strength) as max_rssi
              ,min(signal_strength) as min_rssi
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60.0 as detect_duration --单位min
              ,sum(t.signal_strength) as all_rssi
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,cast(signal_strength as int) as signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171207'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171207 16:00:00' and '20171207 16:10:00' 
         group by t.phone_mac
       ) tt
                  
 union all       
       
select 20 as distance
      ,phone_mac
      ,case when phone_mac = '3480b3fc5268' then '小米'
            when phone_mac = 'ecdf3adcad7c' then 'vivo'
            when phone_mac = 'f025b796195c' then '三星'
            when phone_mac = '78f5fd6e5bf8' then '华为'
            when phone_mac = 'c0ccf8ef75bf' then '苹果' end as phone_brand
      ,scan_cnt
      ,max_rssi
      ,min_rssi
      ,all_rssi/scan_cnt as avg_rssi
      ,detect_duration
      ,case when detect_duration = 0 then 0 else scan_cnt/detect_duration end as scan_freq_detect
      ,10 as actual_duration
      ,scan_cnt/10 as scan_freq_actual
      ,min_time
      ,max_time
  from (
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,max(signal_strength) as max_rssi
              ,min(signal_strength) as min_rssi
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60.0 as detect_duration --单位min
              ,sum(t.signal_strength) as all_rssi
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,cast(signal_strength as int) as signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171207'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171207 16:13:00' and '20171207 16:23:00' 
         group by t.phone_mac
       ) tt
                  
 union all       
       
select 25 as distance
      ,phone_mac
      ,case when phone_mac = '3480b3fc5268' then '小米'
            when phone_mac = 'ecdf3adcad7c' then 'vivo'
            when phone_mac = 'f025b796195c' then '三星'
            when phone_mac = '78f5fd6e5bf8' then '华为'
            when phone_mac = 'c0ccf8ef75bf' then '苹果' end as phone_brand
      ,scan_cnt
      ,max_rssi
      ,min_rssi
      ,all_rssi/scan_cnt as avg_rssi
      ,detect_duration
      ,case when detect_duration = 0 then 0 else scan_cnt/detect_duration end as scan_freq_detect
      ,10 as actual_duration
      ,scan_cnt/10 as scan_freq_actual
      ,min_time
      ,max_time
  from (
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,max(signal_strength) as max_rssi
              ,min(signal_strength) as min_rssi
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60.0 as detect_duration --单位min
              ,sum(t.signal_strength) as all_rssi
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,cast(signal_strength as int) as signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171207'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171207 16:30:00' and '20171207 16:40:00' 
         group by t.phone_mac
       ) tt
                  
 union all       
       
select 30 as distance
      ,phone_mac
      ,case when phone_mac = '3480b3fc5268' then '小米'
            when phone_mac = 'ecdf3adcad7c' then 'vivo'
            when phone_mac = 'f025b796195c' then '三星'
            when phone_mac = '78f5fd6e5bf8' then '华为'
            when phone_mac = 'c0ccf8ef75bf' then '苹果' end as phone_brand
      ,scan_cnt
      ,max_rssi
      ,min_rssi
      ,all_rssi/scan_cnt as avg_rssi
      ,detect_duration
      ,case when detect_duration = 0 then 0 else scan_cnt/detect_duration end as scan_freq_detect
      ,10 as actual_duration
      ,scan_cnt/10 as scan_freq_actual
      ,min_time
      ,max_time
  from (
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,max(signal_strength) as max_rssi
              ,min(signal_strength) as min_rssi
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60.0 as detect_duration --单位min
              ,sum(t.signal_strength) as all_rssi
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,cast(signal_strength as int) as signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171207'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171207 16:41:00' and '20171207 16:51:00' 
         group by t.phone_mac
       ) tt


==== 南1探针 (2)============
select 5 as distance
      ,phone_mac
      ,case when phone_mac = '3480b3fc5268' then '小米'
            when phone_mac = 'ecdf3adcad7c' then 'vivo'
            when phone_mac = 'f025b796195c' then '三星'
            when phone_mac = '78f5fd6e5bf8' then '华为'
            when phone_mac = 'c0ccf8ef75bf' then '苹果' end as phone_brand
      ,scan_cnt
      ,max_rssi
      ,min_rssi
      ,all_rssi/scan_cnt as avg_rssi
      ,detect_duration
      ,case when detect_duration = 0 then 0 else scan_cnt/detect_duration end as scan_freq_detect
      ,10 as actual_duration
      ,scan_cnt/10 as scan_freq_actual
      ,min_time
      ,max_time
  from (
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,max(signal_strength) as max_rssi
              ,min(signal_strength) as min_rssi
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60.0 as detect_duration --单位min
              ,sum(t.signal_strength) as all_rssi
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,cast(signal_strength as int) as signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171207'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171207 17:03:00' and '20171207 17:13:00' 
         group by t.phone_mac
       ) tt
       
 union all       
       
select 10 as distance
      ,phone_mac
      ,case when phone_mac = '3480b3fc5268' then '小米'
            when phone_mac = 'ecdf3adcad7c' then 'vivo'
            when phone_mac = 'f025b796195c' then '三星'
            when phone_mac = '78f5fd6e5bf8' then '华为'
            when phone_mac = 'c0ccf8ef75bf' then '苹果' end as phone_brand
      ,scan_cnt
      ,max_rssi
      ,min_rssi
      ,all_rssi/scan_cnt as avg_rssi
      ,detect_duration
      ,case when detect_duration = 0 then 0 else scan_cnt/detect_duration end as scan_freq_detect
      ,10 as actual_duration
      ,scan_cnt/10 as scan_freq_actual
      ,min_time
      ,max_time
  from (
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,max(signal_strength) as max_rssi
              ,min(signal_strength) as min_rssi
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60.0 as detect_duration --单位min
              ,sum(t.signal_strength) as all_rssi
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,cast(signal_strength as int) as signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171207'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171207 17:15:00' and '20171207 17:25:00' 
         group by t.phone_mac
       ) tt
              
 union all       
       
select 15 as distance
      ,phone_mac
      ,case when phone_mac = '3480b3fc5268' then '小米'
            when phone_mac = 'ecdf3adcad7c' then 'vivo'
            when phone_mac = 'f025b796195c' then '三星'
            when phone_mac = '78f5fd6e5bf8' then '华为'
            when phone_mac = 'c0ccf8ef75bf' then '苹果' end as phone_brand
      ,scan_cnt
      ,max_rssi
      ,min_rssi
      ,all_rssi/scan_cnt as avg_rssi
      ,detect_duration
      ,case when detect_duration = 0 then 0 else scan_cnt/detect_duration end as scan_freq_detect
      ,10 as actual_duration
      ,scan_cnt/10 as scan_freq_actual
      ,min_time
      ,max_time
  from (
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,max(signal_strength) as max_rssi
              ,min(signal_strength) as min_rssi
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60.0 as detect_duration --单位min
              ,sum(t.signal_strength) as all_rssi
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,cast(signal_strength as int) as signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171207'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171207 17:27:00' and '20171207 17:37:00' 
         group by t.phone_mac
       ) tt
                  
 union all       
       
select 20 as distance
      ,phone_mac
      ,case when phone_mac = '3480b3fc5268' then '小米'
            when phone_mac = 'ecdf3adcad7c' then 'vivo'
            when phone_mac = 'f025b796195c' then '三星'
            when phone_mac = '78f5fd6e5bf8' then '华为'
            when phone_mac = 'c0ccf8ef75bf' then '苹果' end as phone_brand
      ,scan_cnt
      ,max_rssi
      ,min_rssi
      ,all_rssi/scan_cnt as avg_rssi
      ,detect_duration
      ,case when detect_duration = 0 then 0 else scan_cnt/detect_duration end as scan_freq_detect
      ,10 as actual_duration
      ,scan_cnt/10 as scan_freq_actual
      ,min_time
      ,max_time
  from (
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,max(signal_strength) as max_rssi
              ,min(signal_strength) as min_rssi
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60.0 as detect_duration --单位min
              ,sum(t.signal_strength) as all_rssi
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,cast(signal_strength as int) as signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171207'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171207 18:04:00' and '20171207 18:14:00' 
         group by t.phone_mac
       ) tt
                  
 union all       
       
select 25 as distance
      ,phone_mac
      ,case when phone_mac = '3480b3fc5268' then '小米'
            when phone_mac = 'ecdf3adcad7c' then 'vivo'
            when phone_mac = 'f025b796195c' then '三星'
            when phone_mac = '78f5fd6e5bf8' then '华为'
            when phone_mac = 'c0ccf8ef75bf' then '苹果' end as phone_brand
      ,scan_cnt
      ,max_rssi
      ,min_rssi
      ,all_rssi/scan_cnt as avg_rssi
      ,detect_duration
      ,case when detect_duration = 0 then 0 else scan_cnt/detect_duration end as scan_freq_detect
      ,10 as actual_duration
      ,scan_cnt/10 as scan_freq_actual
      ,min_time
      ,max_time
  from (
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,max(signal_strength) as max_rssi
              ,min(signal_strength) as min_rssi
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60.0 as detect_duration --单位min
              ,sum(t.signal_strength) as all_rssi
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,cast(signal_strength as int) as signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171207'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171207 17:38:00' and '20171207 17:48:00' 
         group by t.phone_mac
       ) tt
                  
 union all       
       
select 30 as distance
      ,phone_mac
      ,case when phone_mac = '3480b3fc5268' then '小米'
            when phone_mac = 'ecdf3adcad7c' then 'vivo'
            when phone_mac = 'f025b796195c' then '三星'
            when phone_mac = '78f5fd6e5bf8' then '华为'
            when phone_mac = 'c0ccf8ef75bf' then '苹果' end as phone_brand
      ,scan_cnt
      ,max_rssi
      ,min_rssi
      ,all_rssi/scan_cnt as avg_rssi
      ,detect_duration
      ,case when detect_duration = 0 then 0 else scan_cnt/detect_duration end as scan_freq_detect
      ,10 as actual_duration
      ,scan_cnt/10 as scan_freq_actual
      ,min_time
      ,max_time
  from (
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,max(signal_strength) as max_rssi
              ,min(signal_strength) as min_rssi
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60.0 as detect_duration --单位min
              ,sum(t.signal_strength) as all_rssi
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,cast(signal_strength as int) as signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171207'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171207 17:52:00' and '20171207 18:02:00' 
         group by t.phone_mac
       ) tt


==== 南1探针 (3)============
select 5 as distance
      ,phone_mac
      ,case when phone_mac = '3480b3fc5268' then '小米'
            when phone_mac = 'ecdf3adcad7c' then 'vivo'
            when phone_mac = 'f025b796195c' then '三星'
            when phone_mac = '78f5fd6e5bf8' then '华为'
            when phone_mac = 'c0ccf8ef75bf' then '苹果' end as phone_brand
      ,scan_cnt
      ,max_rssi
      ,min_rssi
      ,all_rssi/scan_cnt as avg_rssi
      ,detect_duration
      ,case when detect_duration = 0 then 0 else scan_cnt/detect_duration end as scan_freq_detect
      ,10 as actual_duration
      ,scan_cnt/10 as scan_freq_actual
      ,min_time
      ,max_time
  from (
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,max(signal_strength) as max_rssi
              ,min(signal_strength) as min_rssi
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60.0 as detect_duration --单位min
              ,sum(t.signal_strength) as all_rssi
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,cast(signal_strength as int) as signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171207'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171207 18:15:00' and '20171207 18:25:00' 
         group by t.phone_mac
       ) tt
       
 union all       
       
select 10 as distance
      ,phone_mac
      ,case when phone_mac = '3480b3fc5268' then '小米'
            when phone_mac = 'ecdf3adcad7c' then 'vivo'
            when phone_mac = 'f025b796195c' then '三星'
            when phone_mac = '78f5fd6e5bf8' then '华为'
            when phone_mac = 'c0ccf8ef75bf' then '苹果' end as phone_brand
      ,scan_cnt
      ,max_rssi
      ,min_rssi
      ,all_rssi/scan_cnt as avg_rssi
      ,detect_duration
      ,case when detect_duration = 0 then 0 else scan_cnt/detect_duration end as scan_freq_detect
      ,10 as actual_duration
      ,scan_cnt/10 as scan_freq_actual
      ,min_time
      ,max_time
  from (
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,max(signal_strength) as max_rssi
              ,min(signal_strength) as min_rssi
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60.0 as detect_duration --单位min
              ,sum(t.signal_strength) as all_rssi
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,cast(signal_strength as int) as signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171207'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171207 18:26:00' and '20171207 18:36:00' 
         group by t.phone_mac
       ) tt
              
 union all       
       
select 15 as distance
      ,phone_mac
      ,case when phone_mac = '3480b3fc5268' then '小米'
            when phone_mac = 'ecdf3adcad7c' then 'vivo'
            when phone_mac = 'f025b796195c' then '三星'
            when phone_mac = '78f5fd6e5bf8' then '华为'
            when phone_mac = 'c0ccf8ef75bf' then '苹果' end as phone_brand
      ,scan_cnt
      ,max_rssi
      ,min_rssi
      ,all_rssi/scan_cnt as avg_rssi
      ,detect_duration
      ,case when detect_duration = 0 then 0 else scan_cnt/detect_duration end as scan_freq_detect
      ,10 as actual_duration
      ,scan_cnt/10 as scan_freq_actual
      ,min_time
      ,max_time
  from (
        select phone_mac
              ,count(*) as scan_cnt
              ,min(t.time_stamp) as min_time
              ,max(t.time_stamp) as max_time
              ,max(signal_strength) as max_rssi
              ,min(signal_strength) as min_rssi
              ,(unix_timestamp(max(t.time_stamp),'yyyyMMdd HH:mm:ss')-unix_timestamp(min(t.time_stamp),'yyyyMMdd HH:mm:ss'))/60.0 as detect_duration --单位min
              ,sum(t.signal_strength) as all_rssi
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,cast(signal_strength as int) as signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171207'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171207 18:38:00' and '20171207 18:48:00' 
         group by t.phone_mac
       ) tt
