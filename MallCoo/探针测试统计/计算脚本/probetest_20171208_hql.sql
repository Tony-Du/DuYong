
===== 南1探针 ========

select 1 as distance
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
                 where a.src_file_day = '20171208'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171208 10:22:00' and '20171208 10:33:00' 
         group by t.phone_mac
       ) tt
       
 union all       
       
select 2 as distance
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
                 where a.src_file_day = '20171208'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171208 10:11:00' and '20171208 10:21:00' 
         group by t.phone_mac
       ) tt
              
 union all       
       
select 3 as distance
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
                 where a.src_file_day = '20171208'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171208 10:34:00' and '20171208 10:44:00' 
         group by t.phone_mac
       ) tt
                  
 union all       
       
select 4 as distance
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
                 where a.src_file_day = '20171208'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171208 10:44:00' and '20171208 10:54:00' 
         group by t.phone_mac
       ) tt
                  
 union all       
       
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
                 where a.src_file_day = '20171208'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171208 10:55:00' and '20171208 11:05:00' 
         group by t.phone_mac
       ) tt
                  
 union all       
       
select 6 as distance
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
                 where a.src_file_day = '20171208'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171208 11:07:00' and '20171208 11:17:00' 
         group by t.phone_mac
       ) tt
                  
 union all       
       
select 7 as distance
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
                 where a.src_file_day = '20171208'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171208 11:18:00' and '20171208 11:28:00' 
         group by t.phone_mac
       ) tt
                         
 union all       
       
select 8 as distance
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
                 where a.src_file_day = '20171208'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171208 11:30:00' and '20171208 11:40:00' 
         group by t.phone_mac
       ) tt
                         
 union all       
       
select 9 as distance
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
                 where a.src_file_day = '20171208'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171208 11:41:00' and '20171208 11:51:00' 
         group by t.phone_mac
       ) tt
	   
	   
=== 八吉岛Mo站======================
select * from (
select 1 as distance
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
                 where a.src_file_day = '20171208'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171208 13:07:00' and '20171208 13:17:00' 
         group by t.phone_mac
       ) tt
       
 union all       
       
select 2 as distance
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
                 where a.src_file_day = '20171208'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171208 12:56:00' and '20171208 13:06:00' 
         group by t.phone_mac
       ) tt
              
 union all       
       
select 3 as distance
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
                 where a.src_file_day = '20171208'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171208 13:18:00' and '20171208 13:28:00' 
         group by t.phone_mac
       ) tt
                  
 union all       
       
select 4 as distance
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
                 where a.src_file_day = '20171208'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171208 13:30:00' and '20171208 13:40:00' 
         group by t.phone_mac
       ) tt
                  
 union all       
       
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
                 where a.src_file_day = '20171208'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171208 13:41:00' and '20171208 13:51:00' 
         group by t.phone_mac
       ) tt
                  
 union all       
       
select 6 as distance
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
                 where a.src_file_day = '20171208'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171208 13:53:00' and '20171208 14:03:00' 
         group by t.phone_mac
       ) tt
                  
 union all       
       
select 7 as distance
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
                 where a.src_file_day = '20171208'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171208 14:04:00' and '20171208 14:14:00' 
         group by t.phone_mac
       ) tt
                         
 union all       
       
select 8 as distance
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
                 where a.src_file_day = '20171208'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171208 14:16:00' and '20171208 14:26:00' 
         group by t.phone_mac
       ) tt
                         
 union all       
       
select 9 as distance
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
                 where a.src_file_day = '20171208'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171208 14:27:00' and '20171208 14:37:00' 
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
                 where a.src_file_day = '20171208'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171208 14:38:00' and '20171208 14:48:00' 
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
                 where a.src_file_day = '20171208'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171208 14:50:00' and '20171208 15:00:00' 
         group by t.phone_mac
       ) tt
)aa
order by phone_brand,distance
       