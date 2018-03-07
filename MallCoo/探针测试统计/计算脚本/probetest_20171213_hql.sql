select * from (
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
                 where a.src_file_day = '20171213'
                   and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171213 13:32:00' and '20171213 13:42:00' 
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
                 where a.src_file_day = '20171213'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171213 13:43:00' and '20171213 13:53:00' 
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
                 where a.src_file_day = '20171213'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171213 13:56:00' and '20171213 14:06:00' 
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
                 where a.src_file_day = '20171213'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171213 14:07:00' and '20171213 14:17:00' 
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
                 where a.src_file_day = '20171213'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171213 14:19:00' and '20171213 14:29:00' 
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
                 where a.src_file_day = '20171213'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171213 14:30:00' and '20171213 14:40:00' 
         group by t.phone_mac
       ) tt
)aa
order by phone_brand,distance


================================================================
select * from (
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
                 where a.src_file_day = '20171213'
                   and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171213 14:43:00' and '20171213 14:53:00' 
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
                 where a.src_file_day = '20171213'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171213 14:54:00' and '20171213 15:04:00' 
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
                 where a.src_file_day = '20171213'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171213 15:05:00' and '20171213 15:15:00' 
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
                 where a.src_file_day = '20171213'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171213 15:17:00' and '20171213 15:27:00' 
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
                 where a.src_file_day = '20171213'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171213 15:58:00' and '20171213 16:08:00' 
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
                 where a.src_file_day = '20171213'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171213 16:10:00' and '20171213 16:20:00' 
         group by t.phone_mac
       ) tt
)aa
order by phone_brand,distance


======================================================

select * from (
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
                 where a.src_file_day = '20171213'
                   and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171213 16:22:00' and '20171213 16:32:00' 
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
                 where a.src_file_day = '20171213'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171213 16:34:00' and '20171213 16:44:00' 
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
                 where a.src_file_day = '20171213'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171213 16:57:00' and '20171213 17:07:00' 
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
                 where a.src_file_day = '20171213'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171213 17:08:00' and '20171213 17:18:00' 
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
                 where a.src_file_day = '20171213'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171213 17:20:00' and '20171213 17:30:00' 
         group by t.phone_mac
       ) tt
)aa
order by phone_brand,distance



====== 亮屏，链接商场wifi =======================================================================
       
select * from (
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
                 where a.src_file_day = '20171213'
                   and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171213 17:32:00' and '20171213 17:42:00' 
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
                 where a.src_file_day = '20171213'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171213 17:44:00' and '20171213 17:54:00' 
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
                 where a.src_file_day = '20171213'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171213 17:56:00' and '20171213 18:06:00' 
         group by t.phone_mac
       ) tt
                  
 union all       
       
select 20 as distanc
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
                 where a.src_file_day = '20171213'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171213 18:08:00' and '20171213 18:18:00' 
         group by t.phone_mac
       ) tt
)aa
order by phone_brand,distance
	   