alter table test.probe_system_test_ex add if not exists partition (src_file_day = '20171212')
alter table test.probe_system_test_ex add if not exists partition (src_file_day = '20171213')

==========  八吉岛 Mo站  ==================================

select * from (
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
                 where a.src_file_day = '20171212'
                   and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171212 14:41:00' and '20171212 14:51:00' 
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
                 where a.src_file_day = '20171212'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171212 14:53:00' and '20171212 15:03:00' 
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
                 where a.src_file_day = '20171212'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171212 15:05:00' and '20171212 15:15:00' 
         group by t.phone_mac
       ) tt
)aa
order by phone_brand,distance
 


====================================================


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
                 where a.src_file_day = '20171212'
                   and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171212 15:17:00' and '20171212 15:27:00' 
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
                 where a.src_file_day = '20171212'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171212 15:28:00' and '20171212 15:38:00' 
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
                 where a.src_file_day = '20171212'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171212 15:44:00' and '20171212 15:54:00' 
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
                 where a.src_file_day = '20171212'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171212 16:07:00' and '20171212 16:17:00' 
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
                 where a.src_file_day = '20171212'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171212 15:56:00' and '20171212 16:06:00' 
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
                 where a.src_file_day = '20171212'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171212 16:18:00' and '20171212 16:28:00' 
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
                 where a.src_file_day = '20171212'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171212 16:29:00' and '20171212 16:39:00' 
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
                 where a.src_file_day = '20171212'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171212 16:40:00' and '20171212 16:50:00' 
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
                 where a.src_file_day = '20171212'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171212 16:51:00' and '20171212 17:01:00' 
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
                 where a.src_file_day = '20171212'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171212 17:02:00' and '20171212 17:12:00' 
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
                 where a.src_file_day = '20171212'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171212 17:13:00' and '20171212 17:23:00' 
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
                 where a.src_file_day = '20171212'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171212 17:24:00' and '20171212 17:34:00' 
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
                 where a.src_file_day = '20171212'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171212 17:35:00' and '20171212 17:45:00' 
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
                 where a.src_file_day = '20171212'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171212 17:47:00' and '20171212 17:57:00' 
         group by t.phone_mac
       ) tt
)aa
order by phone_brand,distance


=====================================================




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
                 where a.src_file_day = '20171212'
                   and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171212 17:59:00' and '20171212 18:09:00' 
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
                 where a.src_file_day = '20171212'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171212 18:11:00' and '20171212 18:21:00' 
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
                 where a.src_file_day = '20171212'
                  and phone_mac in (   
                  '3480b3fc5268',  
                  'ecdf3adcad7c',  
                  'f025b796195c',  
                  '78f5fd6e5bf8',  
                  'c0ccf8ef75bf' )  
               ) t
         where t.time_stamp between '20171212 18:22:00' and '20171212 18:32:00' 
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
         where t.time_stamp between '20171213 11:27:00' and '20171213 11:37:00' 
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
         where t.time_stamp between '20171213 11:38:00' and '20171213 11:48:00' 
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
         where t.time_stamp between '20171213 11:50:00' and '20171213 12:01:00' 
         group by t.phone_mac
       ) tt
)aa
order by phone_brand,distance
        