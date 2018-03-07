


==1==黑屏，wifi打开并连接商场WIFI状态========
 
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
              ,(unix_timestamp('20171011 13:51:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 13:40:52','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171011'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8',  --华为 
                   'c0ccf8ef75c0' )  --iphone
               ) t
         where t.time_stamp between '20171011 13:40:52' and '20171011 13:51:00' 
         group by t.phone_mac
       ) tt
;

==2==黑屏，wifi打开并连接商场WIFI状态========
 
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
              ,(unix_timestamp('20171011 14:02:16','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 13:52:16','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171011'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8',  --华为 
                   'c0ccf8ef75c0' )  --iphone
               ) t
         where t.time_stamp between '20171011 13:52:16' and '20171011 14:02:16' 
         group by t.phone_mac
       ) tt
;

------------------------






==1==黑屏，wifi打开，不连接商场wifi 也不连入其他非商场WIFI========
 
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
              ,(unix_timestamp('20171011 12:00:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 11:50:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171011'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8',  --华为 
                   'c0ccf8ef75c0' )  --iphone
               ) t
         where t.time_stamp between '20171011 11:50:00' and '20171011 12:00:00' 
         group by t.phone_mac
       ) tt
;

==2==黑屏，wifi打开，不连接商场wifi 也不连入其他非商场WIFI========
 
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
              ,(unix_timestamp('20171011 12:13:07','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 12:02:33','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171011'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8',  --华为 
                   'c0ccf8ef75c0' )  --iphone
               ) t
         where t.time_stamp between '20171011 12:02:33' and '20171011 12:13:07' 
         group by t.phone_mac
       ) tt
;


==3==黑屏，wifi打开，不连接商场wifi 也不连入其他非商场WIFI========
 
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
              ,(unix_timestamp('20171011 12:25:44','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 12:15:30','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171011'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8',  --华为 
                   'c0ccf8ef75c0' )  --iphone
               ) t
         where t.time_stamp between '20171011 12:15:30' and '20171011 12:25:44' 
         group by t.phone_mac
       ) tt
;


==4==黑屏，wifi打开，不连接商场wifi 也不连入其他非商场WIFI========
 
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
              ,(unix_timestamp('20171011 12:37:35','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 12:27:40','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171011'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8',  --华为 
                   'c0ccf8ef75c0' )  --iphone
               ) t
         where t.time_stamp between '20171011 12:27:40' and '20171011 12:37:35' 
         group by t.phone_mac
       ) tt
;

==5==黑屏，wifi打开，不连接商场wifi 也不连入其他非商场WIFI========
 
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
              ,(unix_timestamp('20171011 12:50:07','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 12:39:24','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171011'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8',  --华为 
                   'c0ccf8ef75c0' )  --iphone
               ) t
         where t.time_stamp between '20171011 12:39:24' and '20171011 12:50:07' 
         group by t.phone_mac
       ) tt
;


==6==黑屏，wifi打开，不连接商场wifi 也不连入其他非商场WIFI========
 
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
              ,(unix_timestamp('20171011 13:02:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 12:51:26','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171011'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8',  --华为 
                   'c0ccf8ef75c0' )  --iphone
               ) t
         where t.time_stamp between '20171011 12:51:26' and '20171011 13:02:00' 
         group by t.phone_mac
       ) tt
;

==7==黑屏，wifi打开，不连接商场wifi 也不连入其他非商场WIFI========
 
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
              ,(unix_timestamp('20171011 13:13:42','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 13:03:20','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171011'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8',  --华为 
                   'c0ccf8ef75c0' )  --iphone
               ) t
         where t.time_stamp between '20171011 13:03:20' and '20171011 13:13:42' 
         group by t.phone_mac
       ) tt
;


==8==黑屏，wifi打开，不连接商场wifi 也不连入其他非商场WIFI========
 
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
              ,(unix_timestamp('20171011 13:24:43','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 13:14:18','yyyyMMdd HH:mm:ss'))/60 as actual_duration
              ,sum(t.signal_strength) as all_signal_strength
          from (
                select from_unixtime(cast(time_stamp as bigint), 'yyyyMMdd HH:mm:ss') as time_stamp
                      ,phone_mac
                      ,signal_strength
                      ,src_file_day
                  from test.probe_system_test_ex a
                 where a.src_file_day = '20171011'
                   and phone_mac in (   
                   '3480b3fc5268',  --小米手机
                   'ecdf3adcad7c',  --vivo
                   'f025b796195c',  --三星
                   '78f5fd6e5bf8',  --华为 
                   'c0ccf8ef75c0' )  --iphone
               ) t
         where t.time_stamp between '20171011 13:14:18' and '20171011 13:24:43' 
         group by t.phone_mac
       ) tt
;






























