

==1==黑屏，wifi打开，不连接商场wifi 但连入其他非商场WIFI========
 
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
              ,(unix_timestamp('20171011 16:04:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 15:54:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
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
         where t.time_stamp between '20171011 15:54:00' and '20171011 16:04:00' 
         group by t.phone_mac
       ) tt
;

==2==黑屏，wifi打开，不连接商场wifi 但连入其他非商场WIFI========
 
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
              ,(unix_timestamp('20171011 16:16:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 16:06:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
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
         where t.time_stamp between '20171011 16:06:00' and '20171011 16:16:00' 
         group by t.phone_mac
       ) tt
;


==3==黑屏，wifi打开，不连接商场wifi 但连入其他非商场WIFI========
 
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
              ,(unix_timestamp('20171011 16:27:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 16:17:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
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
         where t.time_stamp between '20171011 16:17:00' and '20171011 16:27:00' 
         group by t.phone_mac
       ) tt
;


==4==黑屏，wifi打开，不连接商场wifi 但连入其他非商场WIFI========
 
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
              ,(unix_timestamp('20171011 16:38:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 16:28:34','yyyyMMdd HH:mm:ss'))/60 as actual_duration
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
         where t.time_stamp between '20171011 16:28:34' and '20171011 16:38:00' 
         group by t.phone_mac
       ) tt
;

==5==黑屏，wifi打开，不连接商场wifi 但连入其他非商场WIFI========
 
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
              ,(unix_timestamp('20171011 16:51:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 16:41:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
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
         where t.time_stamp between '20171011 16:41:00' and '20171011 16:51:00' 
         group by t.phone_mac
       ) tt
;


==6==黑屏，wifi打开，不连接商场wifi 但连入其他非商场WIFI========
 
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
              ,(unix_timestamp('20171011 17:03:38','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 16:53:08','yyyyMMdd HH:mm:ss'))/60 as actual_duration
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
         where t.time_stamp between '20171011 16:53:08' and '20171011 17:03:38' 
         group by t.phone_mac
       ) tt
;

==7==黑屏，wifi打开，不连接商场wifi 但连入其他非商场WIFI========
 
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
              ,(unix_timestamp('20171011 17:19:33','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 17:09:12','yyyyMMdd HH:mm:ss'))/60 as actual_duration
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
         where t.time_stamp between '20171011 17:09:12' and '20171011 17:19:33' 
         group by t.phone_mac
       ) tt
;


==8==黑屏，wifi打开，不连接商场wifi 但连入其他非商场WIFI========
 
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
              ,(unix_timestamp('20171011 17:33:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 17:23:19','yyyyMMdd HH:mm:ss'))/60 as actual_duration
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
         where t.time_stamp between '20171011 17:23:19' and '20171011 17:33:00' 
         group by t.phone_mac
       ) tt
;



----------------------------------------------



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
              ,(unix_timestamp('20171011 14:21:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 14:11:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
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
         where t.time_stamp between '20171011 14:11:00' and '20171011 14:21:00' 
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
              ,(unix_timestamp('20171011 14:33:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 14:22:38','yyyyMMdd HH:mm:ss'))/60 as actual_duration
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
         where t.time_stamp between '20171011 14:22:38' and '20171011 14:33:00' 
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
              ,(unix_timestamp('20171011 15:46:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 15:35:47','yyyyMMdd HH:mm:ss'))/60 as actual_duration
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
         where t.time_stamp between '20171011 15:35:47' and '20171011 15:46:00' 
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
              ,(unix_timestamp('20171011 14:45:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 14:35:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
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
         where t.time_stamp between '20171011 14:35:00' and '20171011 14:45:00' 
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
              ,(unix_timestamp('20171011 15:31:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 15:21:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
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
         where t.time_stamp between '20171011 15:21:00' and '20171011 15:31:00' 
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
              ,(unix_timestamp('20171011 15:20:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 15:09:45','yyyyMMdd HH:mm:ss'))/60 as actual_duration
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
         where t.time_stamp between '20171011 15:09:45' and '20171011 15:20:00' 
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
              ,(unix_timestamp('20171011 15:08:40','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 14:58:28','yyyyMMdd HH:mm:ss'))/60 as actual_duration
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
         where t.time_stamp between '20171011 14:58:28' and '20171011 15:08:40' 
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
              ,(unix_timestamp('20171011 14:57:00','yyyyMMdd HH:mm:ss')-unix_timestamp('20171011 14:47:00','yyyyMMdd HH:mm:ss'))/60 as actual_duration
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
         where t.time_stamp between '20171011 14:47:00' and '20171011 14:57:00' 
         group by t.phone_mac
       ) tt
;

