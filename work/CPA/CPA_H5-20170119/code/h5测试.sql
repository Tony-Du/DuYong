
create table app.cpa_h5_hourly (
stat_time string comment 'YYYYMMDDHH',
product_name string,
product_key tinyint,
channel_name string,
channel_id string,
page_id string,
os string,
uv_num bigint,
pv_cnt bigint,
avg_visit_duration_sec bigint,
download_click_cnt bigint
)
partitioned by (src_file_day string, src_file_hour string)
stored as parquet;

create table app.cpa_h5_daily (
stat_time string comment 'YYYYMMDD',
product_name string,
product_key tinyint,
channel_name string,
channel_id string,
page_id string,
os string,
uv_num bigint,
pv_cnt bigint,
avg_visit_duration_sec bigint,
download_click_cnt bigint
)
partitioned by (src_file_day string)
stored as parquet;
-- ====================================================================
set mapreduce.job.name=app.cpa_h5_hourly_${EXTRACT_DATE}__${EXTRACT_HOUR};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

insert overwrite table app.cpa_h5_hourly partition (src_file_day, src_file_hour)
select concat(a.src_file_day,a.src_file_hour) as stat_time
,a.product_key
,d.product_name
,e.chn_name as channel_name
,a.channel_id
,a.page_id
,a.os
,a.uv_num
,a.pv_cnt
,b.visit_duration_ms/a.uv_num as avg_visit_duration_sec
,c.download_click_cnt
,a.src_file_day
,a.src_file_hour
from rptdata.fact_kesheng_h5_common_hourly a
left join rptdata.fact_kesheng_h5_page_visit_hourly b on a.product_key = b.product_key and a.channel_id = b.channel_id and a.page_id = b.page_id and a.os = b.os
left join rptdata.fact_kesheng_h5_log_event_hourly c on a.product_key = c.product_key and a.channel_id = c.channel_id and a.page_id = c.page_id and a.os = c.os
left join mscdata.dim_kesheng_h5_product d on a.product_key = d.product_key
left join rptdata.dim_chn e on a.channel_id = e.chn_id
where a.src_file_day = b.src_file_day and a.src_file_day = c.src_file_day and a.src_file_day = '20170208' 
and a.src_file_hour = b.src_file_hour and a.src_file_hour = c.src_file_hour and a.src_file_hour = '15' ;

-- ==============================================================================
set mapreduce.job.name=app.cpa_h5_daily_${EXTRACT_DATE};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

insert overwrite table app.cpa_h5_daily partition (src_file_day)
select a.src_file_day as stat_time 
,d.product_name
,a.product_key 
,e.chn_name as channel_name 
,a.channel_id 
,a.page_id 
,a.os 
,a.uv_num 
,a.pv_cnt 
,sum(b.visit_duration_ms)/a.uv_num as avg_visit_duration_sec 
,sum(c.download_click_cnt) as download_click_cnt 
,a.src_file_day
from rptdata.fact_kesheng_h5_common_daily a
left join rptdata.fact_kesheng_h5_page_visit_hourly b on a.product_key = b.product_key and a.channel_id = b.channel_id and a.page_id = b.page_id and a.os = b.os
left join rptdata.fact_kesheng_h5_log_event_hourly c on a.product_key = c.product_key and a.channel_id = c.channel_id and a.page_id = c.page_id and a.os = c.os
left join mscdata.dim_kesheng_h5_product d on a.product_key = d.product_key 
left join rptdata.dim_chn e on a.channel_id = e.chn_id 
where a.src_file_day = b.src_file_day = c.src_file_day = ;
--==========================================================================
set mapreduce.job.name=app.cpa_h5_daily_${EXTRACT_DATE};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

insert overwrite table app.cpa_h5_daily partition (src_file_day)
select max(a.src_file_day) as stat_time 
,max(d.product_name)
,a.product_key 
,max(e.chn_name) as channel_name 
,a.channel_id 
,a.page_id 
,a.os 
,max(a.uv_num) 
,max(a.pv_cnt) 
,sum(b.visit_duration_ms)/max(a.uv_num) as avg_visit_duration_sec 
,sum(c.download_click_cnt) as download_click_cnt	
,a.src_file_day
from rptdata.fact_kesheng_h5_common_daily a
left join rptdata.fact_kesheng_h5_page_visit_hourly b on a.product_key = b.product_key and a.channel_id = b.channel_id and a.page_id = b.page_id and a.os = b.os
left join rptdata.fact_kesheng_h5_log_event_hourly c on a.product_key = c.product_key and a.channel_id = c.channel_id and a.page_id = c.page_id and a.os = c.os
left join mscdata.dim_kesheng_h5_product d on a.product_key = d.product_key 
left join rptdata.dim_chn e on a.channel_id = e.chn_id 
where a.src_file_day = b.src_file_day = c.src_file_day = '20170208'
group by a.product_key, a.channel_id, a.os, a.page_id, a.src_file_day ;


insert overwrite table app.cpa_h5_hourly partition (src_file_day, src_file_hour, grain_ind)
select concat(a.src_file_day,a.src_file_hour) as stat_time
,a.product_key
,d.product_name
,e.chn_name as channel_name
,a.channel_id
,a.page_id
,a.os
,a.uv_num
,a.pv_cnt
,b.visit_duration_ms/a.uv_num as avg_visit_duration_sec
,c.download_click_cnt
,a.src_file_day
,a.src_file_hour
,'0000' grain_ind
from rptdata.fact_kesheng_h5_common_hourly_dy a
left join rptdata.fact_kesheng_h5_page_visit_hourly b on a.product_key = b.product_key and a.channel_id = b.channel_id and a.page_id = b.page_id and a.os = b.os
left join rptdata.fact_kesheng_h5_log_event_hourly c on a.product_key = c.product_key and a.channel_id = c.channel_id and a.page_id = c.page_id and a.os = c.os
left join mscdata.dim_kesheng_h5_product d on a.product_key = d.product_key
left join rptdata.dim_chn e on a.channel_id = e.chn_id
where a.src_file_day = b.src_file_day = c.src_file_day = '20170208' and a.src_file_hour = b.src_file_hour = c.src_file_hour = '15' 
and a.grain_ind = '0000';

http://h5.miguvideo.com/wap/resource/mgsp/yx01.jsp?channelId=12345678901        mgsp    yx01    12345678901     pc      1486712270365   downloadClick   {"a":"b","type":"downloadClick"}       20170210        15
http://h5.miguvideo.com/wap/resource/mgsp/yx01.jsp?channelId=12345678901        mgsp    yx01    12345678901     pc      1486712270595   downloadClick   {"a":"b","type":"downloadClick"}       20170210        15
http://h5.miguvideo.com/wap/resource/mgsp/yx01.jsp?channelId=12345678901        mgsp    yx01    12345678901     pc      1486712270628   pageVisit       {"type":"pageVisit","cookieId":"c2a970a9f30c1c636bfb0b9b8f0fcd981486694431710","IP":"221.181.101.37","startTime":"1486712463136","endTime":"1486712463361","visitTime":"225","src":"downloadClick"}   20170210        15
http://h5.miguvideo.com/wap/resource/mgsp/yx01.jsp?channelId=12345678901        mgsp    yx01    12345678901     pc      1486712465547   common  {"type":"common","IP":"221.181.101.37","domain":"h5.miguvideo.com","title":"咪咕视频","referrer":"","height":"768","width":"1366","colorDepth":"24","_lang":"zh-CN","cookieId":"b133cd783e498b860fa9c2755741e15c1486712702079"}       20170210        15
http://h5.miguvideo.com/wap/resource/mgsp/yx01.jsp?channelId=12345678901        mgsp    yx01    12345678901     pc      1486712475601   pageVisit       {"type":"pageVisit","cookieId":"b133cd783e498b860fa9c2755741e15c1486712702079","IP":"221.181.101.37","startTime":"1486712702030","endTime":"1486712711578","visitTime":"9548","src":"beforeunload"}   20170210        15
http://h5.miguvideo.com/wap/resource/mgsp/yx01.jsp?channelId=12345678901        mgsp    yx01    12345678901     pc      1486712709243   pageVisit       {"type":"pageVisit","cookieId":"c2a970a9f30c1c636bfb0b9b8f0fcd981486694431710","IP":"221.181.101.37","startTime":"1486712463361","endTime":"1486712901978","visitTime":"438617","src":"onbeforeunload"}       20170210        15
http://h5.miguvideo.com/wap/resource/mgsp/yx01.jsp?channelId=12345678901        mgsp    yx01    12345678901     pc      1486712709410   pageVisit       {"type":"pageVisit","cookieId":"c2a970a9f30c1c636bfb0b9b8f0fcd981486694431710","IP":"221.181.101.37","startTime":"1486712901978","endTime":"1486712902130","visitTime":"152","src":"onunload"}        20170210        15