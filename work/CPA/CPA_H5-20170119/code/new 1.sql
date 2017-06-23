分隔符 两个竖线



select a.product_key, a.channel_id, a.page_id, a.os, a.uv_num, a.pv_cnt, 0 visit_duration_ms, 0 download_click_cnt
from rptdata.fact_kesheng_h5_common_hourly a
where a.src_file_day = '20170208' and a.src_file_hour = '15'
union all
select b.product_key, b.channel_id, b.page_id, b.os, 0 uv_num, 0 pv_cnt, b.visit_duration_ms, 0 download_click_cnt
from rptdata.fact_kesheng_h5_page_visit_hourly b
where b.src_file_day = '20170208' and b.src_file_hour = '15'
union all
select c.product_key, c.channel_id, c.page_id, c.os, 0 uv_num, 0 pv_cnt, 0 visit_duration_ms, c.download_click_cnt
from rptdata.fact_kesheng_h5_log_event_hourly c
where c.src_file_day = '20170208' and c.src_file_hour = '15';


select concat(t1.src_file_day,t1.src_file_hour) as stat_time
,t1.product_key
,d.product_name
,e.chn_name as channel_name
,t1.channel_id
,t1.page_id
,t1.os
,t1.uv_num
,t1.pv_cnt
,case when t1.uv_num = 0 then 0 else round(t1.visit_duration_ms/(t1.uv_num*1000)) end as avg_visit_duration_sec
,t1.download_click_cnt
from(
select a.product_key, a.channel_id, a.page_id, a.os, a.uv_num, a.pv_cnt, 0 visit_duration_ms, 0 download_click_cnt, a.src_file_day, a.src_file_hour
from rptdata.fact_kesheng_h5_common_hourly a
where a.src_file_day = 20170208 and a.src_file_hour = 15
union all
select b.product_key, b.channel_id, b.page_id, b.os, 0 uv_num, 0 pv_cnt, b.visit_duration_ms, 0 download_click_cnt, b.src_file_day, b.src_file_hour
from rptdata.fact_kesheng_h5_page_visit_hourly b
where b.src_file_day = 20170208 and b.src_file_hour = 15
union all
select c.product_key, c.channel_id, c.page_id, c.os, 0 uv_num, 0 pv_cnt, 0 visit_duration_ms, c.download_click_cnt, c.src_file_day, c.src_file_hour
from rptdata.fact_kesheng_h5_log_event_hourly c
where c.src_file_day = 20170208 and c.src_file_hour = 15
)t1
left join mscdata.dim_kesheng_h5_product d on t1.product_key = d.product_key
left join rptdata.dim_chn e on t1.channel_id = e.chn_id
where t1.src_file_day = '20170208' and t1.src_file_hour = '15';
-- group by t1.product_key, t1.channel_id, t1.os, t1.page_id;


select a.product_key, a.channel_id, a.page_id, a.os, a.uv_num, a.pv_cnt, 0 visit_duration_ms, 0 download_click_cnt, a.src_file_day
from rptdata.fact_kesheng_h5_common_daily a
where a.src_file_day = 20170208
union all
select b.product_key, b.channel_id, b.page_id, b.os, 0 uv_num, 0 pv_cnt, sum(b.visit_duration_ms) as visit_duration_ms, 0 download_click_cnt, b.src_file_day
from rptdata.fact_kesheng_h5_page_visit_hourly b
where b.src_file_day = 20170208
group by b.product_key, b.channel_id, b.page_id, b.os, b.src_file_day
union all
select c.product_key, c.channel_id, c.page_id, c.os, 0 uv_num, 0 pv_cnt, 0 visit_duration_ms, sum(c.download_click_cnt) as download_click_cnt, c.src_file_day
from rptdata.fact_kesheng_h5_log_event_hourly c
where c.src_file_day = 20170208
group by c.product_key, c.channel_id, c.page_id, c.os, c.src_file_day;




select t1.src_file_day as stat_time
,t1.product_key
,d.product_name
,e.chn_name as channel_name
,t1.channel_id
,t1.page_id
,t1.os
,t1.uv_num
,t1.pv_cnt
,case when t1.uv_num = 0 then 0 else round(t1.visit_duration_ms/(t1.uv_num*1000)) end as avg_visit_duration_sec
,t1.download_click_cnt
from(
select a.product_key, a.channel_id, a.page_id, a.os, a.uv_num, a.pv_cnt, 0 visit_duration_ms, 0 download_click_cnt, a.src_file_day
from rptdata.fact_kesheng_h5_common_daily a
where a.src_file_day = 20170208
union all
select b.product_key, b.channel_id, b.page_id, b.os, 0 uv_num, 0 pv_cnt, sum(b.visit_duration_ms) as visit_duration_ms, 0 download_click_cnt, b.src_file_day
from rptdata.fact_kesheng_h5_page_visit_hourly b
where b.src_file_day = 20170208
group by b.product_key, b.channel_id, b.page_id, b.os, b.src_file_day
union all
select c.product_key, c.channel_id, c.page_id, c.os, 0 uv_num, 0 pv_cnt, 0 visit_duration_ms, sum(c.download_click_cnt) as download_click_cnt, c.src_file_day
from rptdata.fact_kesheng_h5_log_event_hourly c
where c.src_file_day = 20170208
group by c.product_key, c.channel_id, c.page_id, c.os, c.src_file_day
)t1
left join mscdata.dim_kesheng_h5_product d on t1.product_key = d.product_key
left join rptdata.dim_chn e on t1.channel_id = e.chn_id
where t1.src_file_day = 20170208 ;



select a1.product_key
,a1.channel_id
,a1.page_id
,a1.os
,sum(a1.uv_num)
,sum(a1.pv_cnt)
,sum(a1.visit_duration_ms)
,sum(a1.download_click_cnt)
,20170208 as src_file_day
,20170208 as src_file_hour
from(
select a.product_key, a.channel_id, a.page_id, a.os, a.uv_num, a.pv_cnt, 0 visit_duration_ms, 0 download_click_cnt, a.src_file_day, a.src_file_hour
from rptdata.fact_kesheng_h5_common_hourly a
where a.src_file_day = 20170208 and a.src_file_hour = 15
union all
select b.product_key, b.channel_id, b.page_id, b.os, 0 uv_num, 0 pv_cnt, b.visit_duration_ms, 0 download_click_cnt, b.src_file_day, b.src_file_hour
from rptdata.fact_kesheng_h5_page_visit_hourly b
where b.src_file_day = 20170208 and b.src_file_hour = 15
union all
select c.product_key, c.channel_id, c.page_id, c.os, 0 uv_num, 0 pv_cnt, 0 visit_duration_ms, c.download_click_cnt, c.src_file_day, c.src_file_hour
from rptdata.fact_kesheng_h5_log_event_hourly c
where c.src_file_day = 20170208 and c.src_file_hour = 15
) a1
group by a1.product_key, a1.channel_id, a1.os, a1.page_id



select concat(t1.src_file_day,t1.src_file_hour) as stat_time
,t1.product_key
,d.product_name
,e.chn_name as channel_name
,t1.channel_id
,t1.page_id
,t1.os
,t1.uv_num
,t1.pv_cnt
,case when t1.uv_num = 0 then 0 else round(t1.visit_duration_ms/(t1.uv_num*1000)) end as avg_visit_duration_sec
,t1.download_click_cnt
from(
select a1.product_key
,a1.channel_id
,a1.page_id
,a1.os
,sum(a1.uv_num) as uv_num
,sum(a1.pv_cnt) as pv_cnt
,sum(a1.visit_duration_ms) as visit_duration_ms
,sum(a1.download_click_cnt) as download_click_cnt
,20170208 as src_file_day
,15 as src_file_hour
from(
select a.product_key, a.channel_id, a.page_id, a.os, a.uv_num, a.pv_cnt, 0 visit_duration_ms, 0 download_click_cnt, a.src_file_day, a.src_file_hour
from rptdata.fact_kesheng_h5_common_hourly a
where a.src_file_day = 20170208 and a.src_file_hour = 15
union all
select b.product_key, b.channel_id, b.page_id, b.os, 0 uv_num, 0 pv_cnt, b.visit_duration_ms, 0 download_click_cnt, b.src_file_day, b.src_file_hour
from rptdata.fact_kesheng_h5_page_visit_hourly b
where b.src_file_day = 20170208 and b.src_file_hour = 15
union all
select c.product_key, c.channel_id, c.page_id, c.os, 0 uv_num, 0 pv_cnt, 0 visit_duration_ms, c.download_click_cnt, c.src_file_day, c.src_file_hour
from rptdata.fact_kesheng_h5_log_event_hourly c
where c.src_file_day = 20170208 and c.src_file_hour = 15
) a1
group by a1.product_key, a1.channel_id, a1.os, a1.page_id
)t1
left join mscdata.dim_kesheng_h5_product d on t1.product_key = d.product_key
left join rptdata.dim_chn e on t1.channel_id = e.chn_id
where t1.src_file_day = 20170208 and t1.src_file_hour = 15;




select a1.product_key
,a1.channel_id
,a1.page_id
,a1.os
,sum(a1.uv_num)
,sum(a1.pv_cnt)
,sum(a1.visit_duration_ms)
,sum(a1.download_click_cnt)
,20170208 as src_file_day
from(
select a.product_key, a.channel_id, a.page_id, a.os, a.uv_num, a.pv_cnt, 0 visit_duration_ms, 0 download_click_cnt, a.src_file_day
from rptdata.fact_kesheng_h5_common_daily a
where a.src_file_day = 20170208
union all
select b.product_key, b.channel_id, b.page_id, b.os, 0 uv_num, 0 pv_cnt, sum(b.visit_duration_ms) as visit_duration_ms, 0 download_click_cnt, b.src_file_day
from rptdata.fact_kesheng_h5_page_visit_hourly b
where b.src_file_day = 20170208
group by b.product_key, b.channel_id, b.page_id, b.os, b.src_file_day
union all
select c.product_key, c.channel_id, c.page_id, c.os, 0 uv_num, 0 pv_cnt, 0 visit_duration_ms, sum(c.download_click_cnt) as download_click_cnt, c.src_file_day
from rptdata.fact_kesheng_h5_log_event_hourly c
where c.src_file_day = 20170208
group by c.product_key, c.channel_id, c.page_id, c.os, c.src_file_day
) a1
group by a1.product_key, a1.channel_id, a1.os, a1.page_id ;




select t1.src_file_day as stat_time
,t1.product_key
,d.product_name
,e.chn_name as channel_name
,t1.channel_id
,t1.page_id
,t1.os
,t1.uv_num
,t1.pv_cnt
,case when t1.uv_num = 0 then 0 else round(t1.visit_duration_ms/(t1.uv_num*1000)) end as avg_visit_duration_sec
,t1.download_click_cnt
from(
select a1.product_key
,a1.channel_id
,a1.page_id
,a1.os
,sum(a1.uv_num) as uv_num
,sum(a1.pv_cnt) as pv_cnt
,sum(a1.visit_duration_ms) as visit_duration_ms
,sum(a1.download_click_cnt) as download_click_cnt
,20170208 as src_file_day
from(
select a.product_key, a.channel_id, a.page_id, a.os, a.uv_num, a.pv_cnt, 0 visit_duration_ms, 0 download_click_cnt, a.src_file_day
from rptdata.fact_kesheng_h5_common_daily a
where a.src_file_day = 20170208
union all
select b.product_key, b.channel_id, b.page_id, b.os, 0 uv_num, 0 pv_cnt, sum(b.visit_duration_ms) as visit_duration_ms, 0 download_click_cnt, b.src_file_day
from rptdata.fact_kesheng_h5_page_visit_hourly b
where b.src_file_day = 20170208
group by b.product_key, b.channel_id, b.page_id, b.os, b.src_file_day
union all
select c.product_key, c.channel_id, c.page_id, c.os, 0 uv_num, 0 pv_cnt, 0 visit_duration_ms, sum(c.download_click_cnt) as download_click_cnt, c.src_file_day
from rptdata.fact_kesheng_h5_log_event_hourly c
where c.src_file_day = 20170208
group by c.product_key, c.channel_id, c.page_id, c.os, c.src_file_day
) a1
group by a1.product_key, a1.channel_id, a1.os, a1.page_id
)t1
left join mscdata.dim_kesheng_h5_product d on t1.product_key = d.product_key
left join rptdata.dim_chn e on t1.channel_id = e.chn_id
where t1.src_file_day = 20170208 ;



select concat('20170208','15') as stat_time
,t1.product_key
,nvl(d.product_name,'-998') as product_name
,nvl(e.chn_name,'-998') as channel_name
,t1.channel_id
,t1.page_id
,t1.os
,t1.uv_num
,t1.pv_cnt
,case when t1.uv_num = 0 then 0 else round(t1.visit_duration_ms/(t1.uv_num*1000)) end as avg_visit_duration_sec
,t1.download_click_cnt
from(
select a1.product_key
,a1.channel_id
,a1.page_id
,a1.os
,sum(a1.uv_num) as uv_num
,sum(a1.pv_cnt) as pv_cnt
,sum(a1.visit_duration_ms) as visit_duration_ms
,sum(a1.download_click_cnt) as download_click_cnt
from(
select a.product_key, a.channel_id, a.page_id, a.os, a.uv_num, a.pv_cnt, 0 visit_duration_ms, 0 download_click_cnt
from rptdata.fact_kesheng_h5_common_hourly a
where a.src_file_day = 20170208 and a.src_file_hour = 15
union all
select b.product_key, b.channel_id, b.page_id, b.os, 0 uv_num, 0 pv_cnt, b.visit_duration_ms, 0 download_click_cnt
from rptdata.fact_kesheng_h5_page_visit_hourly b
where b.src_file_day = 20170208 and b.src_file_hour = 15
union all
select c.product_key, c.channel_id, c.page_id, c.os, 0 uv_num, 0 pv_cnt, 0 visit_duration_ms, c.download_click_cnt
from rptdata.fact_kesheng_h5_log_event_hourly c
where c.src_file_day = 20170208 and c.src_file_hour = 15
) a1
group by a1.product_key, a1.channel_id, a1.os, a1.page_id
)t1
left join mscdata.dim_kesheng_h5_product d on t1.product_key = d.product_key
left join rptdata.dim_chn e on t1.channel_id = e.chn_id;



select '20170208' as stat_time
,t1.product_key
,nvl(d.product_name,'-998') as product_name
,nvl(e.chn_name,'-998') as channel_name
,t1.channel_id
,t1.page_id
,t1.os
,t1.uv_num
,t1.pv_cnt
,case when t1.uv_num = 0 then 0 else round(t1.visit_duration_ms/(t1.uv_num*1000)) end as avg_visit_duration_sec
,t1.download_click_cnt
from(
select a1.product_key
,a1.channel_id
,a1.page_id
,a1.os
,sum(a1.uv_num) as uv_num
,sum(a1.pv_cnt) as pv_cnt
,sum(a1.visit_duration_ms) as visit_duration_ms
,sum(a1.download_click_cnt) as download_click_cnt
from(
select a.product_key, a.channel_id, a.page_id, a.os, a.uv_num, a.pv_cnt, 0 visit_duration_ms, 0 download_click_cnt
from rptdata.fact_kesheng_h5_common_daily a
where a.src_file_day = '20170208'
union all
select b.product_key, b.channel_id, b.page_id, b.os, 0 uv_num, 0 pv_cnt, sum(b.visit_duration_ms) as visit_duration_ms, 0 download_click_cnt
from rptdata.fact_kesheng_h5_page_visit_hourly b
where b.src_file_day = '20170208'
group by b.product_key, b.channel_id, b.page_id, b.os
union all
select c.product_key, c.channel_id, c.page_id, c.os, 0 uv_num, 0 pv_cnt, 0 visit_duration_ms, sum(c.download_click_cnt) as download_click_cnt
from rptdata.fact_kesheng_h5_log_event_hourly c
where c.src_file_day = '20170208'
group by c.product_key, c.channel_id, c.page_id, c.os
) a1
group by a1.product_key, a1.channel_id, a1.os, a1.page_id
)t1
left join mscdata.dim_kesheng_h5_product d on t1.product_key = d.product_key
left join rptdata.dim_chn e on t1.channel_id = e.chn_id;

select concat(20170208,15) as stat_time
,t1.product_key
,if(t1.product_key='-1','-1',d.product_name) as product_name
,if(t1.channel_id='-1','-1',e.channel_name) as channel_name
,t1.channel_id
,t1.page_id
,t1.os
,t1.uv_num
,t1.pv_cnt
,case when t1.uv_num = 0 then 0 else round(t1.visit_duration_ms/(t1.uv_num*1000)) end as avg_visit_duration_sec
,t1.download_click_cnt
from(
select if(substr(a1.grain_ind,1,1) = '0', '-1', a1.product_key) as product_key
,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id) as channel_id
,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.os) as os
,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.page_id) as page_id
,sum(a1.uv_num) as uv_num
,sum(a1.pv_cnt) as pv_cnt
,sum(a1.visit_duration_ms) as visit_duration_ms
,sum(a1.download_click_cnt) as download_click_cnt
,a1.grain_ind
from(
select a.product_key, a.channel_id, a.page_id, a.os, a.uv_num, a.pv_cnt, 0 visit_duration_ms, 0 download_click_cnt, a.grain_ind
from rptdata.fact_kesheng_h5_common_hourly a
where a.src_file_day = 20170208 and a.src_file_hour = 15
union all
select b.product_key, b.channel_id, b.page_id, b.os, 0 uv_num, 0 pv_cnt, b.visit_duration_ms, 0 download_click_cnt, b.grain_ind
from rptdata.fact_kesheng_h5_page_visit_hourly b
where b.src_file_day = 20170208 and b.src_file_hour = 15
union all
select c.product_key, c.channel_id, c.page_id, c.os, 0 uv_num, 0 pv_cnt, 0 visit_duration_ms, c.download_click_cnt, c.grain_ind
from rptdata.fact_kesheng_h5_log_event_hourly c
where c.src_file_day = 20170208 and c.src_file_hour = 15
) a1
group by if(substr(a1.grain_ind,1,1) = '0', '-1', a1.product_key)
,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id)
,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.os)
,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.page_id)
)t1
left join mscdata.dim_kesheng_sdk_product d on t1.product_key = d.product_key
left join rptdata.dim_chn e on t1.channel_id = e.chn_id;













select if(substr(a1.grain_ind,1,1) = '0', '-1', a1.product_key) as product_key
,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id) as channel_id
,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.os) as os
,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.page_id) as page_id
,sum(a1.uv_num) as uv_num
,sum(a1.pv_cnt) as pv_cnt
,sum(a1.visit_duration_ms) as visit_duration_ms
,sum(a1.download_click_cnt) as download_click_cnt
from(
select a.product_key, a.channel_id, a.page_id, a.os, a.uv_num, a.pv_cnt, 0 visit_duration_ms, 0 download_click_cnt, a.grain_ind
from rptdata.fact_kesheng_h5_common_daily a
where a.src_file_day = 20170208
union all
select b.product_key, b.channel_id, b.page_id, b.os, 0 uv_num, 0 pv_cnt, b.visit_duration_ms, 0 download_click_cnt, b.grain_ind
from rptdata.fact_kesheng_h5_page_visit_hourly b
where b.src_file_day = 20170208
union all
select c.product_key, c.channel_id, c.page_id, c.os, 0 uv_num, 0 pv_cnt, 0 visit_duration_ms, c.download_click_cnt, c.grain_ind
from rptdata.fact_kesheng_h5_log_event_hourly c
where c.src_file_day = 20170208
) a1
group by if(substr(a1.grain_ind,1,1) = '0', '-1', a1.product_key)
,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id)
,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.os)
,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.page_id);


select if(substr(a1.grain_ind,1,1) = '0', '-1', a1.product_key) as product_key
,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id) as channel_id
,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.os) as os
,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.page_id) as page_id
,sum(a1.uv_num) as uv_num
,sum(a1.pv_cnt) as pv_cnt
,sum(a1.visit_duration_ms) as visit_duration_ms
,sum(a1.download_click_cnt) as download_click_cnt
from(
select a.product_key, a.channel_id, a.page_id, a.os, a.uv_num, a.pv_cnt, 0 visit_duration_ms, 0 download_click_cnt, a.grain_ind
from rptdata.fact_kesheng_h5_common_daily a
where a.src_file_day = 20170208
union all
select b.product_key, b.channel_id, b.page_id, b.os, 0 uv_num, 0 pv_cnt, b.visit_duration_ms, 0 download_click_cnt, b.grain_ind
from rptdata.fact_kesheng_h5_page_visit_hourly b
where b.src_file_day = 20170208
union all
select c.product_key, c.channel_id, c.page_id, c.os, 0 uv_num, 0 pv_cnt, 0 visit_duration_ms, c.download_click_cnt, c.grain_ind
from rptdata.fact_kesheng_h5_log_event_hourly c
where c.src_file_day = 20170208
) a1
group by product_key, channel_id,os,page_id;



select concat('20170208','15') as stat_time
,t1.product_key
,if(t1.product_key=-1,-1,nvl(d.product_name,'')) as product_name
,if(t1.channel_id='-1','-1',nvl(e.chn_name,'')) as channel_name
,t1.channel_id
,t1.page_id
,t1.os
,t1.uv_num
,t1.pv_cnt
,case when t1.uv_num = 0 then 0 else round(t1.visit_duration_ms/(t1.uv_num*1000)) end as avg_visit_duration_sec
,t1.download_click_cnt
from(
select if(substr(a1.grain_ind,1,1) = '0', '-1', a1.product_key) as product_key
,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id) as channel_id
,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.os) as os
,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.page_id) as page_id
,sum(a1.uv_num) as uv_num
,sum(a1.pv_cnt) as pv_cnt
,sum(a1.visit_duration_ms) as visit_duration_ms
,sum(a1.download_click_cnt) as download_click_cnt
from(
select a.product_key, a.channel_id, a.page_id, a.os, a.uv_num, a.pv_cnt, 0 visit_duration_ms, 0 download_click_cnt, a.grain_ind
from rptdata.fact_kesheng_h5_common_hourly a
where a.src_file_day = 20170208 and a.src_file_hour = 15
union all
select b.product_key, b.channel_id, b.page_id, b.os, 0 uv_num, 0 pv_cnt, b.visit_duration_ms, 0 download_click_cnt, b.grain_ind
from rptdata.fact_kesheng_h5_page_visit_hourly b
where b.src_file_day = 20170208 and b.src_file_hour = 15
union all
select c.product_key, c.channel_id, c.page_id, c.os, 0 uv_num, 0 pv_cnt, 0 visit_duration_ms, c.download_click_cnt, c.grain_ind
from rptdata.fact_kesheng_h5_log_event_hourly c
where c.src_file_day = 20170208 and c.src_file_hour = 15
) a1
group by if(substr(a1.grain_ind,1,1) = '0', '-1', a1.product_key)
,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id)
,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.os)
,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.page_id)
)t1
left join mscdata.dim_kesheng_sdk_product d on t1.product_key = d.product_key
left join rptdata.dim_chn e on t1.channel_id = e.chn_id
where (d.product_key is not null or t1.product_key=-1)
and (e.chn_id is not null or t1.channel_id='-1');




select '20170208' as stat_time
,t1.product_key
,if(t1.product_key='-1','-1',nvl(d.product_name,'')) as product_name
,if(t1.channel_id='-1','-1',nvl(e.chn_name,'')) as channel_name
,t1.channel_id
,t1.page_id
,t1.os
,t1.uv_num
,t1.pv_cnt
,case when t1.uv_num = 0 then 0 else round(t1.visit_duration_ms/(t1.uv_num*1000)) end as avg_visit_duration_sec
,t1.download_click_cnt
from(
select if(substr(a1.grain_ind,1,1) = '0', '-1', a1.product_key) as product_key
,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id) as channel_id
,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.os) as os
,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.page_id) as page_id
,sum(a1.uv_num) as uv_num
,sum(a1.pv_cnt) as pv_cnt
,sum(a1.visit_duration_ms) as visit_duration_ms
,sum(a1.download_click_cnt) as download_click_cnt
from(
select a.product_key, a.channel_id, a.page_id, a.os, a.uv_num, a.pv_cnt, 0 visit_duration_ms, 0 download_click_cnt, a.grain_ind
from rptdata.fact_kesheng_h5_common_daily a
where a.src_file_day = 20170208
union all
select b.product_key, b.channel_id, b.page_id, b.os, 0 uv_num, 0 pv_cnt, b.visit_duration_ms, 0 download_click_cnt, b.grain_ind
from rptdata.fact_kesheng_h5_page_visit_hourly b
where b.src_file_day = 20170208
union all
select c.product_key, c.channel_id, c.page_id, c.os, 0 uv_num, 0 pv_cnt, 0 visit_duration_ms, c.download_click_cnt, c.grain_ind
from rptdata.fact_kesheng_h5_log_event_hourly c
where c.src_file_day = 20170208
) a1
group by if(substr(a1.grain_ind,1,1) = '0', '-1', a1.product_key)
,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id)
,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.os)
,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.page_id)
)t1
left join mscdata.dim_kesheng_sdk_product d on t1.product_key = d.product_key
left join rptdata.dim_chn e on t1.channel_id = e.chn_id
where (d.product_key is not null or t1.product_key=-1)
and (e.chn_id is not null or t1.channel_id='-1');




select concat('20170208','15') as stat_time
,t1.product_key
,if(t1.product_key=-1, '-1', d.product_name) as product_name
,if(t1.channel_id='-1', '-1', e.chn_name) as channel_name
,t1.channel_id
,t1.page_id
,t1.os
,t1.uv_num
,t1.pv_cnt
,case when t1.uv_num = 0 then 0 else round(t1.visit_duration_ms/(t1.uv_num*1000)) end as avg_visit_duration_sec
,t1.download_click_cnt
from(
select if(substr(a1.grain_ind,1,1) = '0', -1, a1.product_key) as product_key
,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id) as channel_id
,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.os) as os
,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.page_id) as page_id
,sum(a1.uv_num) as uv_num
,sum(a1.pv_cnt) as pv_cnt
,sum(a1.visit_duration_ms) as visit_duration_ms
,sum(a1.download_click_cnt) as download_click_cnt
from(
select a.product_key, a.channel_id, a.page_id, a.os, a.uv_num, a.pv_cnt, 0 visit_duration_ms, 0 download_click_cnt, a.grain_ind
from rptdata.fact_kesheng_h5_common_event_hourly a
where a.src_file_day = 20170208 and a.src_file_hour = 15
union all
select b.product_key, b.channel_id, b.page_id, b.os, 0 uv_num, 0 pv_cnt, b.visit_duration_ms, 0 download_click_cnt, b.grain_ind
from rptdata.fact_kesheng_h5_page_visit_event_hourly b
where b.src_file_day = 20170208 and b.src_file_hour = 15
union all
select c.product_key, c.channel_id, c.page_id, c.os, 0 uv_num, 0 pv_cnt, 0 visit_duration_ms, c.download_click_cnt, c.grain_ind
from rptdata.fact_kesheng_h5_download_event_hourly c
where c.src_file_day = 20170208 and c.src_file_hour = 15
) a1
group by if(substr(a1.grain_ind,1,1) = '0', -1, a1.product_key)
,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id)
,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.os)
,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.page_id)
)t1
left join mscdata.dim_kesheng_sdk_product d on t1.product_key = d.product_key
left join rptdata.dim_chn e on t1.channel_id = e.chn_id
where (d.product_key is not null or t1.product_key=-1)
and (e.chn_id is not null or t1.channel_id='-1');



select '20170208' as stat_time
,t1.product_key	--换位
,if(t1.product_key='-1', '-1', d.product_name) as product_name
,if(t1.channel_id='-1', '-1', e.chn_name) as channel_name
,t1.channel_id
,t1.page_id
,t1.os
,t1.uv_num
,t1.pv_cnt
,case when t1.uv_num = 0 then 0 else round(t1.visit_duration_ms/(t1.uv_num*1000)) end as avg_visit_duration_sec
,t1.download_click_cnt
from(
select if(substr(a1.grain_ind,1,1) = '0', -1, a1.product_key) as product_key
,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id) as channel_id
,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.page_id) as page_id	--换位
,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.os) as os
,sum(a1.uv_num) as uv_num
,sum(a1.pv_cnt) as pv_cnt
,sum(a1.visit_duration_ms) as visit_duration_ms
,sum(a1.download_click_cnt) as download_click_cnt
from(
select a.product_key, a.channel_id, a.page_id, a.os, a.uv_num, a.pv_cnt, 0 visit_duration_ms, 0 download_click_cnt, a.grain_ind
from rptdata.fact_kesheng_h5_common_event_daily a
where a.src_file_day = 20170208
union all
select b.product_key, b.channel_id, b.page_id, b.os, 0 uv_num, 0 pv_cnt, b.visit_duration_ms, 0 download_click_cnt, b.grain_ind
from rptdata.fact_kesheng_h5_page_visit_event_hourly b
where b.src_file_day = 20170208
union all
select c.product_key, c.channel_id, c.page_id, c.os, 0 uv_num, 0 pv_cnt, 0 visit_duration_ms, c.download_click_cnt, c.grain_ind
from rptdata.fact_kesheng_h5_download_event_hourly c
where c.src_file_day = 20170208
) a1
group by if(substr(a1.grain_ind,1,1) = '0', -1, a1.product_key)
,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id)
,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.page_id)
,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.os)
)t1
left join mscdata.dim_kesheng_sdk_product d on t1.product_key = d.product_key -- inner join
left join rptdata.dim_chn e on t1.channel_id = e.chn_id
where (d.product_key is not null or t1.product_key=-1) --去除
and (e.chn_id is not null or t1.channel_id='-1');



select concat('20170208','15') as stat_time
,t1.product_key
,if(t1.product_key=-1, '-1', d.product_name) as product_name
,if(t1.channel_id='-1', '-1', e.chn_name) as channel_name
,t1.channel_id
,t1.page_id
,t1.os
,t1.uv_num
,t1.pv_cnt
,case when t1.uv_num = 0 then 0 else round(t1.visit_duration_ms/(t1.uv_num*1000)) end as avg_visit_duration_sec
,t1.download_click_cnt
from(
select if(substr(a1.grain_ind,1,1) = '0', -1, a1.product_key) as product_key
,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id) as channel_id
,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.os) as os
,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.page_id) as page_id
,sum(a1.uv_num) as uv_num
,sum(a1.pv_cnt) as pv_cnt
,sum(a1.visit_duration_ms) as visit_duration_ms
,sum(a1.download_click_cnt) as download_click_cnt
from(
select a.product_key, a.channel_id, a.page_id, a.os, a.uv_num, a.pv_cnt, 0 visit_duration_ms, 0 download_click_cnt, a.grain_ind
from rptdata.fact_kesheng_h5_common_event_hourly a
where a.src_file_day = 20170208 and a.src_file_hour = 15
union all
select b.product_key, b.channel_id, b.page_id, b.os, 0 uv_num, 0 pv_cnt, b.visit_duration_ms, 0 download_click_cnt, b.grain_ind
from rptdata.fact_kesheng_h5_page_visit_event_hourly b
where b.src_file_day = 20170208 and b.src_file_hour = 15
union all
select c.product_key, c.channel_id, c.page_id, c.os, 0 uv_num, 0 pv_cnt, 0 visit_duration_ms, c.download_click_cnt, c.grain_ind
from rptdata.fact_kesheng_h5_download_event_hourly c
where c.src_file_day = 20170208 and c.src_file_hour = 15
) a1
group by if(substr(a1.grain_ind,1,1) = '0', -1, a1.product_key)
,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id)
,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.os)
,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.page_id)
)t1
left join mscdata.dim_kesheng_sdk_product d on t1.product_key = d.product_key
left join rptdata.dim_chn e on t1.channel_id = e.chn_id
where (d.product_key is not null or t1.product_key=-1)
and (e.chn_id is not null or t1.channel_id='-1');



select '20170208' as stat_time
,t1.product_key
,if(t1.product_key=-1, '-1', d.product_name) as product_name
,if(t1.channel_id='-1', '-1', e.chn_name) as channel_name
,t1.channel_id
,t1.page_id
,t1.os
,t1.uv_num
,t1.pv_cnt
,case when t1.uv_num = 0 then 0 else round(t1.visit_duration_ms/(t1.uv_num*1000)) end as avg_visit_duration_sec
,t1.download_click_cnt
from(
select if(substr(a1.grain_ind,1,1) = '0', -1, a1.product_key) as product_key
,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id) as channel_id
,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.os) as os
,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.page_id) as page_id
,sum(a1.uv_num) as uv_num
,sum(a1.pv_cnt) as pv_cnt
,sum(a1.visit_duration_ms) as visit_duration_ms
,sum(a1.download_click_cnt) as download_click_cnt
from(
select a.product_key, a.channel_id, a.page_id, a.os, a.uv_num, a.pv_cnt, 0 visit_duration_ms, 0 download_click_cnt, a.grain_ind
from rptdata.fact_kesheng_h5_common_event_daily a
where a.src_file_day = 20170208
union all
select b.product_key, b.channel_id, b.page_id, b.os, 0 uv_num, 0 pv_cnt, b.visit_duration_ms, 0 download_click_cnt, b.grain_ind
from rptdata.fact_kesheng_h5_page_visit_event_hourly b
where b.src_file_day = 20170208
union all
select c.product_key, c.channel_id, c.page_id, c.os, 0 uv_num, 0 pv_cnt, 0 visit_duration_ms, c.download_click_cnt, c.grain_ind
from rptdata.fact_kesheng_h5_download_event_hourly c
where c.src_file_day = 20170208
) a1
group by if(substr(a1.grain_ind,1,1) = '0', -1, a1.product_key)
,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id)
,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.os)
,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.page_id)
)t1
left join mscdata.dim_kesheng_sdk_product d on t1.product_key = d.product_key
left join rptdata.dim_chn e on t1.channel_id = e.chn_id
where (d.product_key is not null or t1.product_key=-1)
and (e.chn_id is not null or t1.channel_id='-1');




select concat('20170208','15') as stat_time
,if(t1.product_key=-1, '-1', d.product_name) as product_name
,t1.product_key
,if(t1.channel_id='-1', '-1', e.chn_name) as channel_name
,t1.channel_id
,t1.page_id
,t1.os
,t1.uv_num
,t1.pv_cnt
,case when t1.uv_num = 0 then 0 else round(t1.visit_duration_ms/(t1.uv_num*1000)) end as avg_visit_duration_sec
,t1.download_click_cnt
from(
select if(substr(a1.grain_ind,1,1) = '0', -1, a1.product_key) as product_key
,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id) as channel_id
,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.page_id) as page_id 					--换位
,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.os) as os
,sum(a1.uv_num) as uv_num
,sum(a1.pv_cnt) as pv_cnt
,sum(a1.visit_duration_ms) as visit_duration_ms
,sum(a1.download_click_cnt) as download_click_cnt
from(
select a.product_key
,a.channel_id
,a.page_id
,a.os
,a.uv_num
,a.pv_cnt
,0 visit_duration_ms
,0 download_click_cnt
,a.grain_ind
from rptdata.fact_kesheng_h5_common_event_hourly a
where a.src_file_day = 20170208 and a.src_file_hour = 15
union all
select b.product_key
,b.channel_id
,b.page_id
,b.os
,0 uv_num
,0 pv_cnt
,b.visit_duration_ms
,0 download_click_cnt
,b.grain_ind
from rptdata.fact_kesheng_h5_page_visit_event_hourly b
where b.src_file_day = 20170208 and b.src_file_hour = 15
union all
select c.product_key
,c.channel_id
,c.page_id
,c.os
,0 uv_num
,0 pv_cnt
,0 visit_duration_ms
,c.download_click_cnt
,c.grain_ind
from rptdata.fact_kesheng_h5_download_event_hourly c
where c.src_file_day = 20170208 and c.src_file_hour = 15
) a1
group by if(substr(a1.grain_ind,1,1) = '0', -1, a1.product_key)
,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id)
,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.page_id)
,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.os)
)t1
inner join mscdata.dim_kesheng_sdk_product d on t1.product_key = d.product_key
left join rptdata.dim_chn e on t1.channel_id = e.chn_id
where e.chn_id is not null or t1.channel_id='-1';