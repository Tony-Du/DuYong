
drop table if exists app.cpa_h5_event_daily;

create table app.cpa_h5_event_daily --先建表
(
	stat_time 		string comment 'YYYYMMDD', 
	product_name 	string, 
	product_key 	smallint, 
	channel_name 	string,
	channel_id 		string, 
	page_id 		string, 
	os 				string, 
	uv_num 			bigint, 
	pv_cnt 			bigint, 
	avg_visit_duration_sec 	bigint, 
	download_click_cnt 		bigint
)
partitioned by (src_file_day string)
row format delimited fields terminated by '31';


--update version=========================================================================================

set mapreduce.job.name=app.cpa_h5_event_daily_${SRC_FILE_DAY}; --这个你暂时不管，hive的配置

insert overwrite table app.cpa_h5_event_daily partition (src_file_day='${SRC_FILE_DAY}')--插入数据到带分区的表中
select '${SRC_FILE_DAY}' as stat_time
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
	select if(substr(a1.grain_ind,1,1) = '0', -1, a1.product_key) as product_key --第二层子查询  差不多建一张表的流程就这样
		  ,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id) as channel_id
		  ,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.page_id) as page_id
		  ,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.os) as os
	      ,sum(a1.uv_num) as uv_num
	      ,sum(a1.pv_cnt) as pv_cnt
	      ,sum(a1.visit_duration_ms) as visit_duration_ms
	      ,sum(a1.download_click_cnt) as download_click_cnt
	  from(
			select a.product_key --第一层子查询
				  ,a.channel_id
				  ,a.page_id
				  ,a.os
				  ,a.uv_num
				  ,a.pv_cnt
				  ,0 visit_duration_ms
				  ,0 download_click_cnt
				  ,a.grain_ind 
			  from rptdata.fact_kesheng_h5_common_event_daily a 
			 where a.src_file_day = '${SRC_FILE_DAY}' 
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
			 where b.src_file_day = '${SRC_FILE_DAY}' 
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
			 where c.src_file_day = '${SRC_FILE_DAY}' 
		  ) a1 
	  group by if(substr(a1.grain_ind,1,1) = '0', -1, a1.product_key)
			  ,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id)
			  ,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.page_id)
			  ,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.os)
	)t1 
inner join mscdata.dim_kesheng_sdk_product d on t1.product_key = d.product_key 
 left join rptdata.dim_chn e on t1.channel_id = e.chn_id 
where e.chn_id is not null or t1.channel_id='-1';




-- 数据源 =======================================
create table rptdata.fact_kesheng_h5_common_event_daily a 
(
   product_key          tiny,
   channel_id           string,
   page_id              string,
   os                   string,
   uv_num               bigint,
   pv_cnt               bigint,
   src_file_day         string
);
hive> select * from rptdata.fact_kesheng_h5_common_event_daily;
OK
NULL	12345678901	yx01	iOS	1	1	20170208

create table rptdata.fact_kesheng_h5_page_visit_event_hourly
(
   product_key          tiny,
   channel_id           string,
   page_id              string,
   os                   string,
   visit_duration_ms    bigint,
   src_file_day         string,
   src_file_hour        string
);

create table rptdata.fact_kesheng_h5_download_event_hourly
(
   product_key          tiny,
   channel_id           string,
   page_id              string,
   os                   string,
   download_click_cnt   bigint,
   src_file_day         string,
   src_file_hour        string
);

hive> desc mscdata.dim_kesheng_sdk_product;		d
OK
product_key             int                                         
product_name            string                                      
create_day              string


desc rptdata.dim_chn;  e
OK
chn_id              	string              	                    
chn_name            	string              	                    
chn_type            	string              	                    
chn_attr_1_id       	string              	                    
chn_attr_1_name     	string              	                    
chn_attr_2_id       	string              	                    
chn_attr_2_name     	string              	                    
chn_attr_3_id       	string              	                    
chn_attr_3_name     	string              	                    
chn_attr_4_id       	string              	                    
chn_attr_4_name     	string              	                    
chn_attr_5_id       	string              	                    
chn_attr_5_name     	string              	                    
chn_type_id         	string              	                    
chn_class           	string              	                    
dw_create_by        	string              	                    
dw_create_time      	string              	                    
dw_update_by        	string              	                    
dw_update_time      	string              	                    
dw_delete_flag      	string              	                    
dw_crc              	string