
drop table if exists app.cpa_h5_event_hourly;

create table app.cpa_h5_event_hourly (
	stat_time 		       string comment 'YYYYMMDDHH',
	product_name 	       string,
	product_key 	       smallint,
	channel_name 	       string,
	channel_id 		       string,
	page_id 		       string,
	os 				       string,
	uv_num 			       bigint,
	pv_cnt 			       bigint,
	avg_visit_duration_sec bigint,
	download_click_cnt 	   bigint
)
partitioned by (src_file_day string, src_file_hour string) 
row format delimited fields terminated by '31';


--update version=========================================================================================

set mapreduce.job.name=app.cpa_h5_event_hourly_${SRC_FILE_DAY}__${SRC_FILE_HOUR}; 

insert overwrite table app.cpa_h5_event_hourly partition (src_file_day='${SRC_FILE_DAY}', src_file_hour='${SRC_FILE_HOUR}')
select concat('${SRC_FILE_DAY}','${SRC_FILE_HOUR}') as stat_time
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
			,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.page_id) as page_id 
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
			 where a.src_file_day = '${SRC_FILE_DAY}' and a.src_file_hour = '${SRC_FILE_HOUR}' 
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
			 where b.src_file_day = '${SRC_FILE_DAY}' and b.src_file_hour = '${SRC_FILE_HOUR}'
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
			 where c.src_file_day = '${SRC_FILE_DAY}' and c.src_file_hour = '${SRC_FILE_HOUR}'
			) a1  
		group by if(substr(a1.grain_ind,1,1) = '0', -1, a1.product_key)	
				,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id)
				,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.page_id)
				,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.os)
	)t1
inner join mscdata.dim_kesheng_sdk_product d on t1.product_key = d.product_key
left join rptdata.dim_chn e on t1.channel_id = e.chn_id 
where e.chn_id is not null or t1.channel_id='-1';

-- group by 后面的字段要和select中对应的字段的表达式一样

--取关联后d1.product_key不为null(但是t1.product_key=-1时，关联时d1.product_key为null，这种数据也是需要的)  
   
-- 取产品名称的时候从表mscdata.dim_kesheng_sdk_product取
-- 取产品名称，渠道名称的时候需要判断是否为全维度-1，如果是-1那么名称也是-1；可以参考一下CAP的处理方式
-- APP层表的创建脚本没有在git上，需要提交到git

--每一个union all子查询都必须具有相同的列，而且对应的每一个字段的字段类型必须一致



-- 数据源 ==================================================================== --
create table rptdata.fact_kesheng_h5_common_event_hourly
(
   product_key          smallint,
   channel_id           string,
   page_id              string,
   os                   string,
   uv_num               bigint,
   pv_cnt               bigint,
   src_file_day         string,
   grain_ind            string,
   src_file_hour        string
);
hive> select * from rptdata.fact_kesheng_h5_common_event_hourly;
OK
NULL	12345678901	yx01	iOS	1	1	20170208	15

create table rptdata.fact_kesheng_h5_page_visit_event_hourly
(
   product_key          smallint,
   channel_id           string,
   page_id              string,
   os                   string,
   visit_duration_ms    bigint,
   src_file_day         string,
   grain_ind            string,
   src_file_hour        string
);
hive> select * from rptdata.fact_kesheng_h5_page_visit_event_hourly;
OK
NULL	22345678901	yx02	iOS	234	20170208	15

create table rptdata.fact_kesheng_h5_download_event_hourly
(
   product_key          smallint,
   channel_id           string,
   page_id              string,
   os                   string,
   download_click_cnt   bigint,
   src_file_day         string,
   grain_ind            string,
   src_file_hour        string
);
hive> select * from rptdata.fact_kesheng_h5_download_event_hourly;;
OK
NULL		aa	iOS	1	20170208	15

hive> desc mscdata.dim_kesheng_sdk_product;
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