
/*
create table stg.cpa_event_params_daily_01 (
app_channel_id string,
product_key    string,
app_ver_code   string,
event_name     string,
param_name     string,
param_val      string,
val_cnt		   bigint
);
*/

set mapreduce.job.name=stg.cpa_event_params_daily_01_${SRC_FILE_DAY};
set hive.merge.mapredfiles=true;

insert overwrite table stg.cpa_event_params_daily_01
select if(substr(a.grain_ind,1,1)='0', '-1', a.app_channel_id) as app_channel_id 
	  ,if(substr(a.grain_ind,2,1)='0', -1, a.product_key) as product_key 
	  ,if(substr(a.grain_ind,3,1)='0', '-1', a.app_ver_code) as app_ver_code 
	  ,a.event_name
	  ,a.param_name
	  ,a.param_val
	  ,sum(a.val_cnt) as val_cnt
  from rptdata.fact_kesheng_event_params_hourly a 
 where a.src_file_day = '${SRC_FILE_DAY}' 
 group by if(substr(a.grain_ind,1,1)='0', '-1', a.app_channel_id)
		 ,if(substr(a.grain_ind,2,1)='0', -1, a.product_key)
		 ,if(substr(a.grain_ind,3,1)='0', '-1', a.app_ver_code)
		 ,a.event_name
		 ,a.param_name
		 ,a.param_val;

/*

drop table if exists app.cpa_event_params_daily;

create table app.cpa_event_params_daily(
product_key string,
product_name string,
app_ver_code string,
app_channel_id string,
event_name string,
param_name string,
param_val string,
val_cnt bigint,
val_pct decimal(8,4)
)
partitioned by (src_file_day string);

*/

set mapreduce.job.name=app.cpa_event_params_daily_${SRC_FILE_DAY};
set hive.merge.mapredfiles=true;


with stg_cpa_event_params_daily as
(
select a1.event_name
      ,a1.param_name
      ,a1.param_val
  from (
        select a.event_name
			  ,a.param_name
			  ,a.param_val
			  ,sum(a.val_cnt)
			  ,row_number()over(partition by a.event_name, a.param_name, a.param_val order by sum(a.val_cnt) desc) param_val_rank
          from rptdata.fact_kesheng_event_params_hourly a
		 where a.src_file_day = '${SRC_FILE_DAY}'
           and a.grain_ind = '000111'
		 group by a.event_name
				 ,a.param_name
				 ,a.param_val
	    ) a1
 where a1.param_val_rank <= 1000
)
insert overwrite table app.cpa_event_params_daily partition (src_file_day = '${SRC_FILE_DAY}')
select t1.product_key
	  ,if(t1.product_key=-1, '-1', nvl(b1.product_name,'')) as product_name
	  ,t1.app_ver_code
	  ,t1.app_channel_id
	  ,t1.event_name
	  ,t1.param_name
	  ,t1.param_val
	  ,t1.val_cnt
	  ,if(t1.all_val_cnt = 0, 0, round(t1.val_cnt/t1.all_val_cnt, 4)) as val_pct
  from (
		select t0.app_channel_id
		      ,t0.product_key
		      ,t0.app_ver_code
		      ,t0.event_name
		      ,t0.param_name
		      ,t0.param_val
		      ,t0.val_cnt
		      ,sum(t0.val_cnt) over(partition by t0.app_channel_id, t0.product_key, t0.app_ver_code, t0.event_name, t0.param_name) as all_val_cnt
		  from (
				select a.app_channel_id
					  ,a.product_key
					  ,a.app_ver_code
					  ,a.event_name
					  ,a.param_name
					  ,nvl(b.param_val,'-998') as param_val
					  ,sum(a.val_cnt) as val_cnt			
				  from stg.cpa_event_params_daily_01 a
				  left join stg_cpa_event_params_daily b	
				    on a.event_name = b.event_name and a.param_name = b.param_name and a.param_val = b.param_val
				 group by a.app_channel_id
					     ,a.product_key
					     ,a.app_ver_code
					     ,a.event_name
					     ,a.param_name
					     ,nvl(b.param_val,'-998')
			   )t0
       ) t1
  left join mscdata.dim_kesheng_sdk_product b1 on t1.product_key = b1.product_key
 where b1.product_key is not null or t1.product_key = -1;



-- 20170315
/*

with stg_cpa_sec_event_params_daily as
(
select a1.event_name
,a1.param_name
,a1.param_val
from (
select a.event_name
,a.param_name
,a.param_val
,sum(a.val_cnt)
,row_number()over(partition by a.event_name, a.param_name, a.param_val order by sum(a.val_cnt) desc) param_val_rank
from temp.rptdata_fact_kesheng_sec_event_params_hourly_dy a
where a.src_file_day = 20170208
and a.grain_ind = '000'
group by a.event_name
,a.param_name
,a.param_val
) a1
where a1.param_val_rank <= 1000
)
insert overwrite table temp.app_cpa_sec_event_params_daily_dy partition (src_file_day = 20170208)
select t1.product_key
,if(t1.product_key=-1, '-1', nvl(b1.product_name,'')) as product_name
,t1.app_ver_code
,t1.app_channel_id
,t1.event_name
,t1.param_name
,nvl(t1.param_val,'-998') as param_val
,t1.val_cnt
,if(t1.all_val_cnt = 0, 0, round(t1.val_cnt/t1.all_val_cnt, 2)) as val_pct
from (
select if(substr(a.grain_ind,1,1)='0', '-1', a.app_channel_id) as app_channel_id
,if(substr(a.grain_ind,2,1)='0', -1, a.product_key) as product_key
,if(substr(a.grain_ind,3,1)='0', '-1', a.app_ver_code) as app_ver_code
,a.event_name
,a.param_name
,b.param_val
,sum(a.val_cnt) as val_cnt
,sum(a.val_cnt) over(partition by a.app_channel_id, a.product_key, a.app_ver_code, a.event_name, a.param_name) as all_val_cnt	--这个会出现错误
from temp.rptdata_fact_kesheng_sec_event_params_hourly_dy a
left join stg_cpa_sec_event_params_daily b
on a.event_name = b.event_name and a.param_name = b.param_name and a.param_val = b.param_val
where a.src_file_day = 20170208
group by if(substr(a.grain_ind,1,1)='0', '-1', a.app_channel_id)
,if(substr(a.grain_ind,2,1)='0', -1, a.product_key)
,if(substr(a.grain_ind,3,1)='0', '-1', a.app_ver_code)
,a.event_name
,a.param_name
,b.param_val
) t1
left join mscdata.dim_kesheng_sdk_app_pkg b1 on t1.product_key = b1.product_key
where b1.product_key is not null or t1.product_key = -1;

-- FAILED: SemanticException Failed to breakup Windowing invocations into Groups. At least 1 group must only depend on input columns. Also check for circular dependencies.
-- Underlying error: org.apache.hadoop.hive.ql.parse.SemanticException: Line 39:7 Invalid column reference 'val_cnt'

*/



