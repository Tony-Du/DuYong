

/*
create table stg.cpa_event_params_last30_daily_01 (
app_channel_id string,
product_key    string,
app_ver_code   string,
event_name     string,
param_name     string,
param_val      string,
val_cnt		   bigint
);
*/

set mapreduce.job.name=stg.cpa_event_params_last30_daily_01_${SRC_FILE_DAY};
set hive.merge.mapredfiles=true;

insert overwrite table stg.cpa_event_params_last30_daily_01
select if(substr(a.grain_ind,1,1)='0', '-1', a.app_channel_id) as app_channel_id 
	  ,if(substr(a.grain_ind,2,1)='0', -1, a.product_key) as product_key 
	  ,if(substr(a.grain_ind,3,1)='0', '-1', a.app_ver_code) as app_ver_code 
	  ,a.event_name
	  ,a.param_name
	  ,a.param_val
	  ,sum(a.val_cnt) as val_cnt
  from rptdata.fact_kesheng_event_params_hourly a 
 where a.src_file_day <= '${SRC_FILE_DAY}' 
   and a.src_file_day > from_unixtime(unix_timestamp('${SRC_FILE_DAY}','yyyyMMdd')-60*60*24*30,'yyyyMMdd')
 group by if(substr(a.grain_ind,1,1)='0', '-1', a.app_channel_id)
		 ,if(substr(a.grain_ind,2,1)='0', -1, a.product_key)
		 ,if(substr(a.grain_ind,3,1)='0', '-1', a.app_ver_code)
		 ,a.event_name
		 ,a.param_name
		 ,a.param_val;



/*

drop table if exists app.cpa_event_params_last30_daily;	

create table app.cpa_event_params_last30_daily(
product_key    string,
product_name   string,
app_ver_code   string,
app_channel_id string,
event_name     string,
param_name     string,
param_val      string,
val_cnt        bigint,
val_pct        decimal(8,4)
) 
partitioned by (src_file_day string);

*/


set mapreduce.job.name=app.cpa_event_params_last30_daily_${SRC_FILE_DAY};
set hive.merge.mapredfiles=true;

with stg_cpa_event_params_last30_daily as 
(
select a1.event_name
	  ,a1.param_name
      ,a1.param_val		
  from (
		select a.event_name
			  ,a.param_name
			  ,a.param_val
			  ,row_number()over(partition by a.event_name, a.param_name, a.param_val order by a.val_cnt desc) param_val_rank
		  from stg.cpa_event_params_last30_daily_01 a
		 where a.app_channel_id = '-1' and a.product_key= -1 and a.app_ver_code = '-1'																			
		) a1
 where a1.param_val_rank <= 1000
)
insert overwrite table app.cpa_event_params_last30_daily partition (src_file_day = '${SRC_FILE_DAY}')
select t1.product_key
	  ,if(t1.product_key=-1, '-1', nvl(b1.product_name,'')) as product_name 
	  ,t1.app_ver_code
	  ,t1.app_channel_id
	  ,t1.event_name
	  ,t1.param_name
	  ,t1.param_val
	  ,t1.val_cnt
	  ,if (t1.all_val_cnt = 0, 0 ,round(t1.val_cnt/t1.all_val_cnt, 4)) as val_pct 
  from (
		select a.app_channel_id
			  ,a.product_key
			  ,a.app_ver_code
			  ,a.event_name
			  ,a.param_name
			  ,nvl(b.param_val, '-998') as param_val
			  ,sum(a.val_cnt) as val_cnt 
			  ,sum(sum(a.val_cnt)) over(partition by a.app_channel_id, a.product_key, a.app_ver_code, a.event_name, a.param_name) as all_val_cnt
		  from stg.cpa_event_params_last30_daily_01 a 
		  left join stg_cpa_event_params_last30_daily b
			on a.event_name = b.event_name and a.param_name = b.param_name and a.param_val = b.param_val
		 group by a.app_channel_id
				 ,a.product_key
				 ,a.app_ver_code
				 ,a.event_name
				 ,a.param_name
				 ,nvl(b.param_val, '-998')	   
        ) t1
  left join mscdata.dim_kesheng_sdk_product b1 on t1.product_key = b1.product_key 
 where b1.product_key is not null or t1.product_key = -1; 
 
 
 
sqoop export --connect jdbc:oracle:thin:@172.16.14.201:1521:cdmpdb1 \
--username cdmp_dmt \
--password \!2012cdmp_dmt\! \
--table cpa_event_params_last30_daily \
--export-dir /user/hive/warehouse/app.db/cpa_event_params_last30_daily/src_file_day=20170322 \
--columns product_key,product_name,app_ver_code,app_channel_id,event_name,param_name,param_val,val_cnt,val_pct,src_file_day \
--input-fields-terminated-by '\001' \
--input-lines-terminated-by '\n' \
--input-null-string '\\N' \
--input-null-non-string '\\N' 
 