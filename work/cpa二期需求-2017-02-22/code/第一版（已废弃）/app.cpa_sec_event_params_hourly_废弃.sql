
drop table if exists app.cpa_sec_event_params_hourly;

create table app.cpa_sec_event_params_hourly(
product_key     string,
product_name    string,
app_ver_code    string,
app_channel_id  string,
event_name      string,		
param_name      string,		--事件参数名称
param_val       string,		--事件参数值
val_cnt         bigint		--参数值出现次数
)
partitioned by (src_file_day string, src_file_hour string);

-- =======================================================================
with stg_cpa_sec_event_params_hourly as 
(
	select a1.event_name
		  ,a1.param_name
		  ,a1.param_val
	  from (select a.event_name
				  ,a.param_name
				  ,a.param_val
				  ,sum(a.val_cnt) as val_cnt
				  ,row_number()over(partition by a.event_name, a.param_name, a.param_val 
							order by sum(a.val_cnt) desc) param_val_rank
			  from rptdata.fact_kesheng_sec_event_params_hourly a
			 where a.src_file_day = '${SRC_FILE_DAY}' and a.src_file_hour = '{SRC_FILE_HOUR}' 
			   and a.grain_ind = '000'	
			 group by a.event_name, a.param_name, a.param_val
			) a1
	 where a1.param_val_rank <= 1000
)
insert overwrite table app.cpa_sec_event_params_hourly partition (src_file_day = '${SRC_FILE_DAY}', src_file_hour = '${SRC_FILE_HOUR}')
select t1.product_key
	  ,if(t1.product_key=-1, '-1', nvl(b1.product_name,'')) as product_name 
	  ,t1.app_ver_code
	  ,t1.app_channel_id
	  ,t1.event_name
	  ,t1.param_name
	  ,nvl(t1.param_val, '-998') as param_val
	  ,t1.val_cnt 
  from (
		select if(substr(a.grain_ind,1,1)='0', '-1', a.app_channel_id) as app_channel_id 
			  ,if(substr(a.grain_ind,2,1)='0', -1, a.product_key) as product_key 
			  ,if(substr(a.grain_ind,3,1)='0', '-1', a.app_ver_code) as app_ver_code   
			  ,a.event_name
			  ,a.param_name
			  ,b.param_val
			  ,sum(a.val_cnt) as val_cnt  
		  from rptdata.fact_kesheng_sec_event_params_hourly a 
		  left join stg_cpa_sec_event_params_hourly b 
			   on a.event_name = b.event_name and a.param_name = b.param_name and a.param_val = b.param_val
		 where a.src_file_day = '${SRC_FILE_DAY}' and a.src_file_hour = '${SRC_FILE_HOUR}'
		 group by if(substr(a.grain_ind,1,1)='0', '-1', a.app_channel_id) 
				 ,if(substr(a.grain_ind,2,1)='0', -1, a.product_key) 
				 ,if(substr(a.grain_ind,3,1)='0', '-1', a.app_ver_code)
				 ,a.event_name, a.param_name, b.param_val 
		) t1  
 left join mscdata.dim_kesheng_sdk_app_pkg b1 on t1.product_key = b1.product_key 
where d1.product_key is not null or t1.product_key = -1;


-- == 数据源 ===========================================

create table rptdata.fact_kesheng_sec_event_params_hourly 
(
   app_channel_id       string,
   product_key          smallint,
   app_ver_code         string,
   event_name           string,
   param_name           string,
   param_val            string,
   val_cnt              bigint,
   grain_ind            string comment '000-将所渠道、产品、版本汇总成一条记录;
										010-产品;
										011-产品+版本;
										100-渠道;
										110-渠道+产品;
										111-渠道+产品+版本;'
   src_file_day         string,
   src_file_hour        string
);


