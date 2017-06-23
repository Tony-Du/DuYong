
drop table if exists app.cpa_sec_event_occur_weekly;

create table app.cpa_sec_event_occur_weekly(
product_key    string,
product_name   string,
app_ver_code   string,
app_channel_id string,
event_name     string,
event_cnt      bigint,
sum_du         decimal(20,2),
avg_du         decimal(20,2)
)
partitioned by (src_file_week string);

-- ===================================================================
insert overwrite table app.cpa_sec_event_occur_weekly partition (src_file_week = '${WEEK_START_DAY}')
select t1.product_key
	  ,if(t1.product_key=-1, '-1', nvl(b1.product_name,'')) product_name
	  ,t1.app_ver_code
	  ,t1.app_channel_id
	  ,t1.event_name
	  ,t1.event_cnt
	  ,t1.sum_du 
	  ,if(t1.event_cnt = 0, 0, round(t1.sum_du/t1.event_cnt,2)) as avg_du 
  from( 
		select if(substr(a.grain_ind,1,1)= '0', '-1', a.app_channel_id) as app_channel_id
			  ,if(substr(a.grain_ind,2,1)= '0', -1, a.product_key) as product_key
			  ,if(substr(a.grain_ind,3,1)= '0', '-1', a.app_ver_code) as app_ver_code
			  ,a.event_name
			  ,sum(a.event_cnt) as event_cnt
			  ,sum(a.sum_du) as sum_du
		 from rptdata.fact_kesheng_sec_event_occur_hourly a 
		where a.src_file_day >= '${WEEK_START_DAY}' 
		  and a.src_file_day <= '${WEEK_END_DAY}' 		-- '${WEEK_START_DAY}' = from_unixtime(unix_timestamp('${WEEK_START_DAY}','yyyyMMdd')+6*24*60*60,'yyyyMMdd')
		group by if(substr(a.grain_ind,1,1)= '0', '-1', a.app_channel_id)
				,if(substr(a.grain_ind,2,1)= '0', -1, a.product_key)
				,if(substr(a.grain_ind,3,1)= '0', '-1', a.app_ver_code)
				,a.event_name
      )t1
 left join mscdata.dim_kesheng_sdk_app_pkg b1 on t1.product_key = b1.product_key
where b1.product_key is not null or t1.product_key = -1; 


BDI：
week_start_day   #flow.startDataTime#
week_end_day   tostring(adddays(todate(#flow.startDataTime#, 'yyyyMMdd'), 6) ,'yyyyMMdd')

month_start_day   #flow.startDataTime#+'01'
month_end_day   tostring(adddays(addmonths(todate(#flow.startDataTime#+'01', 'yyyyMMdd'), 1),-1) ,'yyyyMMdd')