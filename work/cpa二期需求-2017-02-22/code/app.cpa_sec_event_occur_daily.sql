
drop table if exists app.cpa_sec_event_occur_daily;

create table app.cpa_sec_event_occur_daily(
product_key      string,
product_name     string,
app_ver_code     string,
app_channel_id   string,
event_name       string,
event_cnt        bigint,
sum_du           decimal(20,2),
avg_du           decimal(20,2)
) partitioned by (src_file_day string);

-- ===================================================================
insert overwrite table app.cpa_sec_event_occur_daily partition (src_file_day = '${SRC_FILE_DAY}')
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
		where a.src_file_day = '${SRC_FILE_DAY}' 
		group by if(substr(a.grain_ind,1,1)= '0', '-1', a.app_channel_id)
				,if(substr(a.grain_ind,2,1)= '0', -1, a.product_key)
				,if(substr(a.grain_ind,3,1)= '0', '-1', a.app_ver_code)
				,a.event_name
      )t1
 left join mscdata.dim_kesheng_sdk_app_pkg b1 on t1.product_key = b1.product_key
where b1.product_key is not null or t1.product_key = -1;  	   

-- == 数据源 =====================================
create table rptdata.fact_kesheng_sec_event_occur_hourly
(
   app_channel_id       string,
   product_key          smallint,
   app_ver_code         string,
   event_name           string,
   event_cnt            bigint,
   sum_du               decimal(20,2),
   avg_du               decimal(20,2),
   grain_ind            string comment '000-将所渠道、产品、版本汇总成一条记录;
										010-产品;
										011-产品+版本;
										100-渠道;
										110-渠道+产品;
										111-渠道+产品+版本;' 
   src_file_day         string,
   src_file_hour        string
);

hive> desc mscdata.dim_kesheng_sdk_app_pkg;
OK
app_pkg_name        	string              	                    
app_os_type         	string              	                    
product_ver_name    	string              	                    
product_key         	int                 	                    
product_name        	string              	                    
create_day          	string

