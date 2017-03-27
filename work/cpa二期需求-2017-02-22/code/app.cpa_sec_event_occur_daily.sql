/*

drop table if exists app.cpa_event_occur_daily;

create table app.cpa_event_occur_daily(
product_key      string,
product_name     string,
app_ver_code     string,
app_channel_id   string,
event_name       string,
event_cnt        bigint,
sum_du           decimal(20,2),
avg_du           decimal(20,2),
stat_day string
) partitioned by (src_file_day string);

测试数据：20170315,,2010322
*/

set mapreduce.job.name=app.cpa_event_occur_daily_${SRC_FILE_DAY};
set hive.merge.mapredfiles=true;

insert overwrite table app.cpa_event_occur_daily partition (src_file_day = '${SRC_FILE_DAY}')
select t1.product_key
	  ,if(t1.product_key=-1, '-1', nvl(b1.product_name,'')) product_name
	  ,t1.app_ver_code
	  ,t1.app_channel_id
	  ,t1.event_name
	  ,t1.event_cnt
	  ,t1.sum_du 
	  ,if(t1.event_cnt = 0, 0, round(t1.sum_du/t1.event_cnt,2)) as avg_du
	  ,'${SRC_FILE_DAY}' as stat_day
  from( 
		select if(substr(a.grain_ind,1,1)= '0', '-1', a.app_channel_id) as app_channel_id
			  ,if(substr(a.grain_ind,2,1)= '0', -1, a.product_key) as product_key
			  ,if(substr(a.grain_ind,3,1)= '0', '-1', a.app_ver_code) as app_ver_code
			  ,a.event_name
			  ,sum(a.event_cnt) as event_cnt
			  ,sum(a.sum_du) as sum_du
		 from rptdata.fact_kesheng_event_occur_hourly a 
		where a.src_file_day = '${SRC_FILE_DAY}' 
		group by if(substr(a.grain_ind,1,1)= '0', '-1', a.app_channel_id)
				,if(substr(a.grain_ind,2,1)= '0', -1, a.product_key)
				,if(substr(a.grain_ind,3,1)= '0', '-1', a.app_ver_code)
				,a.event_name
      )t1
 left join mscdata.dim_kesheng_sdk_product b1 on t1.product_key = b1.product_key
where b1.product_key is not null or t1.product_key = -1; 



sqoop export --connect jdbc:oracle:thin:@172.16.14.201:1521:cdmpdb1 \
--username cdmp_dmt \
--password \!2012cdmp_dmt\! \
--table cpa_event_occur_daily \
--export-dir /user/hive/warehouse/app.db/cpa_event_occur_daily/src_file_day=20170322 \
--columns product_key,product_name,app_ver_code,app_channel_id,event_name,event_cnt,sum_du,avg_du,src_file_day \
--input-fields-terminated-by '\001' \
--input-lines-terminated-by '\n' \
--input-null-string '\\N' \
--input-null-non-string '\\N'

--注：columns对应的是oracle中的表

