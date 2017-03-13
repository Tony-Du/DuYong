
drop table if exists app.cpa_h5_event_daily;

create table app.cpa_h5_event_daily 
(
	stat_time 				string comment 'YYYYMMDD', 
	product_name 			string, 
	product_key 			smallint, 
	channel_name 			string,
	channel_id 				string, 
	page_id 				string, 
	os 						string, 
	uv_num 					bigint, 
	pv_cnt 					bigint, 
	avg_visit_duration_sec 	bigint, 
	download_click_cnt 		bigint
)
partitioned by (src_file_day string)
row format delimited fields terminated by '31';