
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