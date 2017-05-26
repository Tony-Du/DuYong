

drop table fact_cdn_flow_daily cascade constraints;
/*==============================================================*/
/* Table: fact_cdn_flow_daily                               */
/*==============================================================*/
create table fact_cdn_flow_daily	--oracle数据库：cdmp_dmt
(
	stat_date				char(8),
	base_all_flow			number(28,2),	
	mobile_data_flow		number(28,2),   
	mobile_4G_flow			number(28,2),   
	non-mobile_data_flow	number(28,2),   
	HSP_flow				number(28,2),   
	HSJ_flow				number(28,2),   
	live_broadcast_flow		number(28,2),   
	abil_output_all_flow	number(28,2),   
	SDK_flow				number(28,2),   
	OPENAPI_flow			number(28,2),   
	third_part_flow			number(28,2),   
	self-build_CDN_flow		number(28,2),   
	migu_video_flow			number(28,2),   
	migu_movie_flow			number(28,2),   
	migu_live_flow			number(28,2)    
);
comment on table fact_cdn_flow_daily is 'CDN流量统计(日)';

comment on column fact_cdn_flow_daily.stat_date is 
'数据周期(天)';
comment on column fact_cdn_flow_daily.base_all_flow is 
'基地总流量     ';                                                                                             
comment on column fact_cdn_flow_daily.mobile_data_flow is 
'移动数据流量';                                                                                                 
comment on column fact_cdn_flow_daily.mobile_4G_flow is 
'移动4G流量';                                                                                                            
comment on column fact_cdn_flow_daily.non-mobile_data_flow is 
'非移动数据流量';                                                                                                  
comment on column fact_cdn_flow_daily.HSP_flow is 
'和视频流量';                                                                                                                                  
comment on column fact_cdn_flow_daily.HSJ_flow is 
'和视界流量';                                                                                                                            
comment on column fact_cdn_flow_daily.live_broadcast_flow is 
'直播流量';                                                                                                                            
comment on column fact_cdn_flow_daily.func_output_all_flow is 
'能力输出总流量';                                                                                   
comment on column fact_cdn_flow_daily.SDK_flow is 
'SDK流量';                                                    
comment on column fact_cdn_flow_daily.OPENAPI_flow is 
'OPENAPI流量';
comment on column fact_cdn_flow_daily.third_part_flow is 
'第三方流量';
comment on column fact_cdn_flow_daily.self-build_CDN_flow is 
'自建CDN流量';
comment on column fact_cdn_flow_daily.migu_video_flow is 
'咪咕视频流量';
comment on column fact_cdn_flow_daily.migu_movie_flow is 
'咪咕影院流量';
comment on column fact_cdn_flow_daily.migu_live_flow is 
'咪咕直播流量';

