create or replace view fact_flow_base_daily_v as
with flow_raw as (
select statis_day, metric_name, metric_value 
from cdmp_dmt.fact_high_level_metric_daily 
where metric_cat_name = '流量日报' 
and dw_del_flag = 'N'
)
select "STATIS_DAY",
       "DLY_BASE_ALL_FLOW",
       "DLY_MOBILE_DATA_FLOW",
       "DLY_MOBILE_4G_FLOW",
       "DLY_NONMOBILE_DATA_FLOW",
       "DLY_HSP_FLOW",
       "DLY_HSJ_FLOW",
       "DLY_LIVE_BROADCAST_FLOW",
       "DLY_ABIL_OUTPUT_ALL_FLOW",
       "DLY_SDK_FLOW",
       "DLY_OPENAPI_FLOW",
       "DLY_THIRD_PART_FLOW",
       "DLY_SELFBUILD_CDN_FLOW",
       "DLY_MIGU_VIDEO_FLOW",
       "DLY_MIGU_MOVIE_FLOW",
       "DLY_MIGU_LIVE_FLOW"
       from flow_raw
pivot (                     --oracle 行转列
sum(metric_value) 
for metric_name 
in (    
    '日_基地_总流量'             as dly_base_all_flow,
    '日_移动_数据流量'           as dly_mobile_data_flow,
    '日_移动4G_数据流量'         as dly_mobile_4G_flow,  
    '日_非移动_数据流量'         as dly_nonmobile_data_flow,
    '日_和视频_流量'             as dly_HSP_flow,   
    '日_和视界_流量'             as dly_HSJ_flow,
    '日_直播_流量'               as dly_live_broadcast_flow, 
    '日_能力输出_总流量'         as dly_abil_output_all_flow,    
    '日_SDK_流量'                as dly_SDK_flow,
    '日_OPENAPI_流量'            as dly_OPENAPI_flow,
    '日_第三方_流量'             as dly_third_part_flow,  
    '日_自建CDN_流量'            as dly_selfbuild_CDN_flow,      
    '日_咪咕视频_流量'           as dly_migu_video_flow,
    '日_咪咕影院_流量'           as dly_migu_movie_flow,
    '日_咪咕直播_流量'           as dly_migu_live_flow
    )
);


create or replace view fact_flow_text_daily_v as
select 
statis_day,
'全天总流量：基地云桥出口 ' || round(dly_base_all_flow,2) || ' TB，第三方CDN ' || round(dly_third_part_flow,2) || ' TB，自建CDN ' || round(dly_selfbuild_CDN_flow,2) || ' TB' || chr(13) || chr(10) || 
'移动234G网络流量 ' || round(dly_mobile_data_flow,2) || ' TB（其中4G流量 ' || round(dly_mobile_4G_flow,2) ||' TB），其他网络流量 '|| round(dly_nonmobile_data_flow,2) || ' TB' || chr(13) || chr(10) ||
'和视频总流量 ' || round(dly_HSP_flow,2) || ' TB' || chr(13) || chr(10) ||
'和视界总流量 ' || round(dly_HSJ_flow,2) || ' TB' || chr(13) || chr(10) ||
'咪咕视频流量 ' || round(dly_migu_video_flow,2) || ' TB' || chr(13) || chr(10) ||
'咪咕影院流量 ' || round(dly_migu_movie_flow,2) || ' TB' || chr(13) || chr(10) ||
'咪咕直播流量 ' || round(dly_migu_live_flow,2) || ' TB' || chr(13) || chr(10) ||
'直播流量 ' || round(dly_live_broadcast_flow,2) || ' TB' || chr(13) || chr(10) ||
'能力输出总流量 ' || round(dly_abil_output_all_flow,2) || ' TB，其中SDK能力输出流量 ' || round(dly_SDK_flow,2) || ' TB，OPENAPI能力输出流量 ' || round(dly_OPENAPI_flow,2) || ' TB' 
as flow
from cdmp_dmt.fact_flow_base_daily_v;




--to_char(round(dly_SDK_flow,2),'fm999999999990.99')








