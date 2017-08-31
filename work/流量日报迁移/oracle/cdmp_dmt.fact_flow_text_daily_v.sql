create or replace view fact_flow_text_daily_v as
select 
statis_day,
'全天总流量：基地云桥出口 ' || to_char(round(dly_base_all_flow,2), 'fm999999999990.99') || ' TB，第三方CDN ' || to_char(round(dly_third_part_flow,2), 'fm999999999990.99') || ' TB，自建CDN ' || to_char(round(dly_selfbuild_CDN_flow,2), 'fm999999999990.99') || ' TB' || chr(13) || chr(10) || 
'移动234G网络流量 ' || to_char(round(dly_mobile_data_flow,2), 'fm999999999990.99') || ' TB（其中4G流量 ' || to_char(round(dly_mobile_4G_flow,2), 'fm999999999990.99') ||' TB），其他网络流量 '|| to_char(round(dly_nonmobile_data_flow,2), 'fm999999999990.99') || ' TB' || chr(13) || chr(10) ||
'和视频总流量 ' || to_char(round(dly_HSP_flow,2), 'fm999999999990.99') || ' TB' || chr(13) || chr(10) ||
'和视界总流量 ' || to_char(round(dly_HSJ_flow,2), 'fm999999999990.99') || ' TB' || chr(13) || chr(10) ||
'咪咕视频流量 ' || to_char(round(dly_migu_video_flow,2), 'fm999999999990.99') || ' TB' || chr(13) || chr(10) ||
'咪咕影院流量 ' || to_char(round(dly_migu_movie_flow,2), 'fm999999999990.99') || ' TB' || chr(13) || chr(10) ||
'咪咕直播流量 ' || to_char(round(dly_migu_live_flow,2), 'fm999999999990.99') || ' TB' || chr(13) || chr(10) ||
'直播流量 ' || to_char(round(dly_live_broadcast_flow,2), 'fm999999999990.99') || ' TB' || chr(13) || chr(10) ||
'能力输出总流量 ' || to_char(round(dly_abil_output_all_flow,2), 'fm999999999990.99') || ' TB，其中SDK能力输出流量 ' || to_char(round(dly_SDK_flow,2), 'fm999999999990.99') || ' TB，OPENAPI能力输出流量 ' || to_char(round(dly_OPENAPI_flow,2), 'fm999999999990.99') || ' TB' 
as flow
from cdmp_dmt.fact_flow_base_daily_v;