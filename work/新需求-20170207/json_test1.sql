kesheng_sdk_deviceinfo
kesheng_sdk_json_custominfo
kesheng_sdk_sessioninfo

kesheng_sdk_deviceinfo_v	---
kesheng_sdk_json_custominfo_v	---
kesheng_sdk_json_sessioninfo_v	---
kesheng_sdk_sessioninfo_v	---


-- show partitions odsdata.kesheng_sdk_sessioninfo;
src_file_day=20170124/custominfo_type=-1
src_file_day=20170124/custominfo_type=10	-- 首页加载时延
src_file_day=20170124/custominfo_type=11	--（视频详情页加载时延）详情页接口
src_file_day=20170124/custominfo_type=12	--（搜索页面加载时延）
src_file_day=20170124/custominfo_type=14	--（视频平均下载速率）- 平均下载速率
src_file_day=20170124/custominfo_type=15	--（心跳信息）－SDK自行上报
src_file_day=20170124/custominfo_type=16	--（token上报）
src_file_day=20170124/custominfo_type=17	--（视频播放状态上报） 通用接口
src_file_day=20170124/custominfo_type=18	--（订购统计）
src_file_day=20170124/custominfo_type=19	--（图片加载时延）- 图片加载接口
src_file_day=20170124/custominfo_type=2
src_file_day=20170124/custominfo_type=21	--（SDK记录用户退出客户端的行为）
src_file_day=20170124/custominfo_type=22	--（获取播放地址的时长）
src_file_day=20170124/custominfo_type=25	--（下载流量）
src_file_day=20170124/custominfo_type=26	--（点播流量）
src_file_day=20170124/custominfo_type=26000004
src_file_day=20170124/custominfo_type=26000005
src_file_day=20170124/custominfo_type=26000006
src_file_day=20170124/custominfo_type=26000007
src_file_day=20170124/custominfo_type=26000010
src_file_day=20170124/custominfo_type=26000011
src_file_day=20170124/custominfo_type=26000014
src_file_day=20170124/custominfo_type=26000015
src_file_day=20170124/custominfo_type=26000016
src_file_day=20170124/custominfo_type=26000017
src_file_day=20170124/custominfo_type=4	--(登录认证)
src_file_day=20170124/custominfo_type=41
src_file_day=20170124/custominfo_type=42
src_file_day=20170124/custominfo_type=43
src_file_day=20170124/custominfo_type=44
src_file_day=20170124/custominfo_type=5	--（无法连接服务平台）
src_file_day=20170124/custominfo_type=56000004	-- 首帧响应时间
src_file_day=20170124/custominfo_type=56000015	--卡顿详细信息
src_file_day=20170124/custominfo_type=57000000	--播放子会话信息上报
src_file_day=20170124/custominfo_type=58000000	--ErrorEvent上报
src_file_day=20170124/custominfo_type=8	--（节目切片下载失败）
src_file_day=20170124/custominfo_type=9	--（播放失败）- 通用接口
src_file_day=20170124/custominfo_type=PayResult
src_file_day=20170124/custominfo_type=ServiceAuthRequest
src_file_day=20170124/custominfo_type=ServiceAuthResult
src_file_day=20170124/custominfo_type=SubscribeRequest
src_file_day=20170124/custominfo_type=SubscribeResult


-- show partitions odsdata.kesheng_sdk_json_custominfo;
extract_date_label=20170123/extract_hour_label=00
extract_date_label=20170123/extract_hour_label=01
extract_date_label=20170123/extract_hour_label=02
extract_date_label=20170123/extract_hour_label=03
extract_date_label=20170123/extract_hour_label=04
extract_date_label=20170123/extract_hour_label=05
extract_date_label=20170123/extract_hour_label=06
extract_date_label=20170123/extract_hour_label=07
extract_date_label=20170123/extract_hour_label=08
extract_date_label=20170123/extract_hour_label=09
extract_date_label=20170123/extract_hour_label=10
extract_date_label=20170123/extract_hour_label=11
extract_date_label=20170123/extract_hour_label=12
extract_date_label=20170123/extract_hour_label=13
extract_date_label=20170123/extract_hour_label=14
extract_date_label=20170123/extract_hour_label=15
extract_date_label=20170123/extract_hour_label=16
extract_date_label=20170123/extract_hour_label=17
extract_date_label=20170123/extract_hour_label=18
extract_date_label=20170123/extract_hour_label=19
extract_date_label=20170123/extract_hour_label=20
extract_date_label=20170123/extract_hour_label=21
extract_date_label=20170123/extract_hour_label=22
extract_date_label=20170123/extract_hour_label=23



select b.MG_MSG_MEDIA_INFO_TYPE
,b.sessionID
,b.MG_MSG_MEDIA_INFO_VIDEO_CODEC
,b.MG_MSG_MEDIA_INFO_AUDIO_SAMPLERATE
,b.Session
,b.LastSession
,b.channelID
,b.MG_MSG_FFRAME_TIME
,b.MG_MSG_MEDIA_INFO_VIDEO_RESOLUTION
,b.MG_MSG_PROGRAM_URL
,b.MG_MSG_MEDIA_INFO_VIDEO_BITRATE
,b.MG_MSG_GETURL_TIME
,b.MG_MSG_START_TIME
,b.MG_MSG_MEDIA_INFO_VIDEO_FRAMERATE
,b.MG_MSG_MEDIA_INFO_AUDIO_CODEC
,b.MG_MSG_MEDIA_INFO_AUDIO_CHANNELS
,b.MG_MSG_TIME
,b.clientID
from 
odsdata.kesheng_sdk_json_custominfo a
lateral view json_tuple(a.custominfo_json
,'MG_MSG_MEDIA_INFO_TYPE'
,'sessionID'
,'MG_MSG_MEDIA_INFO_VIDEO_CODEC'
,'MG_MSG_MEDIA_INFO_AUDIO_SAMPLERATE'
,'Session'
,'LastSession'
,'channelID'
,'MG_MSG_FFRAME_TIME'
,'MG_MSG_MEDIA_INFO_VIDEO_RESOLUTION'
,'MG_MSG_PROGRAM_URL'
,'MG_MSG_MEDIA_INFO_VIDEO_BITRATE'
,'MG_MSG_GETURL_TIME'
,'MG_MSG_START_TIME'
,'MG_MSG_MEDIA_INFO_VIDEO_FRAMERATE'
,'MG_MSG_MEDIA_INFO_AUDIO_CODEC'
,'MG_MSG_MEDIA_INFO_AUDIO_CHANNELS'
,'MG_MSG_TIME'
,'clientID'
) b 
AS MG_MSG_MEDIA_INFO_TYPE
,sessionID
,MG_MSG_MEDIA_INFO_VIDEO_CODEC
,MG_MSG_MEDIA_INFO_AUDIO_SAMPLERATE
,Session
,LastSession
,channelID
,MG_MSG_FFRAME_TIME
,MG_MSG_MEDIA_INFO_VIDEO_RESOLUTION
,MG_MSG_PROGRAM_URL,MG_MSG_MEDIA_INFO_VIDEO_BITRATE
,MG_MSG_GETURL_TIME,MG_MSG_START_TIME
,MG_MSG_MEDIA_INFO_VIDEO_FRAMERATE
,MG_MSG_MEDIA_INFO_AUDIO_CODEC
,MG_MSG_MEDIA_INFO_AUDIO_CHANNELS
,MG_MSG_TIME
,clientID
WHERE a.custominfo_type = '56000004' and a.extract_date_label = 20170123 and a.extract_hour_label=01 and a.custominfo_pos=326 and a.rowkey = 'hdfs://ns1/user/hadoop/ods/kesheng/20170123/01/kesheng.1485104400020.gz:272481071';


select * from odsdata.kesheng_sdk_json_custominfo a where a.extract_date_label = 20170123 and a.extract_hour_label=01 and a.custominfo_type = '56000004' and a.custominfo_pos=326 and a.rowkey = 'hdfs://ns1/user/hadoop/ods/kesheng/20170123/01/kesheng.1485104400020.gz:272481071';
