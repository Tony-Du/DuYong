drop table if exists intdata.kesheng_sdk_play_first_play_hourly_dy;

create table intdata.kesheng_sdk_play_first_play_hourly_dy (
clientId string
,imei string
,udid string
,idfa string
,idfv string
,appVersion string
,apppkg string
,networkType string
,os string
,appchannel string
,installationID string
,MG_MSG_TIME string
,Session string
,LastSession string
,MG_MSG_START_TIME string
,MG_MSG_FFRAME_TIME string
,MG_MSG_GETURL_TIME string
,MG_MSG_PROGRAM_URL string
,MG_MSG_MEDIA_INFO_TYPE string
,MG_MSG_MEDIA_INFO_VIDEO_CODEC string
,MG_MSG_MEDIA_INFO_VIDEO_RESOLUTION string
,MG_MSG_MEDIA_INFO_VIDEO_FRAMERATE string
,MG_MSG_MEDIA_INFO_VIDEO_BITRATE string
,MG_MSG_MEDIA_INFO_AUDIO_CODEC string
,MG_MSG_MEDIA_INFO_AUDIO_CHANNELS string
,MG_MSG_MEDIA_INFO_AUDIO_SAMPLERATE string
)
partitioned by (src_file_day string, src_file_hour string)
stored as parquet;

-- ======================================================================================== --

create or replace view int.kesheng_sdk_play_first_play_hourly_v_dy as
select a.clientid 
,a.imei 
,a.udid 
,a.idfa 
,a.idfv 
,a.appversion 
,a.apppkg 
,a.networktype 
,a.os 
,a.appchannel 
,a.installationid 
,a.src_file_day 
,a.src_file_hour 
,b.MG_MSG_TIME 
,b.Session 
,b.LastSession 
,b.MG_MSG_START_TIME 
,b.MG_MSG_FFRAME_TIME 
,b.MG_MSG_GETURL_TIME 
,b.MG_MSG_PROGRAM_URL 
,b.MG_MSG_MEDIA_INFO_TYPE 
,b.MG_MSG_MEDIA_INFO_VIDEO_CODEC 
,b.MG_MSG_MEDIA_INFO_VIDEO_RESOLUTION 
,b.MG_MSG_MEDIA_INFO_VIDEO_FRAMERATE 
,b.MG_MSG_MEDIA_INFO_VIDEO_BITRATE 
,b.MG_MSG_MEDIA_INFO_AUDIO_CODEC 
,b.MG_MSG_MEDIA_INFO_AUDIO_CHANNELS 
,b.MG_MSG_MEDIA_INFO_AUDIO_SAMPLERATE 
from ods.kesheng_sdk_json_sessioninfo_v a
lateral view json_tuple(a.custominfo_json 
,'MG_MSG_TIME' 
,'Session' 
,'LastSession' 
,'MG_MSG_START_TIME' 
,'MG_MSG_FFRAME_TIME' 
,'MG_MSG_GETURL_TIME' 
,'MG_MSG_PROGRAM_URL' 
,'MG_MSG_MEDIA_INFO_TYPE' 
,'MG_MSG_MEDIA_INFO_VIDEO_CODEC' 
,'MG_MSG_MEDIA_INFO_VIDEO_RESOLUTION' 
,'MG_MSG_MEDIA_INFO_VIDEO_FRAMERATE' 
,'MG_MSG_MEDIA_INFO_VIDEO_BITRATE' 
,'MG_MSG_MEDIA_INFO_AUDIO_CODEC' 
,'MG_MSG_MEDIA_INFO_AUDIO_CHANNELS' 
,'MG_MSG_MEDIA_INFO_AUDIO_SAMPLERATE' 
) b as 
MG_MSG_TIME 
,Session 
,LastSession 
,MG_MSG_START_TIME 
,MG_MSG_FFRAME_TIME 
,MG_MSG_GETURL_TIME 
,MG_MSG_PROGRAM_URL 
,MG_MSG_MEDIA_INFO_TYPE 
,MG_MSG_MEDIA_INFO_VIDEO_CODEC 
,MG_MSG_MEDIA_INFO_VIDEO_RESOLUTION 
,MG_MSG_MEDIA_INFO_VIDEO_FRAMERATE 
,MG_MSG_MEDIA_INFO_VIDEO_BITRATE 
,MG_MSG_MEDIA_INFO_AUDIO_CODEC 
,MG_MSG_MEDIA_INFO_AUDIO_CHANNELS 
,MG_MSG_MEDIA_INFO_AUDIO_SAMPLERATE 
WHERE a.custominfo_type = '56000004' ;

--======================================================================================================================--
set mapreduce.job.name=intdata.kesheng_sdk_play_first_play_hourly_dy_${EXTRACT_DATE}_${EXTRACT_HOUR};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

add jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar; 

insert overwrite table intdata.kesheng_sdk_play_first_play_hourly_dy partition (src_file_day, src_file_hour)
select a.clientid
,a.imei
,a.udid
,a.idfa
,a.idfv
,a.appVersion
,a.apppkg
,a.networktype
,a.os
,a.appchannel
,a.installationid
,a.MG_MSG_TIME
,a.Session
,a.LastSession
,a.MG_MSG_START_TIME
,a.MG_MSG_FFRAME_TIME
,a.MG_MSG_GETURL_TIME
,a.MG_MSG_PROGRAM_URL
,a.MG_MSG_MEDIA_INFO_TYPE
,a.MG_MSG_MEDIA_INFO_VIDEO_CODEC
,a.MG_MSG_MEDIA_INFO_VIDEO_RESOLUTION
,a.MG_MSG_MEDIA_INFO_VIDEO_FRAMERATE
,a.MG_MSG_MEDIA_INFO_VIDEO_BITRATE
,a.MG_MSG_MEDIA_INFO_AUDIO_CODEC
,a.MG_MSG_MEDIA_INFO_AUDIO_CHANNELS
,a.MG_MSG_MEDIA_INFO_AUDIO_SAMPLERATE
,a.src_file_day
,a.src_file_hour
from int.kesheng_sdk_play_first_play_hourly_v_dy a
WHERE a.src_file_day = '${EXTRACT_DATE}' and a.src_file_hour = '${EXTRACT_HOUR}';


-- 没有解决
报错：Caused by: java.lang.ClassNotFoundException: Class org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe not found
add jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;



select * from ods.kesheng_sdk_play_first_play_hourly_v_dy where src_file_day=20170123 and src_file_hour =01;

111111	
f9c15d89d3c04af1b40df79480b19c97	
E43BBD61-CF7F-4EBD-A2DD-5571239059D4	
00000000-0000-0000-0000-000000000000	
CB7517A5-09FD-46F1-BC1E-4CDBF5CA06FA	
3.1.1	
com.wondertek.hecmccmobile	
NULL	
NULL	
NULL	
NULL	
20170123	
1	
1485103528856	
bbbffe6f8ede99fd54f98d3e5c3720b8	
1c707034fc82ef8c91a8033b27a4ec27	
1485103528855	
1516.000000	
1485103527304	
http://live.gslb.cmvideo.cn/envivo_w/SD/liaoning/711/index.m3u8?msisdn=15725268046&mdspid=&spid=699052&netType=4&sid=5500183754&pid=2028597139,2028597138&timestamp=20170123004526&Channel_ID=0116_23040101-99000-200300020100001&ProgramID=623583504&ParentNodeID=-99&client_ip=223.80.238.163&assertID=5500183754&vormsid=2017012300401301154680&imei=b616bb404219c45b2e3b3aea0b35caf920f48bbe51b7a036904fad1bdd6475f8&SecurityKey=20170123004526&encrypt=f91b4fe0c1e58911037ae41f3970ec65&jid=bbbffe6f8ede99fd54f98d3e5c3720b8	
AV	
h264	
852x480	
0     
835	
aac	
2	
44100