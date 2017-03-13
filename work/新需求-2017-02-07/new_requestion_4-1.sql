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
partitioned by (extract_date_label string, extract_hour_label string)
row format delimited
fields terminated by '\t'; 

-- ======================================================================================== --

create or replace view int.kesheng_sdk_play_first_play_hourly_v_dy as 
select a.rowkey 
,a.custominfo_pos
,'56000004' custominfo_type
,a.extract_date_label 
,a.extract_hour_label 
,b.MG_MSG_MEDIA_INFO_TYPE 
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
from odsdata.kesheng_sdk_json_custominfo a
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
WHERE a.custominfo_type = '56000004' ;

-- =============================================================================================== --

set mapreduce.job.name=intdata.kesheng_sdk_play_first_play_hourly_dy_${EXTRACT_DATE}_${EXTRACT_HOUR};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

insert overwrite table intdata.kesheng_sdk_play_first_play_hourly_dy partition (extract_date_label, extract_hour_label) 
select a.clientid 
,a.imei 
,a.udid 
,a.idfa 
,a.idfv 
,a.appVersion 
,a.apppkg 
,a.networkType 
,a.os 
,a.appchannel 
,a.installationID 
	
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
,b.extract_date_label 
,b.extract_hour_label
from int.kesheng_sdk_play_first_play_hourly_v_dy b 
left join odsdata.kesheng_sdk_sessioninfo a 
on (a.rowkey = b.rowkey and a.custominfo_pos = b.custominfo_pos and a.custominfo_type = b.custominfo_type and a.src_file_day = b.extract_date_label)
WHERE b.extract_date_label = '${EXTRACT_DATE}' and b.extract_hour_label = '${EXTRACT_HOUR}';


-- 数据测试  $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
insert overwrite table intdata.kesheng_sdk_play_first_play_hourly_dy partition (extract_date_label, extract_hour_label)
select a.clientid                                111111	
,a.imei                                          9c795777bc03431d9cc731dddb263fd2	
,a.udid                                          F0E2F322-F41B-4BA3-B70A-C9E0FCA2A169	
,a.idfa                                          5BA6DD2C-BA72-4DD2-9DA6-10A7956F2635	
,a.idfv                                          C64BEBCA-3FAD-47E7-B450-520E8075F1D6	
,a.appVersion                                    3.1.1	
,a.apppkg                                        com.wondertek.hecmccmobile	
,a.networkType                                   NULL	
,a.os                                            NULL	
,a.appchannel                                    NULL	
,a.installationID                                NULL	
,b.MG_MSG_TIME                                   1481125179436	
,b.Session                                       eb9caa8e91eeed9c6e12cb9a64bb8cd5	
,b.LastSession                                   7d1fa962d31b0a9a1bbf44645b6fced2	
,b.MG_MSG_START_TIME                             1481125179436	
,b.MG_MSG_FFRAME_TIME                            552.000000	
,b.MG_MSG_GETURL_TIME                            1481125178826 
,b.MG_MSG_PROGRAM_URL                            http://live.gslb.cmvideo.cn/wd-guangxiwssd-600/index.m3u8?msisdn=3712022175297&mdspid=&spid=699010&netType=4&sid=2200131873&pid=2028597139&timestamp=20161207233938&Channel_ID=0116_23040101-99000-200300020100001&ProgramID=608779358&ParentNodeID=-99&client_ip=36.149.72.4&assertID=2200131873&imei=c7c6df085098804082f35d90cc5ce0e968c18aa38e90443d236a37cb5806e08f&SecurityKey=20161207233938&encrypt=fc2b5ba423dd04278e799141f595fcbe&jid=eb9caa8e91eeed9c6e12cb9a64bb8cd5
,b.MG_MSG_MEDIA_INFO_TYPE                        AV	
,b.MG_MSG_MEDIA_INFO_VIDEO_CODEC                 h264	
,b.MG_MSG_MEDIA_INFO_VIDEO_RESOLUTION            852x480	
,b.MG_MSG_MEDIA_INFO_VIDEO_FRAMERATE             0	
,b.MG_MSG_MEDIA_INFO_VIDEO_BITRATE               278	
,b.MG_MSG_MEDIA_INFO_AUDIO_CODEC                 aac	
,b.MG_MSG_MEDIA_INFO_AUDIO_CHANNELS              2	
,b.MG_MSG_MEDIA_INFO_AUDIO_SAMPLERATE            44100	
,b.extract_date_label                            20170123	
,b.extract_hour_label                            01
from ods.kesheng_sdk_play_first_play_hourly_v_dy b
left join odsdata.kesheng_sdk_sessioninfo a
on (a.rowkey = b.rowkey and a.custominfo_pos = b.custominfo_pos and a.custominfo_type = b.custominfo_type and a.src_file_day = b.extract_date_label)
WHERE b.extract_date_label = '20170123' and b.extract_hour_label = '01';


select * from intdata.kesheng_sdk_play_first_play_hourly_dy where extract_date_label = '20170123' and extract_hour_label = '01' limit 2;

select * from odsdata.kesheng_sdk_sessioninfo where src_file_day = 20170123 and clientid=111111 and imei='9c795777bc03431d9cc731dddb263fd2';



rowkey              	hdfs://ns1/user/hadoop/ods/kesheng/20170123/00/kesheng.1485100800039.gz:64449005	
clientid            	111111	
imei                	9c795777bc03431d9cc731dddb263fd2	
udid                	F0E2F322-F41B-4BA3-B70A-C9E0FCA2A169	
idfa                	5BA6DD2C-BA72-4DD2-9DA6-10A7956F2635	
idfv                	C64BEBCA-3FAD-47E7-B450-520E8075F1D6	
appversion          	3.1.1	
apppkg              	com.wondertek.hecmccmobile	
networktype         	NULL	
os                  	NULL	
appchannel          	NULL	
installationid      	NULL	
client_ip           	36.149.138.152	
province_name       	北京	
city_name           	北京	
provider_name       	移动	
custominfo_pos      	134	
custominfo_json     	{"result":0,"account":"3712022175297","stateNumID":1,"programeUrl":"http://live.gslb.cmvideo.cn/envivo_x/SD/cctv15/711/index.m3u8?msisdn=3712022175297&amp;mdspid=&amp;spid=699004&amp;netType=4&amp;sid=2202203578&amp;pid=2028597139&amp;timestamp=20161207233630&amp;Channel_ID=0116_23040101-99000-200300020100001&amp;ProgramID=608807408&amp;ParentNodeID=-99&amp;client_ip=36.149.72.4&amp;assertID=2202203578&amp;imei=c7c6df085098804082f35d90cc5ce0e968c18aa38e90443d236a37cb5806e08f&amp;SecurityKey=20161207233630&amp;encrypt=3e3b57497ed791e52eab9f9fdfa4b3f6","rateType":3,"ContentID":"60880740820161207026","programeType":0,"type":"17","timestamp":"2016-12-07 23:36:30:472","state":"Open","playSessionID":"031d2ed35915d2e8f9f8fb3b02bb6255"}	
src_file_day        	20170123	
custominfo_type     	17
