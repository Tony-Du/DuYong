
CREATE TABLE `odsdata.kesheng_sdk_sessioninfo`(
  `rowkey` string, 
  `clientid` string, 
  `imei` string, 
  `udid` string, 
  `idfa` string, 
  `idfv` string, 
  `appversion` string, 
  `apppkg` string, 
  `networktype` string, 
  `os` string, 
  `appchannel` string, 
  `installationid` string, 
  `client_ip` string, 
  `province_name` string, 
  `city_name` string, 
  `provider_name` string, 
  `custominfo_pos` string, 
  `custominfo_json` string)  -- {"type":"-1","timestamp":"2016-12-02 04:42:13","account":"3848087285292"}
PARTITIONED BY ( 
  `src_file_day` string, 	-- 20161202       
  `custominfo_type` string)	-- -1
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ns1/user/hive/warehouse/odsdata.db/kesheng_sdk_sessioninfo'
TBLPROPERTIES (
  'transient_lastDdlTime'='1481617167')
  
hdfs://ns1/user/hadoop/ods/kesheng/20161202/21/kesheng.1480683636661.gz:356019226       000249          ffffffff-ad60-796a-0000-0000000000001480624861805       NULL   NULL     3.0.1.1 com.cmcc.cmvideo        NULL    NULL    NULL    NULL    1.182.139.56    内蒙古  鄂尔多斯        电信    0       {"type":"-1","timestamp":"2016-12-02 04:42:13","account":"3848087285292"}       20161202        -1
  
-- =============================================================================================== --

CREATE TABLE `odsdata.kesheng_sdk_json_custominfo`(
  `rowkey` string, 
  `custominfo_pos` string, 
  `custominfo_json` string, 
  `custominfo_type` string)
PARTITIONED BY ( 
  `extract_date_label` string, 
  `extract_hour_label` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ns1/user/hive/warehouse/odsdata.db/kesheng_sdk_json_custominfo'
TBLPROPERTIES (
  'last_modified_by'='hadoop', 
  'last_modified_time'='1479957832', 
  'transient_lastDdlTime'='1479957832')

-- 不同的type对应的记录  

select * from odsdata.kesheng_sdk_json_custominfo where custominfo_type='56000004' and extract_date_label=20170123 and extract_hour_label=01;
hdfs://ns1/user/hadoop/ods/kesheng/20170123/01/kesheng.1485104400020.gz:272481071	
326	
{"MG_MSG_MEDIA_INFO_TYPE":"AV"
,"sessionID":"34a2e8008ec39b7bf7a6d9b7446cd20f"
,"MG_MSG_MEDIA_INFO_VIDEO_CODEC":"hevc"
,"MG_MSG_MEDIA_INFO_AUDIO_SAMPLERATE":"44100"
,"Session":"103ba5916bcd9e25b90be0397e139894"
,"LastSession":"","type":"56000004"
,"channelID":"23040101-99000-200300020100001"
,"MG_MSG_FFRAME_TIME":"2389.000000"
,"MG_MSG_MEDIA_INFO_VIDEO_RESOLUTION":"848x480"
,"MG_MSG_PROGRAM_URL":"http://vod.gslb.cmvideo.cn/depository/asset/zhengshi/5100/090/900/5100090900/media/5100090900_5000564842_58.mp4.m3u8?msisdn=13884943827&mdspid=&spid=800033&netType=5&sid=5500114667&pid=2028597139,2028597138&timestamp=20170122225050&Channel_ID=0116_23040101-99000-200300020100001&ProgramID=621951731&ParentNodeID=-99&client_ip=117.136.78.20&assertID=5500114667&vormsid=2017012222513401947830&imei=b616bb404219c45b2e3b3aea0b35caf920f48bbe51b7a036904fad1bdd6475f8&SecurityKey=20170122225050&encrypt=0582e3ba2a8bbfb9a37c31cb375ffa75&jid=103ba5916bcd9e25b90be0397e139894"
,"MG_MSG_MEDIA_INFO_VIDEO_BITRATE":"54"
,"MG_MSG_GETURL_TIME":"1485096650557"
,"MG_MSG_START_TIME":"1485096652995"
,"MG_MSG_MEDIA_INFO_VIDEO_FRAMERATE":"0"
,"MG_MSG_MEDIA_INFO_AUDIO_CODEC":"aac"
,"MG_MSG_MEDIA_INFO_AUDIO_CHANNELS":"2"
,"MG_MSG_TIME":"1485096652996"
,"clientID":"3912645369845"}	
56000004	--custominfo_type
20170123	
1
-- json格式解析后
AV	
34a2e8008ec39b7bf7a6d9b7446cd20f	
hevc	
44100	
103ba5916bcd9e25b90be0397e139894		
23040101-99000-200300020100001	
2389.000000	
848x480	
http://vod.gslb.cmvideo.cn/depository/asset/zhengshi/5100/090/900/5100090900/media/5100090900_5000564842_58.mp4.m3u8?msisdn=13884943827&mdspid=&spid=800033&netType=5&sid=5500114667&pid=2028597139,2028597138&timestamp=20170122225050&Channel_ID=0116_23040101-99000-200300020100001&ProgramID=621951731&ParentNodeID=-99&client_ip=117.136.78.20&assertID=5500114667&vormsid=2017012222513401947830&imei=b616bb404219c45b2e3b3aea0b35caf920f48bbe51b7a036904fad1bdd6475f8&SecurityKey=20170122225050&encrypt=0582e3ba2a8bbfb9a37c31cb375ffa75&jid=103ba5916bcd9e25b90be0397e139894	
54	
1485096650557 
1485096652995	
0	
aac	
2	
1485096652996	
3912645369845

select * from odsdata.kesheng_sdk_json_custominfo where custominfo_type='58000000' and extract_date_label=20170123 and extract_hour_label=01;
hdfs://ns1/user/hadoop/ods/kesheng/20170123/01/kesheng.1485104400021.gz:203104286	
11	
{"Error_Code":"20000001"
,"Failed_Detail_Code":"0"
,"MG_MSG_MEDIA_INFO_AUDIO_SAMPLERATE":"44100"
,"MG_MSG_MEDIA_INFO_VIDEO_BITRATE":"0"
,"channelID":"null"
,"Subsession":"subsession_0_1485104707908"
,"MG_MSG_MEDIA_INFO_TYPE":"AV"
,"clientID":"null"
,"SubsessionServiceIP":"111.10.27.142"
,"MG_MSG_MEDIA_INFO_VIDEO_FRAMERATE":"24.0"
,"type":"58000000"
,"MG_MSG_MEDIA_INFO_AUDIO_CHANNELS":"2"
,"sessionID":"null"
,"MG_MSG_MEDIA_INFO_VIDEO_CODEC":"hevc"
,"MG_MSG_MEDIA_INFO_AUDIO_CODEC":"aac"
,"Session":"553c8f2d9caed4f5a76bb56814b279f5"
,"MG_MSG_MEDIA_INFO_VIDEO_RESOLUTION":"848 x 480"
,"SubsessionServiceURL":"http://miguvod.lovev.com:8080/depository/asset/zhengshi/5100/104/739/5100104739/media/5100104739_5000537513_53.mp4.m3u8?msisdn=13453526898&mdspid=&spid=800033&netType=0&sid=5500137102&pid=2028593060&timestamp=20170123010508&Channel_ID=0111_64000002-99000-800000210000001&ProgramID=622328672&ParentNodeID=-99&client_ip=223.104.108.93&assertID=5500137102&imei=ed317c4b59c8d418f3d4fcfa9d7376e49e8923c871e55e99fe120d4ea271df028cb3826559f9501ad22586130b847624&SecurityKey=20170123010508&encrypt=374349700463fa609690eb54b0ad630e&mtv_session=a1d33f53ed3305bf4cadeeb1ce381a0a&jid=553c8f2d9caed4f5a76bb56814b279f5&sjid=subsession_0_1485104707908"
,"MG_MSG_TIME":"1485104711642"}	
58000000	
20170123	
1


select * from odsdata.kesheng_sdk_json_custominfo where custominfo_type='56000015' and extract_date_label=20170123 and extract_hour_label=01;
hdfs://ns1/user/hadoop/ods/kesheng/20170123/01/kesheng.1485104400021.gz:203641468	
58	
{"MG_MSG_STUCK_START":"1485100576910"
,"Session":"268de1672f0e539be01bc2d2d1a29a1c"
,"SubsessionServiceURL":"http://iphone.cmvideo.cn:8080/depository/asset/zhengshi/5100/034/671/5100034671/media/5100034671_5000151439_55.mp4@0-64.ts?msisdn=15007005174&mdspid=&spid=600112&netType=5&sid=5500038828&pid=2028597138&timestamp=20170122234718&Channel_ID=0116_23040101-99000-200300020100001&ProgramID=619564212&ParentNodeID=-99&cc=619563115&client_ip=223.104.10.243&assertID=5500038828&vormsid=2017012223460901952436&imei=265c96754308576b54bdc8ba021347838cbfec9f306b60d06c8b90a5a2587789&SecurityKey=20170122234718&encrypt=d32ae6e0e9b810054fd1881bda89ac72&jid=268de1672f0e539be01bc2d2d1a29a1c&sjid=hls_subsession_0_1485100038218&hls_type=2&mtv_session=d32ae6e0e9b810054fd1881bda89ac72&HlsSubType=2&HlsProfileId=0"
,"MG_MSG_STUCK_END":"1485100578036"
,"SubsessionServiceIP":"117.169.86.5"
,"MG_MSG_STUCK_DURATION":"1.13"
,"Subsession":"hls_subsession_0_1485100038218"
,"MG_MSG_TIME":"1485100578037"
,"type":"56000015"}	
56000015	
20170123	
1


select * from odsdata.kesheng_sdk_json_custominfo where custominfo_type='57000000' and extract_date_label=20170123 and extract_hour_label=01;
hdfs://ns1/user/hadoop/ods/kesheng/20170123/01/kesheng.1485104400021.gz:132661631	
23	
{"BeginTime":"1485090663096"
,"DataUsage":"9042399"
,"NetType":"Wifi"
,"Session":"e9bd2c8b1a876867a15cd595c41e9654"
,"SubsessionServiceURL":"http://live.hcs.cmvideo.cn:8088/wd-tianjinwssd-600/20170122184339-01-1485082578.ts?msisdn=15805216369&mdspid=&spid=699010&netType=4&sid=5500015644&pid=2028597139,2028597138&timestamp=20170122211059&Channel_ID=0116_23040101-99000-200300020100001&ProgramID=618989927&ParentNodeID=-99&client_ip=117.87.33.125&assertID=5500015644&vormsid=2017012221111401572161&imei=9770d76e9716c48c6fcf0a177cd5b3415b0e87b68cc1f77803b541b9b5158d3a&SecurityKey=20170122211059&playseek=002200&jid=e9bd2c8b1a876867a15cd595c41e9654&sjid=hls_subsession_0_1485090663097&mtv_session=27c6af3e60c11797756d05302f53051c&encrypt=0e523a3b59d38025c386294ddc6ad270&HlsSubType=2&HlsProfileId=1"
,"type":"57000000"
,"MG_MSG_TIME":"1485090666365"
,"HostIP":"unknown"
,"SubsessionServiceIP":"180.97.249.209"
,"Subsession":"hls_subsession_0_1485090663097"
,"EndTime":"1485090666364"
,"MG_MSG_PLAYER_VERSION":"5.7.2"
,"PlayDuration":"2714"}	
57000000      
20170123	
1

