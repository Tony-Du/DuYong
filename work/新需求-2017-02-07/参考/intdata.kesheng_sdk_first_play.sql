
-- create table
CREATE TABLE intdata.kesheng_sdk_first_play(
       clientId                            String,
       imei                                String,
       udid                                String,
       idfa                                String,
       idfv                                String,
       appVersion                          String,
       apppkg                              String,
       networktype                         String,
       os                                  String,
       appchannel                          String,
       installationID                      String,
       client_ip                           String,
       MG_MSG_TIME                         String,
       Session                             String,
       LastSession                         String,
       MG_MSG_START_TIME                   String,  -- 启播时间：第一帧画面的显示时间，绝对时间（UTC时刻） 
       MG_MSG_FFRAME_TIME                  String,  -- 首帧响应时间，单位毫秒
       MG_MSG_GETURL_TIME                  String,  -- GETURL时间：拿到URL的时间，绝对时间（UTC时刻）
       sub_networkType                     String,  
       MG_MSG_PROGRAM_URL                  String,  -- 节目URL。这个URL是有APP传入的URL
       MG_MSG_MEDIA_INFO_TYPE              String,  -- Audio/Video/AV 
       MG_MSG_MEDIA_INFO_VIDEO_CODEC       String,  -- 当前处理视频的视频编码信息 
       MG_MSG_MEDIA_INFO_VIDEO_RESOLUTION  String,  -- 当前处理视频的原始视频分辨率(宽x高：1920x1080) 
       MG_MSG_MEDIA_INFO_VIDEO_FRAMERATE   String,  -- 当前处理视频的帧率 
       MG_MSG_MEDIA_INFO_VIDEO_BITRATE     String,  -- 当前处理视频的码率(kbps) 
       MG_MSG_MEDIA_INFO_AUDIO_CODEC       String,  -- 当前处理的音频编码信息 
       MG_MSG_MEDIA_INFO_AUDIO_CHANNELS    String,  -- 当前处理的音频声道信息 
       MG_MSG_MEDIA_INFO_AUDIO_SAMPLERATE  String,  -- 当前处理音频的采样率(HZ) 
       province_name                       String,
       city_name                           String,
       provider_name                       String
)
PARTITIONED BY (src_file_day String, src_file_hour String)
STORED AS PARQUET;


-- 第一步：
 set mapreduce.job.name=intdata.kesheng_sdk_first_play_${SRC_FILE_DAY}_${SRC_FILE_HOUR};

 insert overwrite table intdata.kesheng_sdk_first_play partition (src_file_day='${SRC_FILE_DAY}',src_file_hour='${SRC_FILE_HOUR}')
 select s.clientId,                                    
        s.imei,                                     
        s.udid,                                  
        s.idfa,                                  
        s.idfv,                                  
        s.appVersion,                            
        s.apppkg,                                
        s.networktype,                           
        s.os,                                    
        s.appchannel,                            
        s.installationid,                        
        s.client_ip,                             
        c.mg_msg_time,                           
        c.session,                               
        c.lastSession,                           
        c.mg_msg_start_time,
        c.mg_msg_fframe_time,
        c.mg_msg_geturl_time,
        c.sub_networkType,
        c.mg_msg_program_url,
        c.mg_msg_media_info_type,
        c.mg_msg_media_info_video_codec,
        c.mg_msg_media_info_video_resolution,
        c.mg_msg_media_info_video_framerate,
        c.mg_msg_media_info_video_bitrate,
        c.mg_msg_media_info_audio_codec,
        c.mg_msg_media_info_audio_channels,
        c.mg_msg_media_info_audio_samplerate,
        s.province_name,
        s.city_name,
        s.provider_name
   from ods.kesheng_sdk_custominfo_json_v s
lateral view json_tuple (
                         s.custominfo_json,
                         'MG_MSG_TIME', 
                         'Session', 
                         'LastSession', 
                         'MG_MSG_START_TIME', 
                         'MG_MSG_FFRAME_TIME',
                         'MG_MSG_GETURL_TIME',
                         'networkType',
                         'MG_MSG_PROGRAM_URL', 
                         'MG_MSG_MEDIA_INFO_TYPE',
                         'MG_MSG_MEDIA_INFO_VIDEO_CODEC', 
                         'MG_MSG_MEDIA_INFO_VIDEO_RESOLUTION', 
                         'MG_MSG_MEDIA_INFO_VIDEO_FRAMERATE',
                         'MG_MSG_MEDIA_INFO_VIDEO_BITRATE', 
                         'MG_MSG_MEDIA_INFO_AUDIO_CODEC', 
                         'MG_MSG_MEDIA_INFO_AUDIO_CHANNELS', 
                         'MG_MSG_MEDIA_INFO_AUDIO_SAMPLERATE'
                        ) c
     as mg_msg_time,session,lastSession,mg_msg_start_time,mg_msg_fframe_time,mg_msg_geturl_time,sub_networkType,mg_msg_program_url,mg_msg_media_info_type,mg_msg_media_info_video_codec,mg_msg_media_info_video_resolution,mg_msg_media_info_video_framerate,mg_msg_media_info_video_bitrate,mg_msg_media_info_audio_codec,mg_msg_media_info_audio_channels,mg_msg_media_info_audio_samplerate
  where s.custominfo_type = '56000004' and src_file_day='${SRC_FILE_DAY}' and src_file_hour='${SRC_FILE_HOUR}';
  
  
  
  
  
  
  
  

  
  
  
  
  
  
  
  
  
  
