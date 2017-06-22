--只保留url
CREATE TABLE temp.fact_user_play_detail
(
      serv_number             string,--手机号码
	  client_id               string,--客户端ID，客户端上报
	  installation_id         string,--安装id,目前传udid的值
      udid                    string,                                     
      imei                    string,                                     
      idfa                    string,
      idfv                    string,--idfv	                                   
      os                      string,
      app_version             string,--app版本号	  
      app_pkg                 string,  	  
      term_mode_name          string,                                     
      term_brand_name         string,                                     
      user_type_id            string,                                     
      term_prod_id            string,     --关联终端大类                                
      term_version_id         string,                                     
      network_type            string,                                     
      app_chn_id              string,                                     
      app_chn_source          string,                                     
      channel_id              string,   --内容ID，关联ods.CMS_20103_video_context_ex，找到	内容名称，原创发行，授权方式，咪咕发行（首发状态），版权ID（关联版权维表，找到 版权类型，NCP_ID	）
										-- 关联ods.cms_20115_context_category_tags_ex 找到 内容一级分类; 内容一级分类,内容二级分类，一级标签
      program_id              string,                                     
      broadcast_type_id       string,
      mg_msg_player_version	  string,--播放器版本号 
	  mg_msg_time             string,--本事件生成的时间
	  session                 string,--会话 count(distinct session) 次数
      sub_session             string,--子会话
	  sub_network_type        string,--子网络类型
      play_begin_time         string,                                     
      play_end_time           string,                                     
      playback_begin_time     string,                                     
      playback_end_time       string,                                     
      playseek_time           string,                                     
      client_ip               string,
      sub_session_service_ip  string,--目标服务器的IP地址
      public_ip  	          string,--本机公网地址
      ip_province_id          string,                                     
      ip_city_id              string,                                     
      isp_id                  string,                                     
      service_url_domain      string, 
      sub_session_service_url string,--目标服务的URL  
      play_flow_kb            string,                                     
      play_duration_ms        bigint                                    
)partitioned by (src_file_day string)
stored as parquet;