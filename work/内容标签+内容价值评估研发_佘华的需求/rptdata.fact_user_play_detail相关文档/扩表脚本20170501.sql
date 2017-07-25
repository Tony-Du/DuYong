set mapreduce.job.name=rptdata.fact_user_play_detail_20170501;
set hive.exec.dynamic.partition.mode=nonstrict; 
set hive.exec.dynamic.partition=true;
WITH temp_table AS
(
  SELECT
         t2.udid,
         t2.imei,
         t2.idfa,
         t2.idfv,
         t2.app_pkg,
         t2.os,
         t2.network_type,
         t2.app_chn_source,
         t2.channel_id,
         t2.program_id,
         t2.play_flow_kb,
         t2.play_begin_time,
         t2.play_end_time,
         t2.play_duration_ms,
         t2.playback_begin_time,
         t2.playback_end_time,
         t2.playseek_time,
         t2.client_ip,
		 t2.serv_number,--手机号码
	     t2.client_id,--客户端ID，客户端上报
	     t2.installation_id,--安装id,目前传udid的值
	     t2.idfv,--idfv
	     t2.app_version,--app版本号
	     t2.mg_msg_player_version,--播放器版本号 
	     t2.mg_msg_time,--本事件生成的时间
	     t2.session,--会话
         t2.sub_session,--子会话
	     t2.sub_network_type,--子网络类型
	     t2.sub_session_service_ip,--目标服务器的IP地址
         t2.public_ip,--本机公网地址
	     t2.sub_session_service_url,--目标服务的URL
         t8.prov_id as ip_province_id,
         t9.region_id as ip_city_id,
         t10.isp_id as isp_id,
         t2.service_url_domain, 
         split(CASE
         WHEN t2.source_channel_mask = '0-00000000000' THEN concat_ws('_', '-998', '-998', substr(app_chn_source, 3),concat('000',substr(app_chn_source,1,1)))
         WHEN t2.source_channel_mask = '0000-00000000000' THEN concat_ws('_', '-998', '-998', substr(app_chn_source, 6),substr(app_chn_source,1,4))
         WHEN t2.source_channel_mask = '0000-000000000000000' THEN concat_ws('_', '-998', '-998', substr(app_chn_source, 6),substr(app_chn_source,1,4))
         WHEN t2.source_channel_mask = '0000-00000000-00000-000000000000000' THEN concat_ws('_', substr(app_chn_source, 6, 8), substr(app_chn_source, 15, 5),substr(app_chn_source, 21),substr(app_chn_source,1,4))
         WHEN t2.source_channel_mask IN ( '00000000000' ,'000000000000000' ) THEN concat_ws('_', '-998', '-998', app_chn_source,'-998')
         WHEN t2.source_channel_mask = '00000000-00000-000000000000000' THEN concat_ws('_', substr(app_chn_source, 1, 8), substr(app_chn_source, 10, 5), substr(app_chn_source, 16),'-998')
         WHEN t2.source_channel_mask LIKE '0-00000000000-%' THEN concat_ws('_', '-998', '-998', substr(app_chn_source, 3, 11),concat('000',substr(app_chn_source,1,1)))
         WHEN t2.source_channel_mask LIKE '0000-00000000000-%' THEN concat_ws('_', '-998', '-998', substr(app_chn_source, 6, 11),substr(app_chn_source,1,4))
         ELSE concat_ws('_', '-998', '-998', '-998', '-998')
         END,'_') AS chn_values,
         t2.src_file_day
   FROM
 ( 
      SELECT
           (case when t.udid IS NULL or t.udid like '%null%' then '-998' else t.udid end) AS udid,
           t.imei,
           t.idfa,
           t.idfv,
           t.apppkg as app_pkg,
           t.os,
           t.networktype as network_type,
           t.appchannel as app_chn_source,
           t1.content_id as channel_id,
           t1.program_id,         
           t.datausage as play_flow_kb,
           t.begintime as play_begin_time,    
           t.endtime as play_end_time, 
           t.playduration as play_duration_ms, 
           t1.playbackbegin as playback_begin_time,
           t1.playbackend as playback_end_time,  
           t1.playseek as playseek_time,
           t.client_ip,
           t1.service_url_domain,
		   t1.serv_number,--手机号码
	       t.clientid as client_id,--客户端ID，客户端上报
	       t.installationID as installation_id,--安装id,目前传udid的值
	       t.idfv,--idfv
	       t.appVersion as app_version,--app版本号
	       t.mg_msg_player_version,--播放器版本号 
	       t.mg_msg_time,--本事件生成的时间
	       t.session,--会话
           t.subsession as sub_session,--子会话
	       t.sub_networkType as sub_network_type,--子网络类型
	       t.subsessionserviceip as sub_session_service_ip,--目标服务器的IP地址
           t.hostip as public_ip,--本机公网地址
	       t.subsessionserviceurl as sub_session_service_url,--目标服务的URL
           split(migu_getIpAttr(t.client_ip), '\t') as ip_attr,       
           regexp_replace(translate(trim(t.appchannel), '123456789', '000000000'),'_','-') as source_channel_mask,
           t.src_file_day
        FROM intdata.kesheng_sdk_sub_play t
lateral view parse_url_tuple(t.subsessionserviceurl,'HOST', 'QUERY:ProgramID', 'QUERY:sid', 'QUERY:timestamp','QUERY:playbackbegin','QUERY:playbackend','QUERY:playseek','QUERY:msisdn','QUERY:spid','QUERY:pid','QUERY:ParentNodeID','QUERY:assertID','QUERY:SecurityKey','QUERY:encrypt','QUERY:hls_type','QUERY:HlsSubType','QUERY:HlsProfileId') t1 as service_url_domain, program_id, content_id,play_time,playbackbegin,playbackend,playseek,serv_number,spid,product_id,parent_node_id,assert_id,security_key,encrypt,hls_type,hls_sub_type,hls_profile_id
       WHERE t.src_file_day = '20170501'
 ) t2
LEFT JOIN (SELECT distinct prov_name,prov_id FROM rptdata.dim_region) t8 ON (t8.prov_name=t2.ip_attr[1])
LEFT JOIN rptdata.dim_region t9 ON (t9.region_name=t2.ip_attr[2])
LEFT JOIN rptdata.dim_isp t10 ON (t10.isp_name=t2.ip_attr[3])
)

INSERT overwrite TABLE rptdata.fact_user_play_detail partition(src_file_day) 
SELECT
        t3.serv_number,       --手机号码
	    t3.client_id,         --客户端ID，客户端上报
	    t3.installation_id,   --安装id,目前传udid的值
        t3.udid,
        t3.imei,
        t3.idfa,
		t3.idfv,              --idfv
		t3.app_version,       --app版本号
        t3.app_pkg,
        t3.os,
        if(nvl(t7.phone_brand_name,'') = '',t7.phone_mode_name,if(instr(upper(t7.phone_mode_name), upper(t7.phone_brand_name)) > 0, t7.phone_mode_name, concat_ws(' ', t7.phone_brand_name, t7.phone_mode_name))) term_mode_name,
        nvl(t7.phone_brand_name,'-998') as term_brand_name,
		case when t7.user_id is null or t7.user_id = '' then '0' else '1' end  as user_type_id,
        nvl(t6.term_prod_id,'-998') as term_prod_id, 
        substr(t3.chn_values[0],1,6) as term_version_id,
        t3.network_type,
        t3.chn_values[2] as app_chn_id,
        t3.app_chn_source,
        t3.channel_id,
        t3.program_id,
        nvl(t5.broadcast_type_id,'-998') broadcast_type_id,
		t3.mg_msg_player_version,       --播放器版本号 
	    t3.mg_msg_time,                 --本事件生成的时间
	    t3.session,                     --会话
        t3.sub_session,                 --子会话
	    t3.sub_network_type,            --子网络类型
        t3.play_begin_time,
        t3.play_end_time,
        t3.playback_begin_time,
        t3.playback_end_time,
        t3.playseek_time,
        t3.client_ip,
		t3.sub_session_service_ip,         --目标服务器的IP地址
        t3.public_ip,                      --本机公网地址
        t3.ip_province_id,
        t3.ip_city_id,
        t3.isp_id,
        t3.service_url_domain,
		t3.sub_session_service_url string,   --目标服务的URL
        t3.play_flow_kb,
        t3.play_duration_ms,
        t3.src_file_day          
      FROM temp_table t3
LEFT JOIN msc.dim_cdn_domain_ex t5 ON(t3.service_url_domain=t5.service_url_domain)  
LEFT JOIN rptdata.dim_prod_id t6 ON (t6.term_version_id <>'-998'and t6.term_version_id=substr(t3.chn_values[0],1,6))
LEFT JOIN (
select udid,imei,idfa,idfv,phone_mode_name,phone_brand_name,user_id,row_number() over (partition by udid,imei,idfa,idfv order by upload_unix_time desc ) as row_num from intdata.kesheng_sdk_device 
where src_file_day='20170501'
) t7
ON ( t7.row_num=1
and nvl(t7.udid,'-998') = nvl(t3.udid,'-998')
and nvl(t7.imei,'-998') = nvl(t3.imei,'-998')
and nvl(t7.idfa,'-998') = nvl(t3.idfa,'-998')
and nvl(t7.idfv,'-998') = nvl(t3.idfv,'-998')
);