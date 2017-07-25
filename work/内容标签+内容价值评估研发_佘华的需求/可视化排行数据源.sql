
desc rptdata.fact_user_play_detail;
OK
serv_number         	string              	                    
client_id           	string              	                    
installation_id     	string              	                    
udid                	string              	                    
imei                	string              	                    
idfa                	string              	                    
idfv                	string              	                    
app_pkg             	string              	                    
app_pkg_name        	string              	                    
os                  	string              	                    
app_os_name         	string              	                    
app_version_code    	string              	                    
cp_id               	string              	                    
product_id          	string              	                    
parent_node_id      	string              	                    
security_key        	string              	                    
encrypt_imei        	string              	                    
term_mode_name      	string              	                    
term_brand_name     	string              	                    
user_type_id        	string              	                    
term_prod_id        	string              	                    
term_version_id     	string              	                    
network_type        	string              	                    
app_chn_id          	string              	                    
app_chn_source      	string              	                    
content_id          	string              	                    
channel_id          	string              	                    
program_id          	string              	                    
broadcast_type_id   	string              	                    
player_version      	string              	                    
event_time          	string              	                    
play_begin_time     	string              	                    
play_end_time       	string              	                    
playback_begin_time 	string              	                    
playback_end_time   	string              	                    
playseek_time       	string              	                    
session             	string              	                    
sub_session         	string              	                    
sub_network_type    	string              	                    
client_ip           	string              	                    
host_ip             	string              	                    
ip_province_id      	string              	                    
ip_city_id          	string              	                    
isp_id              	string              	                    
service_url_domain  	string              	                    
sub_session_service_ip	string              	                    
sub_session_service_url	string              	                    
play_flow_kb        	string              	                    
play_duration_ms    	bigint              	                    
src_file_day        	string

hive> desc rptdata.dim_video_content;
OK
content_id          	string      
cp_id               	string  
cp_name             	string      
play_type           	string      
content_status      	string      
create_date         	string              
last_modify_time    	string          
last_modify_login   	string          
create_login        	string      
prov_id             	string      
content_name        	string      
detail              	string          
key_words           	string      
content_type        	string          
duration            	string      
icms_id             	string              
ud_id               	string      
copyright_provider  	string     	                    
authorization_way   	string     	                    
migu_publish        	string     	                    
bc_license          	string   	                    
in_fluence          	string     	                    
ori_publish         	string                  
first_poms_time     	string      
conv_type           	string
copyright_id        	string              	                    
dw_create_by        	string              	                    
dw_create_time      	string              	                    
dw_update_by        	string              	                    
dw_update_time      	string              	                    
dw_delete_flag      	string              	                    
dw_crc              	string              	                    
series_content_id   	string              	                    
episode_order       	string

hive> desc rptdata.dim_content_copyright;
OK
copyright_id        	string              	                    
copyright_name      	string              	                    
copyright_type      	string              	                    
ncp_id              	string              	                    
insert_day          	string              	                    
dw_create_by        	string              	                    
dw_create_time      	string              	                    
dw_update_by        	string              	                    
dw_update_time      	string              	                    
dw_delete_flag      	string              	                    
dw_crc              	string


hive> desc rptdata.dim_content_class;
OK
content_id          	string              	                    
con_class_1_name    	string              	                    
con_class_2_name    	string              	                    
con_tag_1_name      	string              	                    
con_tag_2_name      	string              	                    
con_tag_3_name      	string              	                    
con_tag_4_name      	string              	                    
con_tag_5_name      	string              	                    
dw_create_by        	string              	                    
dw_create_time      	string              	                    
dw_update_by        	string              	                    
dw_update_time      	string              	                    
dw_delete_flag      	string              	                    
dw_crc              	string

    > desc cms_20103_video_context_ex;
OK
content_id          	string              	--内容ID                    
cp_id               	string              	--CPID                    
play_type           	string              	--内容类型                    
content_status      	string              	--当前状态                    
create_date         	string              	--创建日期                    
last_modify_time    	string              	--最后一次修改时间                    
last_modify_login   	string              	--最后的修改者                    
create_login        	string              	--内容创建者                    
prov_id             	string              	--业务范围                    
content_name        	string              	--内容名称                    
detail              	string              	--内容说明                    
key_words           	string              	--内容关键字                    
content_type        	string              	--内容类别                    
duration            	string              	--内容播放时长                    
icms_id             	string              	--媒资ID                    
ud_id               	string              	--用户自定义标识                    
copyright_provider  	string              	--版权ID                    
authorization_way   	string              	--授权方式  1、单片授权 2、集体授权                  
migu_publish        	string              	--咪咕发行  1、非独家非首发 2、独家或首发                   
bc_license          	string              	--播出许可  1、非院线非电视台 2、院线或电视台                 
in_fluence          	string              	--受众影响  1、非热播 2、热播                    
ori_publish         	string              	--原创发行  1、工作室直签 2、代理发行 3、非原创发行                    
first_poms_time     	string              	--首次同步到POMS的日期                    
conv_type           	string              	--转码方式                    
src_file_day        	string                  --

    > desc rptdata.dim_charge_product;
OK
chrgprod_id         	string              	                    
chrgprod_name       	string              	                    
chrg_type_id        	string              	                    
chrgprod_price      	decimal(12,2)       	                    
create_time         	string              	                    
sp_id               	string              	                    
pu_id               	string              	                    
use_type            	string              	                    
order_type          	string              	                    
product_type        	string              	                    
exper_attr          	string              	                    
exper_start_time    	string              	                    
exper_end_time      	string              	                    
old_bureau_data_id  	string              	                    
old_bureau_data_name	string              	                    
new_bureau_data_id  	string              	                    
new_bureau_data_name	string              	                    
insert_day          	string              	                    
boss_opercode_id    	string              	                    
sub_busi_bdid       	string              	                    
dw_create_by        	string              	                    
dw_create_time      	string              	                    
dw_update_by        	string              	                    
dw_update_time      	string              	                    
dw_delete_flag      	string              	                    
dw_crc              	string



desc intdata.kesheng_sdk_sub_play ;
>
clientid            	string          --客户端ID，客户端上报      	                    
imei                	string          --    	                    
udid                	string          --    	                    
idfa                	string          --    	                    
idfv                	string          --    	                    
appversion          	string          --    	                    
apppkg              	string          --    	                    
networktype         	string          --网络类型：2G、3G、4G、WiFi、unknow    	                    
os                  	string          --    	                    
appchannel          	string          --渠道号，客户端上报    	                    
installationid      	string          --安装id,目前传udid的值    	                    
client_ip           	string          --客户端IP地址	                    
mg_msg_time         	string          --本事件生成的时间（UTC时刻）     	                    
session             	string          --唯一标识一次播放行为，该字段由APP传入    	                    
subsession          	string          --唯一标识播放器的一个统计周期    	                    
mg_msg_player_version	string          --播放器版本号     	                    
datausage           	string          --一个统计周期内的数据流量(上行＋下行)，单位KB    	                    
begintime           	string          --统计开始时间(UTC时刻)，生成subsession的时间     	                    
endtime             	string          --统计结束时间(UTC时刻）     	                    
nettype             	string          --承载网络类型(Wifi/Mobi)     	                    
playduration        	string          --本周期内的实际播放时长，单位：秒    	                    
hostip              	string          --本机公网地址，由 CDN HTTP响应头传入  	                    
sub_networktype     	string          --网络类型：2G、3G、4G、WiFi、unknow    	                    
subsessionserviceurl	string          --目标服务的URL或发生失败时的视频的URL。如果是HLS则为该TS的完整URL     	                    
subsessionserviceip 	string          --目标服务器的IP地址     	                    
province_name       	string          --省份    	                    
city_name           	string          --地市    	                    
provider_name       	string          --运营商    	                    
src_file_day        	string          --分区：    	                    
src_file_hour       	string          --分区：




select subsessionserviceurl from intdata.kesheng_sdk_sub_play a where src_file_day='20170613' and src_file_hour='10';

http://hlsmgspvod.miguvideo.com:8080/depository/asset/zhengshi/1001/024/954/1001024954/media/1001024954_1000146156_55.mp4@0-0.ts?
msisdn=13789192295
&mdspid=&spid=800033
&netType=4
&sid=1500452067
&pid=2028597139
&timestamp=20170601103058
&Channel_ID=0116_23000107-99000-200300150100001
&ProgramID=627528212
&ParentNodeID=-99
&client_ip=183.214.21.94
&assertID=1500452067
&imei=cf03b41c0ac4d301f4cad73ee637f0908ecddb1e5e06e1cbdef3e7faa49e0dfe
&SecurityKey=20170601103058
&encrypt=419c1f87d4434dead098f13ca86c7d0d
&jid=00aadc061da4e1cd1fb9363474898816
&sjid=subsession_1496284259300
&hls_type=2
&mtv_session=419c1f87d4434dead098f13ca86c7d0d
&HlsSubType=2
&HlsProfileId=0


http://iphone.cmvideo.cn:8080/depository/asset/zhengshi/5100/189/479/5100189479/media/5100189479_5000968909_55.mp4@0-101.ts?
msisdn=13852585549
&mdspid=
&spid=600113
&netType=4
&sid=5500264953
&pid=2028597139
&timestamp=20170601001011
&Channel_ID=0116_24040000-99000-200300020100001
&ProgramID=626831358
&ParentNodeID=-99
&cc=626329942
&client_ip=112.20.42.138
&assertID=5500264953
&imei=213708add650cc91c1717be23eb5af1978b0c4ccafdbf59b37ac60b4d7be3279
&resourceId=626831358
&resourceType=POMS_PROGRAM_ID
&SecurityKey=20170601001011
&encrypt=529ba0897ac4a565a73421ecd7cfeee9
&jid=f72326a7b362b0bb010e36a3870c63c8
&sjid=subsession_1496247193173
&hls_type=2
&mtv_session=529ba0897ac4a565a73421ecd7cfeee9
&HlsSubType=2
&HlsProfileId=0
&hcs_mod_ts_url=aHR0cDovL3ZvZC5jYWNoZS5jbXZpZGVvLmNuOjgwODgvZGVwb3NpdG9yeS9hc3NldC96aGVuZ3NoaS81MTAwLzE4OS80NzkvNTEwMDE4OTQ3OS9tZWRpYS81MTAwMTg5NDc5XzUwMDA5Njg5MDlfNTUubXA0QDAtMTAxLnRz_183.207.130.12

http://hlsmgzblive.miguvideo.com:8080/wd_r4/dfl/dongfangwshd/350/01.m3u8?
msisdn=05ci4547628868854022101688
&sid=5500058351
&timestamp=20170601100259
&Channel_ID=0117_43000400-00002-201800000010058
&pid=2028597139
&spid=699019
&imei=8921b5b555268cdae109e36c15f2a164
&netType=4
&ProgramID=550005835120170601008
&client_ip=183.204.67.1
&playbackbegin=20170601100300
&playbackend=20170601114100
&jid=DE7491C6114B36612A826DC8B9070EE51496282579312
&sjid=subsession_1496282579319
&mtv_session=fc3105b57609ba3be2239597874bdfca
&HlsSubType=1
&HlsProfileId=1
&encrypt=7676991ecaef282b42506b144b578275