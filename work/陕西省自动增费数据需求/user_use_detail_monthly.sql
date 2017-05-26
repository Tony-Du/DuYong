
create table app.user_use_detail_sx0290_monthly (      --用户使用月明细
phone_number   string,             --手机号码
use_begin_time string,             --使用时间
busi_prod_name string,             --业务产品名称，从rpt.dim_term_prod_v取
content_id     string,
content_name   string,             --播放内容，从rptdata.dim_video_content取
duration_min   decimal(38,4),      --播放时长
flow_mb        decimal(38,4)       --播放流量
)
partitioned by (src_file_month string)
row format delimited fields terminated by '31'
stored as textfile;



insert overwrite table app.user_use_detail_sx0290_monthly partition (src_file_month = ${SRC_FILE_MONTH})
select a.usernum_id as phone_number                         --手机号码
      ,a.use_begin_time                                     --使用时间(精确到秒)
      ,nvl(d.term_video_type_name, 'other') as busi_prod_name    --业务产品名称
	  ,a.content_id                                         --播放内容id
      ,nvl(c.content_name, '') as content_name              --播放内容
      ,round(a.duration_sec/60, 2) as duration_min          --播放时长(分钟)
      ,round(a.flow_kb/1024, 3) as flow_mb                  --播放流量(mb)
  from rptdata.fact_use_detail a                            --该表分区为天
 inner join rptdata.dim_region b
    on a.msisdn_region_id = b.region_id and b.prov_id = '0290' --陕西省的ID 
  left join rptdata.dim_video_content c
    on a.content_id = c.content_id  
  left join rpt.dim_term_prod_v d 
    on a.termprod_id = d.term_prod_id 
 where a.src_file_day >= '${MONTH_START_DAY}'
   and a.src_file_day <= '${MONTH_END_DAY}'                 
;


-------------------------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------------


sudo su - hadoop -c "/opt/data-integration/kitchen.sh -file=#system.BIGDATA_ETL_PATH#/app/user_use_detail_monthly/main.kjb -param:MONTH_START_DAY=#flow.month_start_day# -param:MONTH_END_DAY=#flow.month_end_day# SRC_FILE_DAY=#flow.src_file_day# -level=Detailed"

month_start_day     #flow.startDataTime#+'01'
month_end_day       tostring(adddays(addmonths(todate(#flow.startDataTime#+'01', 'yyyyMMdd'), 1),-1) ,'yyyyMMdd')


--数据源表  
desc rptdata.fact_use_detail;
OK
play_session_id         string            --播放会话ID                          
usernum_id              string            --用户号码         !!!
user_id                 string            --用户ID            
user_type_id            string            --用户类型            
broadcast_type_id       string            --播放方式          12  
use_begin_time          string            --播放开始时间          
use_end_time            string            --播放结束时间          
duration_sec            bigint            --播放时长        !!! 
flow_kb                 bigint            --播放流量        !!! 
use_source_ip           string            --用户源IP           
program_id              string            --播放节目ID          
chrgprod_id             string            --计费产品ID          
content_id              string            --播放内容ID          
net_type_id             string            --网络类型            
plat_id                 string            --门户ID            
channel_id              string            --渠道ID                        
app_version_code        string            --APP版本号                          
visit_session_id        string            --访问会话ID                          
imei                    string            --播放用户IMEI（解密）                        
region_id               string            --IP归属地区      !!!            
ip_prov_id              string            --IP归属省份ID    !!!                 
termprod_id             string            --终端产品ID      !!!
sub_busi_id             string            --子业务ID                       
play_url_domain         string            --播放 URL 域名                       
program_type_id         string            --播放节目类型                          
isp_id                  string            --运营商ID                       
vv                      bigint            --1                       
cdn_id                  string            --CDN ID
page_id                 string            --页面ID                            
position_id             string            --位置ID                            
msisdn_region_id        string            --手机号码归属地市ID      !!!                         
user_num_type           string            --UserNum类型   !!!!                    
source_channel_value    string            --                        
domain_belong_type_id   string            --                        
preview_flag            string            --是否试播          preview play flage  
advertise_flag          string            --是否广告播放      advertisement play flage
src_file_day            string            --分区 天                    
service_url_domain      string            --分区 服务 URL 域名       
   
desc rpt.dim_term_prod_v;
OK
term_prod_id            string                                      
term_prod_name          string                                      
term_prod_type_id       string                                      
term_prod_type_name     string                                      
term_prod_class_id      string                                      
term_prod_class_name    string                                      
term_os_type_id         string                                      
term_os_type_name       string                                      
term_video_type_id      string                                      
term_video_type_name    string   -----                                  
term_video_soft_id      string                                      
term_video_soft_name    string       

--测试
select a.usernum_id as phone_number 
      --,d.region_name               
      ,a.use_begin_time                 
      ,c.term_video_type_name as busi_prod_name    
      ,b.content_name                              
      ,round(a.duration_sec/60, 2) as duration_min
      ,round(a.flow_kb/1024, 2) as flow_mb 
  from rptdata.fact_use_detail a
  left join rptdata.dim_video_content b
    on a.content_id = b.content_id
  left join rpt.dim_term_prod_v c 
    on a.termprod_id = c.term_prod_id
 inner join rptdata.dim_region d
    on a.msisdn_region_id = d.region_id and d.prov_id = '0290'
 where a.src_file_day >= '20170301'
   and a.src_file_day <= '20170331' 
   and a.user_type_id = '1'                       
 limit 1000; 

 
 
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

insert overwrite table app.user_use_detail_monthly partition (src_file_month)
select a.usernum_id as phone_number                 --手机号码
      ,a.use_begin_time                             --使用时间(精确到秒)
      ,d.term_video_type_name as busi_prod_name     --业务产品名称
      ,c.content_name                               --播放内容
      ,round(a.duration_sec/60, 2) as duration_min  --播放时长(分钟)
      ,round(a.flow_kb/1024, 2) as flow_mb          --播放流量(mb)
      ,substr('20170301',1,6) as src_file_month
  from rptdata.fact_use_detail a                    --该表分区为天
 inner join rptdata.dim_region b
    on a.msisdn_region_id = b.region_id and b.prov_id = '0290' --陕西省的ID 
  left join rptdata.dim_video_content c
    on a.content_id = c.content_id  
  left join rpt.dim_term_prod_v d 
    on a.termprod_id = d.term_prod_id 
 where a.src_file_day >= '20170301'
   and a.src_file_day <= '20170331' 
   and a.user_type_id = '1'                         --手机号码                     
;
 
13892141950 20170303020713  咪咕视频    我的lol竟然如此的带感    0.35    8.48    201703
18710702690 20170315213214  咪咕影院    周润发讲义气，他最后说的这段话，在90年代影响了很多社会青年  1.75    15.43   201703
15891163683 20170305025121  咪咕视频    费玉清薛之谦过招    0.13    10.85   201703
13659114097 20170322211536  咪咕视频    费玉清薛之谦过招    2.05    22.44   201703
18829024278 20170323103403  咪咕视频    费玉清薛之谦过招    1.43    23.16   201703
15709162887 20170303110305  咪咕视频    费玉清薛之谦过招    0.58    15.36   201703
15191941062 20170331163847  咪咕视频    费玉清薛之谦过招    0.95    15.36   201703
18791017412 20170310113607  咪咕影院    《惊天破》超清预告片  0.12    5.11    201703
18329264244 20170323002208  咪咕影院    《惊天破》超清预告片  0.08    5.1     201703
18710541142 20170330224610  咪咕影院    《惊天破》超清预告片  0.13    5.11    201703
15129610080 20170331233155  咪咕影院    《惊天破》超清预告片  0.0     0.19    201703
17802938850 20170331204029  咪咕影院    《惊天破》超清预告片  0.08    0.97    201703
13891077661 20170321221532  咪咕影院    《惊天破》超清预告片  0.02    0.19    201703
18740506443 20170325042044  咪咕影院    《惊天破》超清预告片  0.1     1.29    201703
15929468627 20170326211829  咪咕影院    《惊天破》超清预告片  0.18    5.11    201703
15929468627 20170326211847  咪咕影院    《惊天破》超清预告片  0.12    4.51    201703
13720501558 20170329175725  咪咕影院    《惊天破》超清预告片  0.07    1.49    201703
13720501558 20170329174913  咪咕影院    《惊天破》超清预告片  0.15    5.11    201703
15229918114 20170322102005  咪咕影院    《惊天破》超清预告片  0.18    5.12    201703
15891126222 20170304021735  咪咕影院    《惊天破》超清预告片  0.13    1.84    201703
18791585277 20170311075501  咪咕影院    《惊天破》超清预告片  0.02    0.41    201703
15760954420 20170311223601  咪咕影院    《惊天破》超清预告片  0.2     5.12    201703
15991016168 20170311202032  咪咕影院    《惊天破》超清预告片  0.17    5.12    201703
13484631614 20170322215038  咪咕影院    《惊天破》超清预告片  0.08    2.22    201703
18292990363 20170327192112  咪咕影院    《惊天破》超清预告片  0.03    1.27    201703
18220008746 20170320151924  咪咕影院    《惊天破》超清预告片  0.07    0.67    201703
13892181419 20170313112127  咪咕影院    《惊天破》超清预告片  0.08    5.11    201703