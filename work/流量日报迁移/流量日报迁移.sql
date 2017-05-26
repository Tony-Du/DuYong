
create table rptdata.fact_cdn_flow_daily (
    flow_000001 decimal(38,4),
    flow_000002 decimal(38,4),
    flow_000003 decimal(38,4),
    flow_000004 decimal(38,4),
    flow_000005 decimal(38,4),
    flow_000006 decimal(38,4),
    flow_000007 decimal(38,4),
    flow_000008 decimal(38,4),
    flow_000009 decimal(38,4),
    flow_000010 decimal(38,4),
    flow_000011 decimal(38,4),
    flow_000012 decimal(38,4),
    flow_000013 decimal(38,4),
    flow_000014 decimal(38,4),
    flow_000015 decimal(38,4)
) 
partitioned by (src_file_day string);


        
insert overwrite table rptdata.fact_cdn_flow_daily partition (src_file_day = ${SRC_FILE_DAY})                                           
select t.stat_date
      ,0 as flow_000001                                                     --单位 TB
      ,round(sum(t.flow_000002)/(1024*1024*1024), 2) as flow_000002
      ,round(sum(t.flow_000003)/(1024*1024*1024), 2) as flow_000003
      ,round(sum(t.flow_000004)/(1024*1024*1024), 2) as flow_000004
      ,round(sum(t.flow_000005)/(1024*1024*1024), 2) as flow_000005
      ,round(sum(t.flow_000006)/(1024*1024*1024), 2) as flow_000006
      ,round(sum(t.flow_000007)/(1024*1024*1024), 2) as flow_000007
      ,round(sum(t.flow_000008)/(1024*1024*1024), 2) as flow_000008
      ,round(sum(t.flow_000009)/(1024*1024*1024), 2) as flow_000009
      ,round(sum(t.flow_000010)/(1024*1024*1024), 2) as flow_000010
      ,round(sum(t.flow_000011)/(1024*1024*1024), 2) as flow_000011
      ,round(sum(t.flow_000012)/(1024*1024*1024), 2) as flow_000012
      ,round(sum(t.flow_000013)/(1024*1024*1024), 2) as flow_000013
      ,round(sum(t.flow_000014)/(1024*1024*1024), 2) as flow_000014
      ,round(sum(t.flow_000015)/(1024*1024*1024), 2) as flow_000015
  from (                
        select '${SRC_FILE_DAY}' as stat_date     
              ,0 as flow_000001                                                                                                --基地总流量             1908.45  
              ,case when a.net_type_id in ('1', '2', '5') then a.flow_kb else 0 end flow_000002                                --移动数据流量           925.0
              ,case when a.net_type_id = '5' then a.flow_kb else 0 end flow_000003                                             --移动4G流量             376.33  
              ,case when a.net_type_id not in ('1', '2', '5') then a.flow_kb else 0 end flow_000004                            --非移动数据流量         983.45 
              ,case when c.term_video_type_id = 'TV00001' then a.flow_kb else 0 end flow_000005                                --和视频流量             811.08   
              ,case when c.term_video_type_id = 'TV00002' then a.flow_kb else 0 end flow_000006                                --和视界流量             3.17 
              ,case when a.broadcast_type_id = '12' then a.flow_kb else 0 end flow_000007                                      --直播流量               1458.89 
              ,case when c.term_video_soft_id in ('TST000003', 'TST000004') then a.flow_kb else 0 end flow_000008              --能力输出总流量         156.12 
              ,case when c.term_video_soft_id = 'TST000003' then a.flow_kb else 0 end flow_000009                              --SDK流量                10.02 
              ,case when c.term_video_soft_id = 'TST000004' then a.flow_kb else 0 end flow_000010                              --OPENAPI流量            146.1 
              ,case when a.cdn_id in ('4000', '4001') then a.flow_kb else 0 end flow_000011                                    --第三方流量             165.28   
              ,case when a.cdn_id in ('1000', '2000') then a.flow_kb else 0 end flow_000012                                    --自建CDN流量            1313.87 
              ,case when c.term_video_type_id = 'TV00303' then a.flow_kb else 0 end flow_000013                                --咪咕视频流量           441.23    
              ,case when c.term_video_type_id = 'TV00203' then a.flow_kb else 0 end flow_000014                                --咪咕影院流量           14.34 
              ,case when c.term_video_type_id = 'TV00306' then a.flow_kb else 0 end flow_000015                                --咪咕直播流量           502.56
          from rptdata.fact_use_detail a
          left join msc.dim_cdn_domain_ex b 
            on a.service_url_domain = b.service_url_domain
          left join rpt.dim_term_prod_v c
            on a.termprod_id = c.term_prod_id
         where a.src_file_day = '${SRC_FILE_DAY}'   
       ) t 
  group by t.stat_date;    
 
 
---------------------------------------------------------------------------------------------------------------------------------------------------- 
----------------------------------------------------------------------------------------------------------------------------------------------------
--第二版

create or replace view rpt.fact_cdn_flow_daily_v as
select round(sum(t.flow_000001)/(1024*1024*1024), 2) as flow_000001       --单位 TB
      ,round(sum(t.flow_000002)/(1024*1024*1024), 2) as flow_000002
      ,round(sum(t.flow_000003)/(1024*1024*1024), 2) as flow_000003
      ,round(sum(t.flow_000004)/(1024*1024*1024), 2) as flow_000004
      ,round(sum(t.flow_000005)/(1024*1024*1024), 2) as flow_000005
      ,round(sum(t.flow_000006)/(1024*1024*1024), 2) as flow_000006
      ,round(sum(t.flow_000007)/(1024*1024*1024), 2) as flow_000007
      ,round(sum(t.flow_000008)/(1024*1024*1024), 2) as flow_000008
      ,round(sum(t.flow_000009)/(1024*1024*1024), 2) as flow_000009
      ,round(sum(t.flow_000010)/(1024*1024*1024), 2) as flow_000010
      ,round(sum(t.flow_000011)/(1024*1024*1024), 2) as flow_000011
      ,round(sum(t.flow_000012)/(1024*1024*1024), 2) as flow_000012
      ,round(sum(t.flow_000013)/(1024*1024*1024), 2) as flow_000013
      ,round(sum(t.flow_000014)/(1024*1024*1024), 2) as flow_000014
      ,round(sum(t.flow_000015)/(1024*1024*1024), 2) as flow_000015
  from (                
        select      
               case when b.belong_type_id = '00001' and b.broadcast_type_id <> '-998' then a.flow_kb else 0 end flow_000001    --基地总流量             1908.45  
              ,case when a.net_type_id in ('1', '2', '5') then a.flow_kb else 0 end flow_000002                                --移动数据流量           925.0
              ,case when a.net_type_id = '5' then a.flow_kb else 0 end flow_000003                                             --移动4G流量             376.33  
              ,case when a.net_type_id not in ('1', '2', '5') then a.flow_kb else 0 end flow_000004                            --非移动数据流量         983.45 
              ,case when c.term_video_type_id = 'TV00001' then a.flow_kb else 0 end flow_000005                                --和视频流量             811.08   
              ,case when c.term_video_type_id = 'TV00002' then a.flow_kb else 0 end flow_000006                                --和视界流量             3.17 
              ,case when a.broadcast_type_id = '12' then a.flow_kb else 0 end flow_000007                                      --直播流量               1458.89 
              ,case when c.term_video_soft_id in ('TST000003', 'TST000004') then a.flow_kb else 0 end flow_000008              --功能输出总流量         156.12 
              ,case when c.term_video_soft_id = 'TST000003' then a.flow_kb else 0 end flow_000009                              --SDK流量                10.02 
              ,case when c.term_video_soft_id = 'TST000004' then a.flow_kb else 0 end flow_000010                              --OPENAPI流量            146.1 
              ,case when a.cdn_id in ('4000', '4001') then a.flow_kb else 0 end flow_000011                                    --第三方流量             165.28   
              ,case when a.cdn_id in ('1000', '2000') then a.flow_kb else 0 end flow_000012                                    --自建CDN流量            1313.87 
              ,case when c.term_video_type_id = 'TV00303' then a.flow_kb else 0 end flow_000013                                --咪咕视频流量           441.23    
              ,case when c.term_video_type_id = 'TV00203' then a.flow_kb else 0 end flow_000014                                --咪咕影院流量           14.34 
              ,case when c.term_video_type_id = 'TV00306' then a.flow_kb else 0 end flow_000015                                --咪咕直播流量           502.56
          from rptdata.fact_use_detail a
          left join msc.dim_cdn_domain_ex b 
            on a.service_url_domain = b.service_url_domain
          left join rpt.dim_term_prod_v c
            on a.termprod_id = c.term_prod_id
         where a.src_file_day = '${SRC_FILE_DAY}'   
       ) t ;

       
       
---------------------------------------------------------------------------------------------------------------------------------------------------- 
----------------------------------------------------------------------------------------------------------------------------------------------------
--第三版
create view rpt.fact_cdn_flow_daily_v as    --防止后续出现同名视图将其覆盖
select t.stat_date
      ,0 as base_all_flow       
      ,round(sum(t.mobile_data_flow)/(1024*1024*1024), 2) as mobile_data_flow       --单位 TB                   stat_date             
      ,round(sum(t.mobile_4G_flow)/(1024*1024*1024), 2) as mobile_4G_flow                                           
      ,round(sum(t.non-mobile_data_flow)/(1024*1024*1024), 2) as non-mobile_data_flow                                       
      ,round(sum(t.HSP_flow)/(1024*1024*1024), 2) as HSP_flow                                           
      ,round(sum(t.HSJ_flow)/(1024*1024*1024), 2) as HSJ_flow                                   
      ,round(sum(t.live_broadcast_flow)/(1024*1024*1024), 2) as live_broadcast_flow                                                 
      ,round(sum(t.abil_output_all_flow)/(1024*1024*1024), 2) as abil_output_all_flow                                               
      ,round(sum(t.SDK_flow)/(1024*1024*1024), 2) as SDK_flow                                       
      ,round(sum(t.OPENAPI_flow)/(1024*1024*1024), 2) as OPENAPI_flow                                   
      ,round(sum(t.third_part_flow)/(1024*1024*1024), 2) as third_part_flow                                                 
      ,round(sum(t.self-build_CDN_flow)/(1024*1024*1024), 2) as self-build_CDN_flow                                             
      ,round(sum(t.migu_video_flow)/(1024*1024*1024), 2) as migu_video_flow             
      ,round(sum(t.migu_movie_flow)/(1024*1024*1024), 2) as migu_movie_flow                                         
      ,round(sum(t.migu_live_flow)/(1024*1024*1024), 2) as migu_live_flow                                           
  from (                                                                                                        
        select a.src_file_day as stat_date                                                                                  --数据日期
              ,0 as base_all_flow                                                                                           --基地总流量             1908.45  
              ,case when a.net_type_id in ('1', '2', '5') then a.flow_kb else 0 end mobile_data_flow                        --移动数据流量           925.0
              ,case when a.net_type_id = '5' then a.flow_kb else 0 end mobile_4G_flow                                       --移动4G流量             376.33  
              ,case when a.net_type_id not in ('1', '2', '5') then a.flow_kb else 0 end non-mobile_data_flow                --非移动数据流量         983.45 
              ,case when c.term_video_type_id = 'TV00001' then a.flow_kb else 0 end HSP_flow                                --和视频流量             811.08   
              ,case when c.term_video_type_id = 'TV00002' then a.flow_kb else 0 end HSJ_flow                                --和视界流量             3.17 
              ,case when a.broadcast_type_id = '12' then a.flow_kb else 0 end live_broadcast_flow                           --直播流量               1458.89 
              ,case when c.term_video_soft_id in ('TST000003', 'TST000004') then a.flow_kb else 0 end abil_output_all_flow  --能力输出总流量         156.12 
              ,case when c.term_video_soft_id = 'TST000003' then a.flow_kb else 0 end SDK_flow                              --SDK流量                10.02 
              ,case when c.term_video_soft_id = 'TST000004' then a.flow_kb else 0 end OPENAPI_flow                          --OPENAPI流量            146.1 
              ,case when a.cdn_id in ('4000', '4001') then a.flow_kb else 0 end third_part_flow                             --第三方流量             165.28   
              ,case when a.cdn_id in ('1000', '2000') then a.flow_kb else 0 end self-build_CDN_flow                         --自建CDN流量            1313.87 
              ,case when c.term_video_type_id = 'TV00303' then a.flow_kb else 0 end migu_video_flow                         --咪咕视频流量           441.23    
              ,case when c.term_video_type_id = 'TV00203' then a.flow_kb else 0 end migu_movie_flow                         --咪咕影院流量           14.34 
              ,case when c.term_video_type_id = 'TV00306' then a.flow_kb else 0 end migu_live_flow                          --咪咕直播流量           502.56
          from rptdata.fact_use_detail a
          left join msc.dim_cdn_domain_ex b 
            on a.service_url_domain = b.service_url_domain
          left join rpt.dim_term_prod_v c
            on a.termprod_id = c.term_prod_id
       ) t 
  group by t.stat_date;

  
---------------------------------------------------------------------------------------------------------------------------------------------------- 
----------------------------------------------------------------------------------------------------------------------------------------------------




       
       
        
--数据源
        
TV00303 咪咕视频
TV00306 咪咕直播
TV00203 咪咕影院
TV00002 和视界
TV00001 和视频

TST000003   SDK
TST000004   OPENAPI

desc rptdata.fact_use_detail;
OK
play_session_id         string            --播放会话ID                          
usernum_id              string            --用户号码  
user_id                 string            --用户ID            
user_type_id            string            --用户类型            
broadcast_type_id       string            --播放方式         *************** 12  直播流量
use_begin_time          string            --播放开始时间          
use_end_time            string            --播放结束时间          
duration_sec            bigint            --播放时长 
flow_kb                 bigint            --播放流量        !!! 
use_source_ip           string            --用户源IP           
program_id              string            --播放节目ID          
chrgprod_id             string            --计费产品ID          
content_id              string            --播放内容ID          
net_type_id             string            --网络类型       *******************
plat_id                 string            --门户ID            
channel_id              string            --渠道ID                        
app_version_code        string            --APP版本号                          
visit_session_id        string            --访问会话ID                          
imei                    string            --播放用户IMEI（解密）                        
region_id               string            --IP归属地区      !!!            
ip_prov_id              string            --IP归属省份ID    !!!                 
termprod_id             string            --终端产品ID      ******************rpt.dim_term_prod_v
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
service_url_domain      string            --分区 服务 URL 域名       dim_cdn_domin_ex
   
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

desc msc.dim_cdn_domain_ex;
OK
service_url_domain      string                                      
belong_type             string                                      
belong_type_id          string                                      
business_type           string                                      
broadcast_type_id       string                                      
broadcast_type          string                                      
remark                  string


