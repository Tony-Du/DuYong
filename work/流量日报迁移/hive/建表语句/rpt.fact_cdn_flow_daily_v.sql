
--create or replace view rpt.fact_cdn_flow_daily_v as
create view rpt.fact_cdn_flow_daily_v as	--防止后续出现同名视图将其覆盖
select t.stat_date
	  ,0 as base_all_flow       
      ,round(sum(t.mobile_data_flow)/(1024*1024*1024), 2) as mobile_data_flow		--单位 TB                   stat_date				
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
        select a.src_file_day as stat_date     																				--数据日期
              ,0 as base_all_flow    																					    --基地总流量             1908.45  
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
	   
	   