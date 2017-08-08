

select a.sub_busi_id as sub_business_id
      ,a.region_id as order_msisdn_region_id
      ,a.region_id as payment_msisdn_region_id
      ,a.chn_id_new as channel_id
      ,b.user_id as order_user_id
  from cdmp_dw.td_aaa_order_log_d a 
  join rptdata.dim_userid_usernum b
    on a.serv_number = b.user_num
 where a.order_type = 1
   and a.src_source_day between '20160101' and '20160131'  
   
   
 
select a.sub_busi_id as sub_business_id
      ,a.region_id as order_msisdn_region_id
      ,a.region_id as payment_msisdn_region_id
      ,a.chn_id_new as channel_id
      ,b.user_id as order_user_id
  from cdmp_dw.td_aaa_order_log_d a 
  join rptdata.dim_userid_usernum b
    on a.serv_number = b.user_num
 where a.order_type = 1
   and substr(a.src_source_day, 1, 6) = '201601' 

   
   
   
 hadoop fs -count /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=201601*
           1            1         1080710627 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160101     
           1            1         1399013439 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160102     
           1            1         1288917596 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160103     
           1            1         1267423045 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160104     
           1            1         1331359639 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160105     
           1            1         1131826529 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160106     
           1            1         1109429334 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160107     
           1            1         1112666581 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160108     
           1            1          929176551 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160109     
           1            1         1164139933 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160110     
           1            1          884365042 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160111     
           1            1          765949539 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160112     
           1            1          732729147 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160113     
           1            1          762859716 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160114     
           1            1          734252528 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160115     
           1            1          757806257 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160116     
           1            1          725787921 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160117     
           1            1          678845363 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160118     
           1            1          661711055 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160119     
           1            1          729412935 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160120     
           1            1          688060657 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160121     
           1            1          638600861 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160122     
           1            1          593843882 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160123     
           1            1          620532421 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160124     
           1            1          649364474 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160125     
           1            1          653205387 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160126     
           1            1          696403023 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160127     
           1            1          716416967 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160128     
           1            1          671852597 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160129     
           1            1          641053858 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160130     
           1            1          694297711 /user/hive/warehouse/cdmp_dw.db/td_aaa_order_log_d/src_source_day=20160131     
           
           