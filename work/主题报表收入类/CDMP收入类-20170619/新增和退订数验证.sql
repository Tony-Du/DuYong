
select count(*) cnt
  from (
        select a.sub_business_id
              ,a.order_msisdn_region_id
              ,a.payment_msisdn_region_id
              ,a.channel_id
              ,a.order_user_id
          from rptdata.fact_order_item_detail a 
         where '201707' >=  '201706'
           and a.order_status in (5,9)   --新增 flag
           and a.src_file_day between '20170701' and '20170731'
       ) t 
+----------+--+
|   cnt    |
+----------+--+
| 8281662  |
+----------+--+



select count(*) cnt
  from (
        select a.sub_business_id
              ,a.order_msisdn_region_id
              ,a.payment_msisdn_region_id
              ,a.channel_id
              ,a.order_user_id
          from rptdata.fact_order_item_detail a 
         where '201706' >=  '201706'
           and a.order_status in (5,9)   --新增 flag
           and a.src_file_day between '20170601' and '20170631'
       ) t 
+-----------+--+
|    cnt    |
+-----------+--+
| 13638251  |
+-----------+--+


----------------------------------------------------


select count(*) cnt
  from (
        select a.sub_business_id
              ,a.order_msisdn_region_id
              ,a.payment_msisdn_region_id
              ,a.channel_id
              ,a.order_user_id
          from rptdata.fact_order_item_detail a 
         where '201707' >=  '201706'
           and a.order_status in (12,14,15,16) --退订 flag
           and a.src_file_day between '20170701' and '20170731'
       ) t        
+----------+--+
|   cnt    |
+----------+--+
| 9390579  |
+----------+--+




select count(*) cnt
  from (
        select a.sub_business_id
              ,a.order_msisdn_region_id
              ,a.payment_msisdn_region_id
              ,a.channel_id
              ,a.order_user_id
          from rptdata.fact_order_item_detail a 
         where '201706' >=  '201706'
           and a.order_status in (12,14,15,16) --退订 flag
           and a.src_file_day between '20170601' and '20170631'
       ) t        
+----------+--+
|   cnt    |
+----------+--+
| 8169386  |
+----------+--+

       
================================================================================================================================================================


select count(*) cnt
  from (
        select a.sub_busi_id as sub_business_id
              ,a.region_id as order_msisdn_region_id
              ,a.region_id as payment_msisdn_region_id
              ,a.chn_id_new as channel_id
              ,b.user_id as order_user_id
          from cdmp_dw.td_aaa_order_log_d a 
          join rptdata.dim_userid_usernum b 
            on a.serv_number = b.user_num
         where '201701' <=  '201705' 
           and a.order_type = 1          --新增 flag
           and a.src_source_day between '20170101' and '20170131'  
       ) t 
+----------+--+
|   cnt    |
+----------+--+
| 7822162  |
+----------+--+       


select count(*) cnt
  from (
        select a.sub_busi_id as sub_business_id
              ,a.region_id as order_msisdn_region_id
              ,a.region_id as payment_msisdn_region_id
              ,a.chn_id_new as channel_id
              ,b.user_id as order_user_id
          from cdmp_dw.td_aaa_order_log_d a 
          join rptdata.dim_userid_usernum b 
            on a.serv_number = b.user_num
         where '201702' <=  '201705' 
           and a.order_type = 1          --新增 flag
           and a.src_source_day between '20170201' and '20170231'  
       ) t 
+----------+--+
|   cnt    |
+----------+--+
| 6206852  |
+----------+--+     
       
       
select count(*) cnt
  from (
        select a.sub_busi_id as sub_business_id
              ,a.region_id as order_msisdn_region_id
              ,a.region_id as payment_msisdn_region_id
              ,a.chn_id_new as channel_id
              ,b.user_id as order_user_id
          from cdmp_dw.td_aaa_order_log_d a 
          join rptdata.dim_userid_usernum b 
            on a.serv_number = b.user_num
         where '201703' <=  '201705' 
           and a.order_type = 1          --新增 flag
           and a.src_source_day between '20170301' and '20170331'  
       ) t 
+----------+--+
|   cnt    |
+----------+--+
| 6823642  |
+----------+--+     
       
       
select count(*) cnt
  from (
        select a.sub_busi_id as sub_business_id
              ,a.region_id as order_msisdn_region_id
              ,a.region_id as payment_msisdn_region_id
              ,a.chn_id_new as channel_id
              ,b.user_id as order_user_id
          from cdmp_dw.td_aaa_order_log_d a 
          join rptdata.dim_userid_usernum b 
            on a.serv_number = b.user_num
         where '201704' <=  '201705' 
           and a.order_type = 1          --新增 flag
           and a.src_source_day between '20170401' and '20170431'  
       ) t
+----------+--+
|   cnt    |
+----------+--+
| 6297258  |
+----------+--+


       
select count(*) cnt
  from (
        select a.sub_busi_id as sub_business_id
              ,a.region_id as order_msisdn_region_id
              ,a.region_id as payment_msisdn_region_id
              ,a.chn_id_new as channel_id
              ,b.user_id as order_user_id
          from cdmp_dw.td_aaa_order_log_d a 
          join rptdata.dim_userid_usernum b 
            on a.serv_number = b.user_num
         where '201705' <=  '201705' 
           and a.order_type = 1          --新增 flag
           and a.src_source_day between '20170501' and '20170531'  
       ) t       
+-----------+--+
|    cnt    |
+-----------+--+
| 10618116  |
+-----------+--+  

       
-----------------------------------------       

select count(*) cnt
  from (
        select a.sub_busi_id as sub_business_id
              ,a.region_id as order_msisdn_region_id
              ,a.region_id as payment_msisdn_region_id
              ,a.chn_id_new as channel_id
              ,b.user_id as order_user_id
          from cdmp_dw.td_aaa_order_log_d a 
          join rptdata.dim_userid_usernum b 
            on a.serv_number = b.user_num
         where '201701' <=  '201705' 
           and a.order_type = 2          --退订 flag
           and a.src_source_day between '20170101' and '20170131'  
       ) t 
+----------+--+
|   cnt    |
+----------+--+
| 4723500  |
+----------+--+       


select count(*) cnt
  from (
        select a.sub_busi_id as sub_business_id
              ,a.region_id as order_msisdn_region_id
              ,a.region_id as payment_msisdn_region_id
              ,a.chn_id_new as channel_id
              ,b.user_id as order_user_id
          from cdmp_dw.td_aaa_order_log_d a 
          join rptdata.dim_userid_usernum b 
            on a.serv_number = b.user_num
         where '201702' <=  '201705' 
           and a.order_type = 2          --退订 flag
           and a.src_source_day between '20170201' and '20170231'  
       ) t        
+----------+--+
|   cnt    |
+----------+--+
| 3942049  |
+----------+--+      
       
       

select count(*) cnt
  from (
        select a.sub_busi_id as sub_business_id
              ,a.region_id as order_msisdn_region_id
              ,a.region_id as payment_msisdn_region_id
              ,a.chn_id_new as channel_id
              ,b.user_id as order_user_id
          from cdmp_dw.td_aaa_order_log_d a 
          join rptdata.dim_userid_usernum b 
            on a.serv_number = b.user_num
         where '201703' <=  '201705' 
           and a.order_type = 2          --退订 flag
           and a.src_source_day between '20170301' and '20170331'  
       ) t        
+----------+--+
|   cnt    |
+----------+--+
| 4841298  |
+----------+--+



select count(*) cnt
  from (
        select a.sub_busi_id as sub_business_id
              ,a.region_id as order_msisdn_region_id
              ,a.region_id as payment_msisdn_region_id
              ,a.chn_id_new as channel_id
              ,b.user_id as order_user_id
          from cdmp_dw.td_aaa_order_log_d a 
          join rptdata.dim_userid_usernum b 
            on a.serv_number = b.user_num
         where '201704' <=  '201705' 
           and a.order_type = 2          --退订 flag
           and a.src_source_day between '20170401' and '20170431'  
       ) t     
+----------+--+
|   cnt    |
+----------+--+
| 5846996  |
+----------+--+



select count(*) cnt
  from (
        select a.sub_busi_id as sub_business_id
              ,a.region_id as order_msisdn_region_id
              ,a.region_id as payment_msisdn_region_id
              ,a.chn_id_new as channel_id
              ,b.user_id as order_user_id
          from cdmp_dw.td_aaa_order_log_d a 
          join rptdata.dim_userid_usernum b 
            on a.serv_number = b.user_num
         where '201705' <=  '201705' 
           and a.order_type = 2          --退订 flag
           and a.src_source_day between '20170501' and '20170531'  
       ) t         
+----------+--+
|   cnt    |
+----------+--+
| 4751685  |
+----------+--+
       