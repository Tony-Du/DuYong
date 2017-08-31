with temp_dim_fcfj_tbl as (  
select company_id, business_id, sub_busi_id              
  from qushupingtai.113_dataset_table a 
 group by company_id, business_id, sub_busi_id   
),
temp_in_order_user_tbl as (
select a.sub_business_id as sub_busi_id
      ,a.order_user_id
  from rptdata.fact_order_daily_snapshot a
  join rptdata.dim_charge_product c
    on a.product_id = c.chrgprod_id   
 where '20170401' >= '20170601' 
   and a.snapshot_day = '20170430'  
   and c.chrgprod_price > 0   
 group by a.sub_business_id, a.order_user_id  

 union all
 
select c.sub_busi_bdid as sub_busi_id
      ,a.usernum as order_user_id    
  from intdata.ugc_90104_monthorder a   
  join rptdata.dim_charge_product c
    on a.product_id = c.chrgprod_id
 where '20170401' >= '20170501'
   and '20170430' <= '20170531' 
   and a.src_file_day = '20170430'
   and c.chrgprod_price >0
 group by c.sub_busi_bdid, a.usernum

 union all
 
select c.sub_busi_bdid as sub_busi_id
      ,a.usernum as order_user_id
  from intdata.ugc_90104_monthorder_union a 
  join rptdata.dim_charge_product c
    on a.product_id = c.chrgprod_id
 where '20170401' >= '20170401'
   and '20170430' <= '20170430' 
   and a.src_file_day = '20170430'
   and c.chrgprod_price >0  
 group by c.sub_busi_bdid, a.usernum 
)
--insert overwrite table qushupingtai.qspt_hzyykh_fcfj_in_order_user partition (src_file_month = '${SRC_FILE_MONTH}') 
select b.company_id
      ,count(t.order_user_id) as in_order_user_cnt
  from temp_in_order_user_tbl t 
  join temp_dim_fcfj_tbl b  
    on t.sub_busi_id = b.sub_busi_id   
 group by b.company_id ; 
 
 
 +---------------+--------------------+--+
| b.company_id  | in_order_user_cnt  |
+---------------+--------------------+--+
| C00001        | 1911105            |
| C00002        | 5760438            |
| C00003        | 4155195            |
| C00004        | 4880228            |
| C00005        | 3915281            |
| C00007        | 4253343            |
| C00008        | 1222380            |
| C00009        | 624495             |
| C00010        | 579126             |
| C00011        | 900728             |
| C00012        | 1800391            |
| C00014        | 763998             |
| C00015        | 297586             |
| C00016        | 2690450            |
| C00017        | 1294305            |
| C00018        | 66405              |
| C00019        | 22319              |
| C00020        | 1617547            |
| C00022        | 225532             |
| C00023        | 506499             |
| C00024        | 1062854            |
| C00025        | 85370              |
| C00026        | 293832             |
| C00027        | 777327             |
| C00028        | 860861             |
| C00029        | 160810             |
| C00030        | 691792             |
| C00031        | 486259             |
| C00032        | 652429             |
| C00033        | 611737             |
| C00034        | 388174             |
| C00035        | 304056             |
| C00036        | 351419             |
| C00037        | 206528             |
| C00038        | 1077084            |
| C00039        | 433082             |
| C00040        | 801720             |
| C00041        | 2689769            |
| C00042        | 193989             |
| C00043        | 153954             |
| C00044        | 96837              |
| C00045        | 27                 |
| C00046        | 74766              |
| C00047        | 3127               |
| C00048        | 511866             |
| C00049        | 17290              |
+---------------+--------------------+--+
with temp_dim_fcfj_tbl as (  
select company_id, business_id, sub_busi_id              
  from qushupingtai.113_dataset_table a 
 group by company_id, business_id, sub_busi_id   
),
temp_in_order_user_tbl as (
select c.sub_busi_bdid as sub_busi_id
      ,a.usernum as order_user_id
  from intdata.ugc_90104_monthorder_union a 
  join rptdata.dim_charge_product c
    on a.product_id = c.chrgprod_id
 where '20170401' >= '20170401'
   and '20170430' <= '20170430' 
   and a.src_file_day = '20170430'
   and c.chrgprod_price >0  
 group by c.sub_busi_bdid, a.usernum 
)
--insert overwrite table qushupingtai.qspt_hzyykh_fcfj_in_order_user partition (src_file_month = '${SRC_FILE_MONTH}') 
select b.company_id
      ,count(t.order_user_id) as in_order_user_cnt
  from temp_in_order_user_tbl t 
  join temp_dim_fcfj_tbl b  
    on t.sub_busi_id = b.sub_busi_id   
 group by b.company_id ; 
+---------------+--------------------+--+
| b.company_id  | in_order_user_cnt  |
+---------------+--------------------+--+
| C00001        | 1911105            |
| C00002        | 5760438            |
| C00003        | 4155195            |
| C00004        | 4880228            |
| C00005        | 3915281            |
| C00007        | 4253343            |
| C00008        | 1222380            |
| C00009        | 624495             |
| C00010        | 579126             |
| C00011        | 900728             |
| C00012        | 1800391            |
| C00014        | 763998             |
| C00015        | 297586             |
| C00016        | 2690450            |
| C00017        | 1294305            |
| C00018        | 66405              |
| C00019        | 22319              |
| C00020        | 1617547            |
| C00022        | 225532             |
| C00023        | 506499             |
| C00024        | 1062854            |
| C00025        | 85370              |
| C00026        | 293832             |
| C00027        | 777327             |
| C00028        | 860861             |
| C00029        | 160810             |
| C00030        | 691792             |
| C00031        | 486259             |
| C00032        | 652429             |
| C00033        | 611737             |
| C00034        | 388174             |
| C00035        | 304056             |
| C00036        | 351419             |
| C00037        | 206528             |
| C00038        | 1077084            |
| C00039        | 433082             |
| C00040        | 801720             |
| C00041        | 2689769            |
| C00042        | 193989             |
| C00043        | 153954             |
| C00044        | 96837              |
| C00045        | 27                 |
| C00046        | 74766              |
| C00047        | 3127               |
| C00048        | 511866             |
| C00049        | 17290              |
+---------------+--------------------+--+

 
 
 



with temp_dim_fcfj_tbl as (  
select company_id, business_id, sub_busi_id              
  from qushupingtai.113_dataset_table a 
 group by company_id, business_id, sub_busi_id   
),
temp_in_order_user_tbl as (
select a.sub_business_id as sub_busi_id
      ,a.order_user_id
  from rptdata.fact_order_daily_snapshot a
  join rptdata.dim_charge_product c
    on a.product_id = c.chrgprod_id   
 where '20170401' >= '20170601' 
   and a.snapshot_day = '20170430'  
   and c.chrgprod_price > 0   
 group by a.sub_business_id, a.order_user_id  

 union all
 
select c.sub_busi_bdid as sub_busi_id
      ,a.usernum as order_user_id    
  from intdata.ugc_90104_monthorder a   
  join rptdata.dim_charge_product c
    on a.product_id = c.chrgprod_id
 where '20170401' >= '20170501'
   and '20170430' <= '20170531' 
   and a.src_file_day = '20170430'
   and c.chrgprod_price >0
 group by c.sub_busi_bdid, a.usernum

 union all
 
select c.sub_busi_bdid as sub_busi_id
      ,a.usernum as order_user_id
  from intdata.ugc_90104_monthorder_union a 
  join rptdata.dim_charge_product c
    on a.product_id = c.chrgprod_id
 where '20170401' >= '20170401'
   and '20170430' <= '20170430' 
   and a.src_file_day = '20170430'
   and c.chrgprod_price >0  
 group by c.sub_busi_bdid, a.usernum 
)
--insert overwrite table qushupingtai.qspt_hzyykh_fcfj_in_order_user partition (src_file_month = '${SRC_FILE_MONTH}') 
select a.company_id
      ,count(a.order_user_id) as in_order_user_cnt
  from (
        select b.company_id
              ,t.order_user_id
          from temp_in_order_user_tbl t 
          join temp_dim_fcfj_tbl b  
            on t.sub_busi_id = b.sub_busi_id 
       ) a            
 group by a.company_id ; 
 
+---------------+--------------------+--+
| a.company_id  | in_order_user_cnt  |
+---------------+--------------------+--+
| C00001        | 1911105            |
| C00002        | 5760438            |
| C00003        | 4155195            |
| C00004        | 4880228            |
| C00005        | 3915281            |
| C00007        | 4253343            |
| C00008        | 1222380            |
| C00009        | 624495             |
| C00010        | 579126             |
| C00011        | 900728             |
| C00012        | 1800391            |
| C00014        | 763998             |
| C00015        | 297586             |
| C00016        | 2690450            |
| C00017        | 1294305            |
| C00018        | 66405              |
| C00019        | 22319              |
| C00020        | 1617547            |
| C00022        | 225532             |
| C00023        | 506499             |
| C00024        | 1062854            |
| C00025        | 85370              |
| C00026        | 293832             |
| C00027        | 777327             |
| C00028        | 860861             |
| C00029        | 160810             |
| C00030        | 691792             |
| C00031        | 486259             |
| C00032        | 652429             |
| C00033        | 611737             |
| C00034        | 388174             |
| C00035        | 304056             |
| C00036        | 351419             |
| C00037        | 206528             |
| C00038        | 1077084            |
| C00039        | 433082             |
| C00040        | 801720             |
| C00041        | 2689769            |
| C00042        | 193989             |
| C00043        | 153954             |
| C00044        | 96837              |
| C00045        | 27                 |
| C00046        | 74766              |
| C00047        | 3127               |
| C00048        | 511866             |
| C00049        | 17290              |
+---------------+--------------------+--+ 
 
 
 
 

 
with temp_dim_fcfj_tbl as (  
select company_id, business_id, sub_busi_id              
  from qushupingtai.113_dataset_table a 
 group by company_id, business_id, sub_busi_id   
),
temp_in_order_user_tbl as (
select b.company_id
      ,a.order_user_id
  from rptdata.fact_order_daily_snapshot a
  join rptdata.dim_charge_product c
    on a.product_id = c.chrgprod_id  
  join temp_dim_fcfj_tbl b  
    on c.sub_busi_bdid = b.sub_busi_id    
 where '20170401' >= '20170601' 
   and a.snapshot_day = '20170430'  
   and c.chrgprod_price > 0   
 group by b.company_id, a.order_user_id  

 union all
 
select b.company_id
      ,a.usernum as order_user_id    
  from intdata.ugc_90104_monthorder a   
  join rptdata.dim_charge_product c
    on a.product_id = c.chrgprod_id
  join temp_dim_fcfj_tbl b  
    on c.sub_busi_bdid = b.sub_busi_id
 where '20170401' >= '20170501'
   and '20170430' <= '20170531' 
   and a.src_file_day = '20170430'
   and c.chrgprod_price >0
 group by b.company_id, a.usernum

 union all
 
select b.company_id
      ,a.usernum as order_user_id
  from intdata.ugc_90104_monthorder_union a 
  join rptdata.dim_charge_product c
    on a.product_id = c.chrgprod_id
  join temp_dim_fcfj_tbl b  
    on c.sub_busi_bdid = b.sub_busi_id
 where '20170401' >= '20170401'
   and '20170430' <= '20170430' 
   and a.src_file_day = '20170430'
   and c.chrgprod_price >0  
 group by b.company_id, a.usernum 
)
--insert overwrite table qushupingtai.qspt_hzyykh_fcfj_in_order_user partition (src_file_month = '${SRC_FILE_MONTH}') 
select t.company_id
      ,count(t.order_user_id) as in_order_user_cnt
  from temp_in_order_user_tbl t 
 group by t.company_id ; 
+---------------+--------------------+--+
| t.company_id  | in_order_user_cnt  |
+---------------+--------------------+--+
| C00001        | 1873650            |
| C00002        | 5509527            |
| C00010        | 579126             |
| C00003        | 4079742            |
| C00011        | 899661             |
| C00004        | 4643274            |
| C00012        | 1778239            |
| C00020        | 1576322            |
| C00005        | 3764074            |
| C00014        | 763998             |
| C00022        | 225532             |
| C00030        | 691792             |
| C00007        | 4002683            |
| C00015        | 297586             |
| C00023        | 506499             |
| C00031        | 486259             |
| C00008        | 1184989            |
| C00016        | 2690450            |
| C00024        | 1062854            |
| C00032        | 652429             |
| C00040        | 801720             |
| C00009        | 624442             |
| C00017        | 1283246            |
| C00025        | 85370              |
| C00033        | 611737             |
| C00041        | 2689769            |
| C00018        | 66405              |
| C00026        | 293832             |
| C00034        | 388174             |
| C00042        | 193989             |
| C00019        | 22319              |
| C00027        | 777327             |
| C00035        | 304056             |
| C00043        | 153954             |
| C00028        | 860861             |
| C00036        | 351419             |
| C00044        | 96837              |
| C00029        | 160810             |
| C00037        | 206528             |
| C00045        | 27                 |
| C00038        | 1077084            |
| C00046        | 74766              |
| C00039        | 433082             |
| C00047        | 3127               |
| C00048        | 511866             |
| C00049        | 17290              |
+---------------+--------------------+--+ 
 