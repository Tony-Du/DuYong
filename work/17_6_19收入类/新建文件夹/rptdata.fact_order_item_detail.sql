rptdata.fact_order_item_detail （订单子项明细，日新增收入）

--set hive.auto.convert.join=false;
set mapreduce.job.name=rptdata.fact_order_item_detail_${SRC_FILE_DAY}_${SRC_FILE_HOUR};

set hive.optimize.sort.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

set hive.exec.parallel=true;	

with program_bus_busi as 
(
select program_id, p2.sub_busi_bdid as sub_business_id from rptdata.dim_program p1 left join rptdata.dim_product_package p2 on (p1.pdpk_id = p2.pdpk_id)
),
order_detail as 
(
select * from rptdata.fact_order_detail 
where src_file_day = '${SRC_FILE_DAY}' and src_file_hour = '${SRC_FILE_HOUR}'
),
order_item as
(
select * from intdata.order_item_hist 
where src_file_day = '${SRC_FILE_DAY}' and src_file_hour = '${SRC_FILE_HOUR}'
),
delivery as
(
select * from intdata.order_item_delivery_hist 
where src_file_day = '${SRC_FILE_DAY}' and src_file_hour = '${SRC_FILE_HOUR}'
),
payment_channel as
(
select * from intdata.order_payment_channel_hist 
where src_file_day = '${SRC_FILE_DAY}' and src_file_hour = '${SRC_FILE_HOUR}'
)
INSERT overwrite TABLE rptdata.fact_order_item_detail PARTITION (src_file_day, src_file_hour)
select
    tm.order_id,
    tm.order_last_upt_time,
    
    t1.order_item_id,
    t3.payment_type,
    t3.currency_type,
        
    tm.order_status,
    tm.order_crt_day,
    tm.order_cancel_day,
    tm.client_ip_region_id,
    tm.payment_msisdn_region_id,
    tm.order_msisdn_region_id,
    tm.plat_id,
    tm.channel_id,
    tm.term_prod_id,
    tm.term_prod_version_id,
    tm.trig_order_program_id,
    tm.trig_order_series_program_id,
    
    
    tm.payment_msisdn                ,
    tm.order_phone_num               ,
    tm.sp_id                         ,
    tm.operation_code                ,
    tm.goods_id                      ,
    tm.goods_type                    ,
    a10.goods_name                   ,
    tm.service_code                  ,
    a11.service_name                 ,
    tm.order_crt_time                ,
    tm.order_accept_time             ,
    tm.order_payment_time            ,
    tm.order_cancel_time             ,
    tm.client_ip                     ,
    tm.app_name                      ,
    tm.order_user_id                 ,
    tm.order_user_num                ,
    tm.order_opr_user_id             ,
    tm.order_opr_user_type           ,
    tm.cancel_opr_user_id            ,
    tm.cancel_opr_user_type          ,
    tm.external_order_id             ,
    tm.payment_id                    ,
    tm.order_source_id               ,
    tm.order_source_type             ,
    tm.merchant_account              ,
    tm.portal_type                   ,
    tm.virtual_operation_flag        ,

       
    case when t1.migu_auth_resource_type = 'POMS_PROGRAM_ID' then t1.migu_auth_resource_id else '-997' end order_program_id,
    case when t1.migu_auth_resource_type = 'POMS_ALBUM_ID'  then t1.migu_auth_resource_id else '-997' end order_series_program_id,
    case when t1.migu_auth_resource_type = 'PMS_PRODUCT_ID'  then t1.migu_auth_resource_id else '-997' end product_id,
    case when t1.migu_auth_resource_type = 'POMS_PKG_ID'  then t1.migu_auth_resource_id else '-997' end product_pkg_id,
    case when t1.migu_auth_resource_type IN ('POMS_PROGRAM_ID', 'POMS_ALBUM_ID') then nvl(a6.sub_business_id, '-998')
         when t1.migu_auth_resource_type  = 'PMS_PRODUCT_ID' then nvl(a8.sub_busi_bdid, '-998')
         when t1.migu_auth_resource_type  = 'POMS_PKG_ID' then nvl(a9.sub_busi_bdid, '-998')
         else '-997'
    end as sub_business_id,
    
    case when delivery_handler in ('MIGU_AUTH') then '内容' 
         when delivery_handler in ('MIGU_MOVIE_CARD', 'MIGU_ACCOUNT') then '非内容'
         when delivery_handler in ('justPay') then '代计' 
         else  null end               as order_item_category,
    t1.delivery_handler              as order_item_delivery_handler,
    t2.delivery_status               as order_item_delivery_status,
    t1.auth_type                     as order_item_auth_type,	--授权类型 BOSS-MONTH\Period\copy\times                  
    t1.auth_qty_unit                 as order_item_auth_unit,   --订购单位类型 month\month.day.hour.minute\month\null                 
    t1.auth_qty                      as order_item_auth_qty,    --交付物数量 由业务系统传入（例如amount=6，authtype=PERIOD，periodUnit=MONTH表示授权6个月）
    t1.delivery_qty                  as order_item_delivery_qty,
    t1.unit_price                    as order_item_unit_price,
    t1.migu_auth_resource_id         as order_item_resource_id ,
    t1.migu_auth_resource_type       as order_item_resource_type,
    t2.migu_auth_order_begin_time    as order_item_order_begin_time,
    t2.migu_auth_order_end_time      as order_item_order_end_time,
    t1.migu_account_account_type     as order_item_account_type,

    case when t1.auth_type = 'BOSS_MONTH' OR t3.payment_type = '309' then 'Y' else 'N' end auto_renew_flag,
--below two flags will be set by daily job
    NULL boss_repeat_order_flag,
    NULL boss_last_success_bill_flag,
    t1.migu_account_charge_amt       as order_item_account_charge_amt,    
    t3.payment_amt,
    tm.src_file_day,
    tm.src_file_hour
from order_detail tm 
left join order_item t1 on (tm.src_file_day = t1.src_file_day and tm.src_file_hour = t1.src_file_hour and tm.order_id = t1.order_id and tm.order_last_upt_time = t1.order_last_upt_time)
left join delivery t2 on (t1.src_file_day = t2.src_file_day and t1.src_file_hour = t2.src_file_hour and t1.order_id = t2.order_id and t1.order_last_upt_time = t2.order_last_upt_time and t1.order_item_id = t2.order_item_id)
left join payment_channel t3 on (tm.src_file_day = t3.src_file_day and tm.src_file_hour = t3.src_file_hour and tm.order_id = t3.order_id and tm.order_last_upt_time = t3.order_last_upt_time)
left join program_bus_busi a6 on (t1.migu_auth_resource_id = a6.program_id and t1.migu_auth_resource_type IN ('POMS_PROGRAM_ID', 'POMS_ALBUM_ID'))
left join rptdata.dim_charge_product a8 on (t1.migu_auth_resource_id = a8.chrgprod_id and t1.migu_auth_resource_type = 'PMS_PRODUCT_ID')
left join rptdata.dim_product_package a9 on (t1.migu_auth_resource_id = a9.pdpk_id and t1.migu_auth_resource_type = 'POMS_PKG_ID')
left join intdata.goods a10 on (tm.goods_id = a10.goods_id and tm.goods_type = a10.goods_type)
left join intdata.service a11 on (tm.service_code = a11.service_code)
WHERE t1.delivery_handler IN ('MIGU_AUTH', 'MIGU_MOVIE_CARD', 'MIGU_ACCOUNT', 'justPay')
      and t1.main_delivery_flag = '0'
