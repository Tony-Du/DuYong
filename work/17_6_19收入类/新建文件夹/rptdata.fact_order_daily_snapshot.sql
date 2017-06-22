rptdata.fact_order_daily_snapshot （订购关系日快照）

----------------------------------------------
-- 每月最后一天逻辑  
-- 参数：${DEST_TBL_SCHEMA}：目标表的SCHEMA
--       ${SRC_FILE_DAY}   ：执行周期
--       ${SRC_FILE_MONTH} ：月份
----------------------------------------------
set mapreduce.job.name=${DEST_TBL_SCHEMA}_fact_order_daily_snapshot_${SRC_FILE_DAY};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
--set hive.optimize.skewjoin=true;
set hive.merge.mapredfiles=true;
--set mapreduce.reduce.shuffle.input.buffer.percent=0.3;
--set mapreduce.reduce.shuffle.parallelcopies=3;

with sixty_days_succ_order as (
select tt.*
  from (
		select foid.*,
			   row_number() over(partition by foid.order_id,foid.order_status,foid.payment_type order by src_file_day desc) row_num
		  from rptdata.fact_order_item_detail foid --订单子项明细，日新增
		 where src_file_day > from_unixtime(unix_timestamp('${SRC_FILE_DAY}','yyyyMMdd')-60*60*24*60,'yyyyMMdd')
		   and src_file_day <= '${SRC_FILE_DAY}' --60天以内，包括当前数据周期
		   and order_status in ('5','9')
       ) tt
 where row_num = 1 
),

new_tp_period_order as ( --当天，如30号，新增包周期
select 
     foid.order_id                       ,
     foid.order_last_upt_time            ,
     foid.order_item_id                  ,
     foid.payment_type                   ,
     foid.currency_type                  ,
     foid.order_status                   ,
     foid.order_crt_day                  ,
     foid.order_cancel_day               ,
     foid.client_ip_region_id            , 
     foid.payment_msisdn_region_id       , 
     foid.order_msisdn_region_id         , 
     foid.plat_id                        ,
     foid.channel_id                     ,
     foid.term_prod_id                   , 
     foid.term_prod_version_id           ,
     foid.trig_order_program_id          ,
     foid.trig_order_series_program_id   ,
     foid.payment_msisdn                 ,
     foid.sp_id                          ,
     foid.operation_code                 ,
     foid.goods_id                       ,
     foid.goods_name                     , 
     foid.goods_type                     ,
     foid.service_code                   ,
     foid.service_name                   , 
     foid.order_crt_time                 ,
     foid.order_accept_time              ,
     foid.order_payment_time             ,
     foid.order_cancel_time              ,
     foid.client_ip                      ,
     foid.app_name                       ,
     foid.order_user_id                  ,
     foid.order_user_num                 ,
     foid.order_opr_user_id              ,
     foid.order_opr_user_type            ,
     foid.cancel_opr_user_id             ,
     foid.cancel_opr_user_type           ,
     foid.external_order_id              ,
     foid.payment_id                     ,
     foid.order_source_id                ,
     foid.order_source_type              ,
     foid.merchant_account               ,
     foid.portal_type                    ,
     foid.order_item_category            ,
     foid.virtual_operation_flag         ,
     foid.order_program_id               ,
     foid.order_series_program_id        ,
     foid.product_id                     ,
     foid.product_pkg_id                 ,
     foid.sub_business_id                ,
     foid.order_item_delivery_handler    ,
     foid.order_item_delivery_status     ,
     foid.order_item_auth_type           ,
     foid.order_item_auth_unit           ,
     foid.order_item_auth_qty            ,
     foid.order_item_delivery_qty        ,
     foid.order_item_unit_price          ,
     foid.order_item_resource_id         ,
     foid.order_item_resource_type       ,
     foid.order_item_order_begin_time    ,
     foid.order_item_order_end_time      ,
     foid.payment_amt                    
from rptdata.fact_order_item_detail foid
where src_file_day = '${SRC_FILE_DAY}'
  and order_status in ('5','9') -- 5-订单完成（支付完成、交付完成）9-订单完成（支付完成，交付失败）
  and order_item_auth_type = 'PERIOD' 
  and order_item_auth_unit in ('MONTH','DAY')
  and payment_type not in ('310','311') --310 支付宝续租 311 微信续租
  and order_item_order_end_time > '${SRC_FILE_DAY}'
),


in_differ_contract  as (  --在全量合约表中，不在前一天快照中
select cms.*
  from intdata.contract_monthly_snapshot cms  --合约月全量表
  left join ${DEST_TBL_SCHEMA}.fact_order_daily_snapshot fods  on fods.order_id = cms.order_id 
 where cms.src_file_month = '${SRC_FILE_MONTH}' 
   and cms.contract_status = 'NORMAL'  
   and fods.snapshot_day = from_unixtime(unix_timestamp('${SRC_FILE_DAY}','yyyyMMdd')-60*60*24,'yyyyMMdd')
   and fods.order_id is null --cms中存在，fods中不存在的合约
),


contract_monthly_snap as (
select 
fods.order_id                       ,
fods.order_last_upt_time            ,
fods.order_item_id                  ,
fods.payment_type                   ,
fods.currency_type                  ,
fods.order_status                   ,
fods.order_crt_day                  ,
fods.order_cancel_day               ,
fods.client_ip_region_id            , 
fods.payment_msisdn_region_id       , 
fods.order_msisdn_region_id         , 
fods.plat_id                        ,
fods.channel_id                     ,
fods.term_prod_id                   , 
fods.term_prod_version_id           ,
fods.trig_order_program_id          ,
fods.trig_order_series_program_id   ,
fods.payment_msisdn                 ,
fods.sp_id                          ,
fods.operation_code                 ,
fods.goods_id                       ,
fods.goods_name                     , 
fods.goods_type                     ,
fods.service_code                   ,
fods.service_name                   , 
fods.order_crt_time                 ,
fods.order_accept_time              ,
fods.order_payment_time             ,
fods.order_cancel_time              ,
fods.client_ip                      ,
fods.app_name                       ,
fods.order_user_id                  ,
fods.order_user_num                 ,
fods.order_opr_user_id              ,
fods.order_opr_user_type            ,
fods.cancel_opr_user_id             ,
fods.cancel_opr_user_type           ,
fods.external_order_id              ,
fods.payment_id                     ,
fods.order_source_id                ,
fods.order_source_type              ,
fods.merchant_account               ,
fods.portal_type                    ,
fods.order_item_category            ,
fods.virtual_operation_flag         ,
fods.order_program_id               ,
fods.order_series_program_id        ,
fods.product_id                     ,
fods.product_pkg_id                 ,
fods.sub_business_id                ,
fods.order_item_delivery_handler    ,
fods.order_item_delivery_status     ,
fods.order_item_auth_type           ,
fods.order_item_auth_unit           ,
fods.order_item_auth_qty            ,
fods.order_item_delivery_qty        ,
fods.order_item_unit_price          ,
fods.order_item_resource_id         ,
fods.order_item_resource_type       ,
fods.order_item_order_begin_time    ,
fods.order_item_order_end_time      ,
fods.payment_amt                    
from  ${DEST_TBL_SCHEMA}.fact_order_daily_snapshot fods
join intdata.contract_monthly_snapshot cms  --既在fods, 也在cms中
  on fods.order_id = cms.order_id and cms.contract_status = 'NORMAL'
where fods.snapshot_day = from_unixtime(unix_timestamp('${SRC_FILE_DAY}','yyyyMMdd')-60*60*24,'yyyyMMdd')
  and cms.src_file_month = '${SRC_FILE_MONTH}' 
  
UNION ALL

select 
idc.order_id                        ,
foid.order_last_upt_time            ,
foid.order_item_id                  ,
foid.payment_type                   ,
foid.currency_type                  ,
foid.order_status                   ,
foid.order_crt_day                  ,
foid.order_cancel_day               ,
foid.client_ip_region_id            , 
foid.payment_msisdn_region_id       , 
foid.order_msisdn_region_id         , 
foid.plat_id                        ,
foid.channel_id                     ,
foid.term_prod_id                   , 
foid.term_prod_version_id           ,
foid.trig_order_program_id          ,
foid.trig_order_series_program_id   ,
idc.payment_msisdn                  ,
idc.sp_id                           ,
idc.operation_code                  ,
foid.goods_id                       ,
foid.goods_name                     , 
foid.goods_type                     ,
foid.service_code                   ,
foid.service_name                   , 
foid.order_crt_time                 ,
foid.order_accept_time              ,
foid.order_payment_time             ,
foid.order_cancel_time              ,
foid.client_ip                      ,
foid.app_name                       ,
idc.order_user_id                   ,
foid.order_user_num                 ,
foid.order_opr_user_id              ,
foid.order_opr_user_type            ,
foid.cancel_opr_user_id             ,
foid.cancel_opr_user_type           ,
foid.external_order_id              ,
foid.payment_id                     ,
foid.order_source_id                ,
foid.order_source_type              ,
foid.merchant_account               ,
foid.portal_type                    ,
foid.order_item_category            ,
foid.virtual_operation_flag         ,
foid.order_program_id               ,
foid.order_series_program_id        ,
idc.product_id                      ,
foid.product_pkg_id                 ,
foid.sub_business_id                ,
foid.order_item_delivery_handler    ,
foid.order_item_delivery_status     ,
foid.order_item_auth_type           ,
foid.order_item_auth_unit           ,
foid.order_item_auth_qty            ,
foid.order_item_delivery_qty        ,
foid.order_item_unit_price          ,
foid.order_item_resource_id         ,
foid.order_item_resource_type       ,
foid.order_item_order_begin_time    ,
foid.order_item_order_end_time      ,
foid.payment_amt                    
from in_differ_contract idc --在全量合约表中，不在前一天快照中
join sixty_days_succ_order foid on idc.order_id = foid.order_id  
),

in_valid_contract as (
select fods.*
from ${DEST_TBL_SCHEMA}.fact_order_daily_snapshot fods  --一天前的快照
where snapshot_day = from_unixtime(unix_timestamp('${SRC_FILE_DAY}','yyyyMMdd')-60*60*24,'yyyyMMdd')
  and ( order_item_auth_type = 'BOSS_MONTH' OR  payment_type in ('310','311' ))
  and order_item_order_end_time > '${SRC_FILE_DAY}' 
)

insert overwrite table ${DEST_TBL_SCHEMA}.fact_order_daily_snapshot partition(snapshot_day='${SRC_FILE_DAY}')
SELECT 
order_id                       ,
order_last_upt_time            ,
order_item_id                  ,
payment_type                   ,
currency_type                  ,
order_status                   ,
order_crt_day                  ,
order_cancel_day               ,
client_ip_region_id            , 
payment_msisdn_region_id       , 
order_msisdn_region_id         , 
plat_id                        ,
channel_id                     ,
term_prod_id                   , 
term_prod_version_id           ,
trig_order_program_id          ,
trig_order_series_program_id   ,
payment_msisdn                 ,
sp_id                          ,
operation_code                 ,
goods_id                       ,
goods_name                     , 
goods_type                     ,
service_code                   ,
service_name                   , 
order_crt_time                 ,
order_accept_time              ,
order_payment_time             ,
order_cancel_time              ,
client_ip                      ,
app_name                       ,
order_user_id                  ,
order_user_num                 ,
order_opr_user_id              ,
order_opr_user_type            ,
cancel_opr_user_id             ,
cancel_opr_user_type           ,
external_order_id              ,
payment_id                     ,
order_source_id                ,
order_source_type              ,
merchant_account               ,
portal_type                    ,
order_item_category            ,
virtual_operation_flag         ,
order_program_id               ,
order_series_program_id        ,
product_id                     ,
product_pkg_id                 ,
sub_business_id                ,
order_item_delivery_handler    ,
order_item_delivery_status     ,
order_item_auth_type           ,
order_item_auth_unit           ,
order_item_auth_qty            ,
order_item_delivery_qty        ,
order_item_unit_price          ,
order_item_resource_id         ,
order_item_resource_type       ,
order_item_order_begin_time    ,
order_item_order_end_time      ,
payment_amt                    
FROM new_tp_period_order 

UNION  ALL 

SELECT 
order_id                       ,
order_last_upt_time            ,
order_item_id                  ,
payment_type                   ,
currency_type                  ,
order_status                   ,
order_crt_day                  ,
order_cancel_day               ,
client_ip_region_id            , 
payment_msisdn_region_id       , 
order_msisdn_region_id         , 
plat_id                        ,
channel_id                     ,
term_prod_id                   , 
term_prod_version_id           ,
trig_order_program_id          ,
trig_order_series_program_id   ,
payment_msisdn                 ,
sp_id                          ,
operation_code                 ,
goods_id                       ,
goods_name                     , 
goods_type                     ,
service_code                   ,
service_name                   , 
order_crt_time                 ,
order_accept_time              ,
order_payment_time             ,
order_cancel_time              ,
client_ip                      ,
app_name                       ,
order_user_id                  ,
order_user_num                 ,
order_opr_user_id              ,
order_opr_user_type            ,
cancel_opr_user_id             ,
cancel_opr_user_type           ,
external_order_id              ,
payment_id                     ,
order_source_id                ,
order_source_type              ,
merchant_account               ,
portal_type                    ,
order_item_category            ,
virtual_operation_flag         ,
order_program_id               ,
order_series_program_id        ,
product_id                     ,
product_pkg_id                 ,
sub_business_id                ,
order_item_delivery_handler    ,
order_item_delivery_status     ,
order_item_auth_type           ,
order_item_auth_unit           ,
order_item_auth_qty            ,
order_item_delivery_qty        ,
order_item_unit_price          ,
order_item_resource_id         ,
order_item_resource_type       ,
order_item_order_begin_time    ,
order_item_order_end_time      ,
payment_amt                    
FROM contract_monthly_snap 

UNION ALL 

SELECT 
order_id                       ,
order_last_upt_time            ,
order_item_id                  ,
payment_type                   ,
currency_type                  ,
order_status                   ,
order_crt_day                  ,
order_cancel_day               ,
client_ip_region_id            , 
payment_msisdn_region_id       , 
order_msisdn_region_id         , 
plat_id                        ,
channel_id                     ,
term_prod_id                   , 
term_prod_version_id           ,
trig_order_program_id          ,
trig_order_series_program_id   ,
payment_msisdn                 ,
sp_id                          ,
operation_code                 ,
goods_id                       ,
goods_name                     , 
goods_type                     ,
service_code                   ,
service_name                   , 
order_crt_time                 ,
order_accept_time              ,
order_payment_time             ,
order_cancel_time              ,
client_ip                      ,
app_name                       ,
order_user_id                  ,
order_user_num                 ,
order_opr_user_id              ,
order_opr_user_type            ,
cancel_opr_user_id             ,
cancel_opr_user_type           ,
external_order_id              ,
payment_id                     ,
order_source_id                ,
order_source_type              ,
merchant_account               ,
portal_type                    ,
order_item_category            ,
virtual_operation_flag         ,
order_program_id               ,
order_series_program_id        ,
product_id                  ,
product_pkg_id                 ,
sub_business_id                ,
order_item_delivery_handler    ,
order_item_delivery_status     ,
order_item_auth_type           ,
order_item_auth_unit           ,
order_item_auth_qty            ,
order_item_delivery_qty        ,
order_item_unit_price          ,
order_item_resource_id         ,
order_item_resource_type       ,
order_item_order_begin_time    ,
order_item_order_end_time      ,
payment_amt                    
FROM in_valid_contract  ;

===================================================================================================================================

--------------------------
-- 非每月最后一天逻辑
-- 参数：${DEST_TBL_SCHEMA}：目标表的SCHEMA
--       ${SRC_FILE_DAY}   ：执行周期
---------------------------
set mapreduce.job.name=${DEST_TBL_SCHEMA}_fact_order_daily_snapshot_${SRC_FILE_DAY};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
--set hive.optimize.skewjoin=true;
set hive.merge.mapredfiles=true;
--set mapreduce.reduce.shuffle.input.buffer.percent=0.3;
--set mapreduce.reduce.shuffle.parallelcopies=3;

with two_days_succ_order as (  --两天的数据， 有可能合约先产生，订单后产生，不在一个数据周期，防止合约关联订单时，订单无法找到
select 
tt.*
from (
select foid.*,
row_number() over(partition by foid.order_id,foid.order_status,foid.payment_type order by src_file_day desc) row_num
from rptdata.fact_order_item_detail foid
where src_file_day >= from_unixtime(unix_timestamp('${SRC_FILE_DAY}','yyyyMMdd')-60*60*24*2,'yyyyMMdd')
  and src_file_day < '${SRC_FILE_DAY}'
  and order_status in ('5','9')
) tt
where row_num = 1 
),

new_tp_period_order as ( --新增包周期的订单
select 
foid.order_id                       ,
foid.order_last_upt_time            ,
foid.order_item_id                  ,
foid.payment_type                   ,
foid.currency_type                  ,
foid.order_status                   ,
foid.order_crt_day                  ,
foid.order_cancel_day               ,
foid.client_ip_region_id            , 
foid.payment_msisdn_region_id       , 
foid.order_msisdn_region_id         , 
foid.plat_id                        ,
foid.channel_id                     ,
foid.term_prod_id                   , 
foid.term_prod_version_id           ,
foid.trig_order_program_id          ,
foid.trig_order_series_program_id   ,
foid.payment_msisdn                 ,
foid.sp_id                          ,
foid.operation_code                 ,
foid.goods_id                       ,
foid.goods_name                     , 
foid.goods_type                     ,
foid.service_code                   ,
foid.service_name                   , 
foid.order_crt_time                 ,
foid.order_accept_time              ,
foid.order_payment_time             ,
foid.order_cancel_time              ,
foid.client_ip                      ,
foid.app_name                       ,
foid.order_user_id                  ,
foid.order_user_num                 ,
foid.order_opr_user_id              ,
foid.order_opr_user_type            ,
foid.cancel_opr_user_id             ,
foid.cancel_opr_user_type           ,
foid.external_order_id              ,
foid.payment_id                     ,
foid.order_source_id                ,
foid.order_source_type              ,
foid.merchant_account               ,
foid.portal_type                    ,
foid.order_item_category            ,
foid.virtual_operation_flag         ,
foid.order_program_id               ,
foid.order_series_program_id        ,
foid.product_id                     ,
foid.product_pkg_id                 ,
foid.sub_business_id                ,
foid.order_item_delivery_handler    ,
foid.order_item_delivery_status     ,
foid.order_item_auth_type           ,
foid.order_item_auth_unit           ,
foid.order_item_auth_qty            ,
foid.order_item_delivery_qty        ,
foid.order_item_unit_price          ,
foid.order_item_resource_id         ,
foid.order_item_resource_type       ,
foid.order_item_order_begin_time    ,
foid.order_item_order_end_time      ,
foid.payment_amt                    
from rptdata.fact_order_item_detail foid
where src_file_day = '${SRC_FILE_DAY}'
  and order_status in ('5','9')
  and order_item_auth_type = 'PERIOD'
  and order_item_auth_unit in ('MONTH','DAY')
  and payment_type not in ('310','311') --非续订， 如果是续订代表什么？  310 支付宝续租 311 微信续租
  and order_item_order_end_time > '${SRC_FILE_DAY}'
),

new_contract as (  --新增合约
select 
ch.order_id                         ,
foid.order_last_upt_time            ,
foid.order_item_id                  ,
foid.payment_type                   ,
foid.currency_type                  ,
foid.order_status                   ,
foid.order_crt_day                  ,
foid.order_cancel_day               ,
foid.client_ip_region_id            , 
foid.payment_msisdn_region_id       , 
foid.order_msisdn_region_id         , 
foid.plat_id                        ,
foid.channel_id                     ,
foid.term_prod_id                   , 
foid.term_prod_version_id           ,
foid.trig_order_program_id          ,
foid.trig_order_series_program_id   ,
foid.payment_msisdn                 ,
foid.sp_id                          ,
foid.operation_code                 ,
foid.goods_id                       ,
foid.goods_name                     , 
foid.goods_type                     ,
foid.service_code                   ,
foid.service_name                   , 
foid.order_crt_time                 ,
foid.order_accept_time              ,
foid.order_payment_time             ,
foid.order_cancel_time              ,
foid.client_ip                      ,
foid.app_name                       ,
foid.order_user_id                  ,
foid.order_user_num                 ,
foid.order_opr_user_id              ,
foid.order_opr_user_type            ,
foid.cancel_opr_user_id             ,
foid.cancel_opr_user_type           ,
foid.external_order_id              ,
foid.payment_id                     ,
foid.order_source_id                ,
foid.order_source_type              ,
foid.merchant_account               ,
foid.portal_type                    ,
foid.order_item_category            ,
foid.virtual_operation_flag         ,
foid.order_program_id               ,
foid.order_series_program_id        ,
foid.product_id                     ,
foid.product_pkg_id                 ,
foid.sub_business_id                ,
foid.order_item_delivery_handler    ,
foid.order_item_delivery_status     ,
foid.order_item_auth_type           ,
foid.order_item_auth_unit           ,
foid.order_item_auth_qty            ,
foid.order_item_delivery_qty        ,
foid.order_item_unit_price          ,
foid.order_item_resource_id         ,
foid.order_item_resource_type       ,
foid.order_item_order_begin_time    ,
foid.order_item_order_end_time      ,
foid.payment_amt                    
from intdata.contract_hist  ch  --合约日增量表
join two_days_succ_order foid on ch.order_id = foid.order_id  --用来验证
where ch.src_file_day = '${SRC_FILE_DAY}'
  and ch.contract_status = 'NORMAL'
),

cancel_contract as (	--取消的合约
select contract_id,
       order_id
  from intdata.contract_hist  ch   --合约日增量表
 where ch.src_file_day = '${SRC_FILE_DAY}'
   and ch.contract_status = 'CANCELLED'
),


in_valid_order as ( --有效订单

select fods.*
  from ${DEST_TBL_SCHEMA}.fact_order_daily_snapshot fods
 where snapshot_day = from_unixtime(unix_timestamp('${SRC_FILE_DAY}','yyyyMMdd')-60*60*24,'yyyyMMdd')
   and ( order_item_auth_type = 'BOSS_MONTH' OR  payment_type in ('310','311' ))
   and order_item_order_end_time > '${SRC_FILE_DAY}' 
  
 UNION ALL
 
select fods.*      --剔除取消的合约
  from ${DEST_TBL_SCHEMA}.fact_order_daily_snapshot fods
  left join cancel_contract cc on fods.order_id = cc.order_id
 where snapshot_day = from_unixtime(unix_timestamp('${SRC_FILE_DAY}','yyyyMMdd')-60*60*24,'yyyyMMdd')
   and cc.order_id is null
)

insert overwrite table ${DEST_TBL_SCHEMA}.fact_order_daily_snapshot partition(snapshot_day='${SRC_FILE_DAY}')
SELECT 
order_id                       ,
order_last_upt_time            ,
order_item_id                  ,
payment_type                   ,
currency_type                  ,
order_status                   ,
order_crt_day                  ,
order_cancel_day               ,
client_ip_region_id            , 
payment_msisdn_region_id       , 
order_msisdn_region_id         , 
plat_id                        ,
channel_id                     ,
term_prod_id                   , 
term_prod_version_id           ,
trig_order_program_id          ,
trig_order_series_program_id   ,
payment_msisdn                 ,
sp_id                          ,
operation_code                 ,
goods_id                       ,
goods_name                     , 
goods_type                     ,
service_code                   ,
service_name                   , 
order_crt_time                 ,
order_accept_time              ,
order_payment_time             ,
order_cancel_time              ,
client_ip                      ,
app_name                       ,
order_user_id                  ,
order_user_num                 ,
order_opr_user_id              ,
order_opr_user_type            ,
cancel_opr_user_id             ,
cancel_opr_user_type           ,
external_order_id              ,
payment_id                     ,
order_source_id                ,
order_source_type              ,
merchant_account               ,
portal_type                    ,
order_item_category            ,
virtual_operation_flag         ,
order_program_id               ,
order_series_program_id        ,
product_id                     ,
product_pkg_id                 ,
sub_business_id                ,
order_item_delivery_handler    ,
order_item_delivery_status     ,
order_item_auth_type           ,
order_item_auth_unit           ,
order_item_auth_qty            ,
order_item_delivery_qty        ,
order_item_unit_price          ,
order_item_resource_id         ,
order_item_resource_type       ,
order_item_order_begin_time    ,
order_item_order_end_time      ,
payment_amt                    
FROM new_tp_period_order   --新增包周期订单

UNION  ALL 

SELECT 
order_id                       ,
order_last_upt_time            ,
order_item_id                  ,
payment_type                   ,
currency_type                  ,
order_status                   ,
order_crt_day                  ,
order_cancel_day               ,
client_ip_region_id            , 
payment_msisdn_region_id       , 
order_msisdn_region_id         , 
plat_id                        ,
channel_id                     ,
term_prod_id                   , 
term_prod_version_id           ,
trig_order_program_id          ,
trig_order_series_program_id   ,
payment_msisdn                 ,
sp_id                          ,
operation_code                 ,
goods_id                       ,
goods_name                     , 
goods_type                     ,
service_code                   ,
service_name                   , 
order_crt_time                 ,
order_accept_time              ,
order_payment_time             ,
order_cancel_time              ,
client_ip                      ,
app_name                       ,
order_user_id                  ,
order_user_num                 ,
order_opr_user_id              ,
order_opr_user_type            ,
cancel_opr_user_id             ,
cancel_opr_user_type           ,
external_order_id              ,
payment_id                     ,
order_source_id                ,
order_source_type              ,
merchant_account               ,
portal_type                    ,
order_item_category            ,
virtual_operation_flag         ,
order_program_id               ,
order_series_program_id        ,
product_id                     ,
product_pkg_id                 ,
sub_business_id                ,
order_item_delivery_handler    ,
order_item_delivery_status     ,
order_item_auth_type           ,
order_item_auth_unit           ,
order_item_auth_qty            ,
order_item_delivery_qty        ,
order_item_unit_price          ,
order_item_resource_id         ,
order_item_resource_type       ,
order_item_order_begin_time    ,
order_item_order_end_time      ,
payment_amt                    
FROM new_contract  --新增合约

UNION ALL 

SELECT 
order_id                       ,
order_last_upt_time            ,
order_item_id                  ,
payment_type                   ,
currency_type                  ,
order_status                   ,
order_crt_day                  ,
order_cancel_day               ,
client_ip_region_id            , 
payment_msisdn_region_id       , 
order_msisdn_region_id         , 
plat_id                        ,
channel_id                     ,
term_prod_id                   , 
term_prod_version_id           ,
trig_order_program_id          ,
trig_order_series_program_id   ,
payment_msisdn                 ,
sp_id                          ,
operation_code                 ,
goods_id                       ,
goods_name                     , 
goods_type                     ,
service_code                   ,
service_name                   , 
order_crt_time                 ,
order_accept_time              ,
order_payment_time             ,
order_cancel_time              ,
client_ip                      ,
app_name                       ,
order_user_id                  ,
order_user_num                 ,
order_opr_user_id              ,
order_opr_user_type            ,
cancel_opr_user_id             ,
cancel_opr_user_type           ,
external_order_id              ,
payment_id                     ,
order_source_id                ,
order_source_type              ,
merchant_account               ,
portal_type                    ,
order_item_category            ,
virtual_operation_flag         ,
order_program_id               ,
order_series_program_id        ,
product_id                     ,
product_pkg_id                 ,
sub_business_id                ,
order_item_delivery_handler    ,
order_item_delivery_status     ,
order_item_auth_type           ,
order_item_auth_unit           ,
order_item_auth_qty            ,
order_item_delivery_qty        ,
order_item_unit_price          ,
order_item_resource_id         ,
order_item_resource_type       ,
order_item_order_begin_time    ,
order_item_order_end_time      ,
payment_amt                    
FROM in_valid_order  ;  --




DEST_TBL_SCHEMA  ----
SRC_FILE_DAY     ----
SRC_FILE_MONTH   ----
SRC_LAST_DAY     ----