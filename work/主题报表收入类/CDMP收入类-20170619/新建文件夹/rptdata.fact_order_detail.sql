


--set hive.auto.convert.join=false;
set mapreduce.job.name=rptdata.fact_order_detail_${SRC_FILE_DAY}_${SRC_FILE_HOUR};

set hive.optimize.sort.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.merge.mapredfiles = true;
set hive.optimize.skewjoin=true;
set hive.groupby.skewindata=true;
set hive.exec.parallel=true;

with order_raw as 
(
SELECT tm.*,  
       split(case when client_ip is null or client_ip = '' then '\t\t\t' else yanfa.migu_getipattr(client_ip) end, '\t') ip_attrs
  FROM intdata.order_hist tm
 where tm.src_file_day = '${SRC_FILE_DAY}' and tm.src_file_hour = '${SRC_FILE_HOUR}'
),
payment_dup AS
(SELECT payment_id, max_src_file_row_id    --一周内的重复记录
 FROM
  (SELECT payment_id,
          MAX(src_file_row_id) as max_src_file_row_id,
          COUNT(*) cnt
     FROM intdata.payment_hist
    WHERE src_file_day between from_unixtime(unix_timestamp('${SRC_FILE_DAY}', 'yyyyMMdd') - 6*24*3600, 'yyyyMMdd') and '${SRC_FILE_DAY}' --6天
    GROUP BY payment_id
  ) x
WHERE cnt > 1
),
payment_lkp as 
(
SELECT tm.payment_id, sp_id, operation_code 
  FROM intdata.payment_hist tm 
  left join payment_dup t1 on (tm.payment_id = t1.payment_id)
 WHERE tm.src_file_day between from_unixtime(unix_timestamp('${SRC_FILE_DAY}', 'yyyyMMdd') - 6*24*3600, 'yyyyMMdd') and '${SRC_FILE_DAY}'
   and (t1.payment_id IS NULL OR tm.src_file_row_id = t1.max_src_file_row_id)
)
INSERT overwrite TABLE rptdata.fact_order_detail PARTITION (src_file_day, src_file_hour)
select
      order_id,
      order_last_upt_time,
      order_status,
      from_unixtime(unix_timestamp(substr(order_crt_time, 1, 8), 'yyyyMMdd'), 'yyyyMMdd') as order_crt_day,
      from_unixtime(unix_timestamp(substr(order_cancel_time, 1, 8), 'yyyyMMdd'), 'yyyyMMdd') as order_cancel_day,
      nvl(d1.region_id, '-998') as client_ip_region_id,
      nvl(d2.region_id, '-998') as payment_msisdn_region_id,
      nvl(d4.region_id, '-998') as order_msisdn_region_id,
      plat_id,
      channel_id,
      d5.term_prod_id as term_prod_id,
      term_prod_version_id,
      case when order_source_type = 'POMS_PROGRAM_ID' then  order_source_id else '-998' end trig_order_program_id,
      case when order_source_type = 'POMS_ALBUM_ID' then  order_source_id else '-998' end trig_order_series_program_id,
      payment_msisdn,
      d3.usernum_02 as order_phone_num,
      t1.sp_id,
      t1.operation_code,
      tm.goods_id,
      tm.goods_type,
      d6.goods_name,
      tm.service_code,
      d7.service_name,
      order_crt_time,
      order_accept_time,
      order_payment_time,
      order_cancel_time,
      client_ip,
      app_name,
      order_user_id,
      order_user_num,
      order_opr_user_id,
      order_opr_user_type,
      cancel_opr_user_id,
      cancel_opr_user_type,
      external_order_id,
      tm.payment_id,
      order_source_id,
      order_source_type,
      merchant_account,
      portal_type,
      case when t1.sp_id = '699999' then 'Y' else 'N' end virtual_operation_flag,
      order_qty,
      raw_amt,
      actual_amt,
      src_file_day,
      src_file_hour
 from order_raw tm 
 left join payment_lkp t1   
   on (tm.payment_id = t1.payment_id)
 
 left join rptdata.dim_region d1 on (tm.ip_attrs[1] = d1.prov_name and tm.ip_attrs[2] = d1.region_name)
 left join rptdata.dim_phone_belong d2 on (substr(tm.payment_msisdn, 3, 7) = d2.phone_range)
 left join rptdata.dim_userid_info d3 on (tm.order_user_id = d3.user_id)
 left join rptdata.dim_phone_belong d4 on (substr(d3.usernum_02, 1, 7) = d4.phone_range)     
 left join rptdata.dim_term_prod d5 on (substr(tm.term_prod_version_id, 1, 6) = d5.term_version_id)
 
 left join intdata.goods d6 on (tm.goods_id = d6.goods_id and tm.goods_type = d6.goods_type)
 left join intdata.service d7 on (tm.service_code = d7.service_code);
	  
--因为可能先有支付后有定单
--支付成功后或失败后，订单状态只设置为最终状态（成功、失败）
--我们只拿倒最终状态的订单记录
--这样通过订单去找支付的时候，可能不在同一个数据周期内
--取一周这个目前是我自己定的，一般情况，一天应该足够了	  
	  
====================================================================================================

set hive.exec.parallel=true; 
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

with tm as(
select
     orderID as order_id,
     orderStatus as order_status,
     createTime as order_crt_time,
     acceptancetime as order_accept_time,
     subscribeTime as order_payment_time,
     unsubscribeTime as order_cancel_time,
     lastUpdateTime as order_last_upt_time,
     IP as client_ip,
     appName as app_name,
     channelID as channel_id_source,
     cv.plat_id,
     cv.term_product_version_id,
     cv.channel_id,
     orderUserID as order_user_id,
     userNum as order_user_num,
     operatorUserID as order_opr_user_id,
     case when operatorUserID = orderUserID then '自主订购' else operatorUserID end order_opr_user_type,
     msisdn as payment_msisdn,
     cancelOperatorUserID as cancel_opr_user_id,
     case when cancelOperatorUserID = orderUserID then '自主退订' else cancelOperatorUserID end  cancel_opr_user_type,
     externalOrderId as external_order_id,
     goodsID as goods_id,
     goodsType as goods_type,
     paymentID as payment_id,
     SPID as sp_id,
     OperaCode as operation_code,
     resourceID as order_source_id,
     resourceType as order_source_type,
     merchantAccount as merchant_account,
     serviceCode as service_code,
     portalType as portal_type,
     cast(count as bigint) as order_qty,
     cast(rawAmount as bigint) as raw_amt,
     cast(totalAmount as bigint) as actual_amt,
     src_file_row_id
from ods.ugc_50201_order_v lateral view yanfa.migu_channelvalue(channelID) cv as plat_id, term_product_version_id, channel_id
where src_file_day='${date}' and src_file_hour='${hour}'
),
dup AS
  (SELECT order_id, order_status, max_src_file_row_id
     FROM
         (SELECT order_id,
                 order_status,
                 MAX(src_file_row_id) as max_src_file_row_id,
                 COUNT(*) cnt
            FROM tm
           GROUP BY order_id, order_status
         ) x
   WHERE cnt > 1		--找出重复记录,即订单
  )
INSERT overwrite TABLE intdata.order_hist PARTITION (src_file_day='${date}' , src_file_hour='${hour}')
SELECT tm.* FROM tm 
  left join dup t1 
    on (tm.order_id = t1.order_id and tm.order_status = t1.order_status)
 WHERE (t1.order_id IS NULL OR tm.src_file_row_id = t1.max_src_file_row_id); --剔除重复记录

====================================================================================================

set hive.exec.parallel=true; 
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

INSERT overwrite TABLE intdata.payment_hist PARTITION (src_file_day='${date}', src_file_hour='${hour}')
SELECT paymentID        as payment_id,
       lastUpdateTime   as payment_last_upt_time,
       createTime       as crt_time,
       acceptancetime   as accept_time,
       preapprovalTime  as pre_payment_time,
       finishTime       as finish_time,
       IP               as client_ip,
       appName          as app_name,
       orderID          as order_id,
       channelID        as channel_id_source,
       cv.plat_id       as plat_id,
       cv.channel_id    as channel_id,
       cv.term_product_version_id as term_product_version_id,
       userID           as user_account,
       mobile           as phone_num,
       merchantAccount  as merchant_account,
       bankCode         as bank_code,
       cpCode           as sp_id,
       operCode         as operation_code,
       productOrderId   as product_order_id,
       thirdToken       as third_token,
       Version          as version,
       chargeMode       as charge_type,
       payType          as payment_type,
       result           as payment_status,
       reason           as payment_reason,
       portalType       as portal_type,
       otherType        as other_type,
       resultSource     as result_source,
       requestFrom      as request_source,
       cast(requestTime as bigint) as request_unix_time,
       src_file_row_id
 from ods.ugc_50101_payment_v lateral view yanfa.migu_channelvalue(channelID) cv as plat_id, term_product_version_id, channel_id
where src_file_day='${date}' and src_file_hour='${hour}'

====================================================================================================

CREATE VIEW `ods.ugc_50201_order_v` AS SELECT `p`.`orderid`, 
       `p`.`orderstatus`,                          
       `p`.`createtime`,                           
       `e`.`acceptancetime`,                       
       `e`.`subscribetime`,                        
       `e`.`unsubscribetime`,                      
       `p`.`lastupdatetime`,                       
       `p`.`ip`,                                   
       `p`.`appname`,                              
       `p`.`channelid`,
       `p`.`orderuserid`,                          
       `e`.`usernum`,                              
       `e`.`operatoruserid`,                       
       `e`.`msisdn`,                               
       `e`.`canceloperatoruserid`,                 
       `p`.`externalorderid`,                      
       `p`.`goodsid`,                              
       `p`.`goodstype`,                            
       `p`.`paymentid`,                            
       `e`.`spid`,                                 
       `e`.`operacode`,                            
       `e`.`resourceid`,                           
       `e`.`resourcetype`,                         
       `p`.`merchantaccount`,                      
       `p`.`servicecode`,                          
       `e`.`portaltype`,                           
       `p`.`count`,                                
       `p`.`rawamount`,                            
       `p`.`totalamount`,
       `cv`.`plat_id`,
       `cv`.`term_product_version_id`, 
       `cv`.`channel_id`,
       `p`.`orderitems`,
       `p`.`deliveryresults`,
       `c`.`paychannels`, 
       concat(`s`.`input__file__name`, ':', LPAD(`s`.`block__offset__inside__file`, 10, '0')) as `src_file_row_id`,       
       `s`.`src_file_day`,                         
       `s`.`src_file_hour`
FROM `ods`.`ugc_50201_order_json_ex` `s`           
LATERAL VIEW json_tuple(`s`.`json`,        --cancelOrderID:没有被解析        
                       'orderID',                  
                       'orderStatus',              
                       'createTime',               
                       'lastUpdateTime',           
                       'IP',                       
                       'appName',                  
                       'channelID',                
                       'orderUserID',              
                       'externalOrderId',          
                       'goodsID',                  
                       'goodsType',                
                       'paymentID',                
                       'merchantAccount',          
                       'serviceCode',              
                       'count',                    
                       'rawAmount',                
                       'totalAmount',              
                       'extInfo',
                       'orderItems',
                       'deliveryResults',
                       'payType'
                       ) `p` as `orderID`, `orderStatus`, `createTime`, `lastUpdateTime`, `IP`, `appName`, `channelID`, `orderUserID`, `externalOrderId`, 
                              `goodsID`, `goodsType`, `paymentID`, `merchantAccount`, `serviceCode`, `count`, `rawAmount`, `totalAmount`, `extInfo_json`, `orderItems`, `deliveryResults`, `payType_json`
LATERAL VIEW json_tuple(`p`.`extinfo_json`, 'acceptancetime',  
                                        'subscribeTime',  
                                        'unsubscribeTime',  
                                        'userNum', 
                                        'operatorUserID', 
                                        'msisdn',  
                                        'cancelOperatorUserID', 
                                        'SPID',    
                                        'resourceID', 
                                        'resourceType', 
                                        'OperaCode', 
                                        'portalType' 
                                        ) `e` as `acceptancetime`, `subscribeTime`, `unsubscribeTime`, `userNum`, `operatorUserID`, `msisdn`, `cancelOperatorUserID`, `SPID`, `resourceID`, `resourceType`, `OperaCode`, `portalType`
LATERAL VIEW json_tuple(`p`.`paytype_json`, 'payChannels') `c` as `payChannels`
lateral view `yanfa.migu_channelvalue`(`p`.`channelid`) `cv` as `plat_id`, `term_product_version_id`, `channel_id`                                        
WHERE `p`.`orderstatus` in ('5', '9', '12', '14', '15', '16')

====================================================================================================

CREATE VIEW `ods.ugc_50101_payment_v` AS SELECT --concat(s.INPUT__FILE__NAME, ':', s.BLOCK__OFFSET__INSIDE__FILE) as rowkey,
       `p`.`paymentid`,
       `p`.`lastupdatetime`,
       `p`.`createtime`,
       `e`.`acceptancetime`,
       `p`.`preapprovaltime`,
       `p`.`finishtime`,
       `p`.`ip`,
       `p`.`appname`,
       `e`.`orderid`,
       `p`.`channelid`,
       `p`.`userid`,
       `p`.`mobile`,
       `p`.`merchantaccount`,
       `e`.`bankcode`,
       `e1`.`cpcode`,
       `e1`.`opercode`,
       `p`.`productorderid`,
       `p`.`thirdtoken`,
       `e`.`version`,
       `e1`.`chargemode`,
       `e`.`paytype`,
       `p`.`result`,
       `e`.`reason`,
       `e`.`portaltype`,
       `e`.`othertype`,
       `e`.`resultsource`,
       `e1`.`requestfrom`,
       `e1`.`requesttime`,
       `s`.`src_file_day`,
       `s`.`src_file_hour`,
       concat(`s`.`input__file__name`, ':', `s`.`block__offset__inside__file`) as `src_file_row_id`
FROM `ods`.`ugc_50101_payment_json_ex` `s`
LATERAL VIEW json_tuple(`s`.`json`, 		--payChannelList 没有被解析
                       'paymentID', 
                       'lastUpdateTime', 
                       'createTime', 
                       'preapprovalTime',
                       'finishTime',
                       'IP',
                       'appName',
                       'channelID',
                       'userID',
                       'mobile',
                       'merchantAccount',
                       'serviceID',
                       'productOrderId',
                       'thirdToken',
                       'result',
                       'extensioninfo'                 
                       ) `p` as `paymentID`, `lastUpdateTime`, `createTime`, `preapprovalTime`, `finishTime`, `IP`, `appName`, `channelID`,
                              `userID`, `mobile`, `merchantAccount`, `serviceID`, `productOrderId`, `thirdToken`, `result`, `extensioninfo_json`
LATERAL VIEW json_tuple(`p`.`extensioninfo_json`, 'Version', 
                                        'acceptancetime', 
                                        'bankCode', 
                                        'orderID',
                                        'otherType',
                                        'payType',
                                        'portalType',
                                        'reason',
                                        'resultSource',
                                        'extendAttr'
                                        ) `e` as `Version`, `acceptancetime`, `bankCode`, `orderID`, `otherType`, `payType`, `portalType`, `reason`, `resultSource`, `extendAttr_json`
LATERAL VIEW json_tuple(cast(unbase64(`e`.`extendattr_json`) as string), 'requestFrom', 
                                                    'requestTime', 
                                                    'operCode', 
                                                    'cpCode', 
                                                    'chargeMode') `e1` as `requestFrom`, `requestTime`, `operCode`, `cpCode`, `chargeMode`
													
													