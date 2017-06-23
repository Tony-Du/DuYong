set hive.exec.parallel=true; 
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

WITH dup AS
  (SELECT order_id, order_status, max_src_file_row_id
  FROM
    (SELECT orderID as order_id,
            orderStatus as order_status,
            MAX(src_file_row_id) as max_src_file_row_id,
            COUNT(*) cnt
    FROM odsdata.ugc_50201_order
    WHERE src_file_day='${date}' and src_file_hour='${hour}'
    GROUP BY orderID, orderStatus
    ) x
  WHERE cnt > 1
  )
INSERT overwrite TABLE intdata.order_item_delivery_hist PARTITION (src_file_day='${date}', src_file_hour='${hour}')
select
       orderID as order_id,
       lastUpdateTime as order_last_upt_time,
       itemId as order_item_id,
       orderStatus as order_status,
       deliveryId as delivery_id,
       deliveryStatus as delivery_status,
       cast(createTime as bigint) as contract_crt_unix_time,
       contractId as contract_id,
       contract_deliveryStatus as contract_delivery_status,
       regexp_replace(starttime, '[^0-9]', '') migu_auth_order_begin_time,
       case when endtime = 'UNLIMITED' then '20991231235959' else regexp_replace(endtime, '[^0-9]', '') end,
       src_file_row_id
  from ods.ugc_50201_order_item_delivery_v tm left join dup t1 on (tm.orderID = t1.order_id and tm.orderStatus = t1.order_status)
 where (t1.order_id IS NULL OR tm.src_file_row_id = t1.max_src_file_row_id)
   and tm.src_file_day='${date}'
   and tm.src_file_hour='${hour}';
   
====================================================================================================   
   
CREATE VIEW `ods.ugc_50201_order_item_delivery_v` AS SELECT `p`.`orderid`, 
       `p`.`orderstatus`,
       `p`.`lastupdatetime`,                       
       `c2`.`itemid`,                              
       `c2`.`deliveryid`,                          
       `c2`.`deliverystatus`,                      
       `c3`.`createtime`,                          
       `c3`.`deliverystatus` as `contract_deliveryStatus`,  
       `c3`.`contractid`,                          
       `c3`.`starttime`,                           
       `c3`.`endtime`,                             
       `p`.`src_file_day`,                         
       `p`.`src_file_hour`,                        
       `p`.`src_file_row_id` 
FROM `odsdata`.`ugc_50201_order` `p`
LATERAL VIEW posexplode(split(regexp_replace(regexp_replace(`p`.`deliveryresults`,'\\}\\,\\{','\\}\u0031\\{'),'\\[|\\]',''), '\u0031')) `c1` as `deliveryResult_seq`, `deliveryResult_json` 
LATERAL VIEW json_tuple(`c1`.`deliveryresult_json`,  'deliveryId', 'deliveryStatus', 'itemId', 'resultData') `c2` as `deliveryId`, `deliveryStatus`, `itemId`, `resultData_json` 
LATERAL VIEW json_tuple(`c2`.`resultdata_json`, 'createTime',  
                                            'deliveryStatus',  
                                            'contractId',  
                                            'startTime',  
                                            'endTime') `c3` as `createTime`, `deliveryStatus`, `contractId`, `startTime`, `endTime`
WHERE `p`.`orderstatus` in ('5', '9', '12', '14', '15', '16')   

==================================================================================================== 

set hive.exec.parallel=true; 
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

INSERT overwrite TABLE odsdata.ugc_50201_order PARTITION (src_file_day='${date}' , src_file_hour='${hour}')
select 
     orderid,
     orderstatus,
     createtime,
     acceptancetime,
     subscribetime,
     unsubscribetime,
     lastupdatetime,
     ip,
     appname,
     channelid,
     orderuserid,
     usernum,
     operatoruserid,
     msisdn,
     canceloperatoruserid,
     externalorderid,
     goodsid,
     goodstype,
     paymentid,
     spid,
     operacode,
     resourceid,
     resourcetype,
     merchantaccount,
     servicecode,
     portaltype,
     count,
     rawamount,
     totalamount,
     plat_id,
     term_product_version_id,
     channel_id,
     orderitems,
     deliveryresults,
     paychannels,
     src_file_row_id 
from ods.ugc_50201_order_v
where src_file_day='${date}' and src_file_hour='${hour}';