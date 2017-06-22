

set hive.exec.parallel=true; 
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

WITH dup AS
  (SELECT order_id, order_status, max_src_file_row_id
  FROM
    (SELECT orderID as order_id,
            orderStatus order_status,
            MAX(src_file_row_id) as max_src_file_row_id,
            COUNT(*) cnt
    FROM odsdata.ugc_50201_order
    WHERE src_file_day='${date}' and src_file_hour='${hour}'
    GROUP BY orderID, orderStatus 
    ) x
  WHERE cnt > 1
  )
INSERT overwrite TABLE intdata.order_item_hist PARTITION (src_file_day='${date}', src_file_hour='${hour}')
select
     orderID as order_id,
     lastUpdateTime as order_last_upt_time,
     itemId as order_item_id,
     orderStatus as order_status,
     handler as delivery_handler,
     main as main_delivery_flag,
     desc as order_item_desc,
     cast(quantity as bigint) as delivery_qty,
     cast(unitPrice as bigint) as unit_price,
     authType as auth_type,	--授权类型
     periodUnit as order_period_unit,
     cast(amount as bigint) as order_period_qty,
     opCode as operation_code,
     cpCode as sp_id,
     contractType as contract_type,
     msisdn as contract_msisdn,
     productId as contract_product_id,
     resource_id as migu_auth_resource_id,
     resource_type as migu_auth_resource_type,
     accountType as migu_account_account_type,
     cast(data_amount as bigint) as migu_account_charge_amt,
     externalOrderId as migu_account_external_order_id,
     src_file_row_id
from ods.ugc_50201_order_item_v tm left join dup t1 on (tm.orderID = t1.order_id and tm.orderStatus = t1.order_status)
where (t1.order_id IS NULL OR tm.src_file_row_id = t1.max_src_file_row_id)
and tm.src_file_day='${date}'
and tm.src_file_hour='${hour}';

====================================================================================================   

CREATE VIEW `ods.ugc_50201_order_item_v` AS SELECT `p`.`orderid`, 
       `p`.`orderstatus`,
       `p`.`lastupdatetime`,                       
       `c2`.`itemid`,            --交付物ID                  
       `c2`.`handler`,           --交付方向      migu_auth\migu_account\contact\...         
       `c2`.`main`,              --是否主交付物                  
       `c2`.`desc`,              --交付物描述                  
       `c2`.`quantity`,          --交付物数量                  
       `c2`.`unitprice`,         --交付物单价     
	   
	   --when handler = migu_auth\migu_account
       `c3`.`authtype`,          --授权类型，BOSS-MONTH\Period\copy\times                  
       `c3`.`periodunit`,        --订购单位类型month\month.day.hour.minute\month\null                 
       `c3`.`amount`,            --交付物数量，由业务系统传入（例如amount=6，authtype=PERIOD，periodUnit=MONTH表示授权6个月） 
	   

       nvl(`c4`.`opcode`, `c4`.`opercode`) as `opCode`, --局数据计费点OPERACODE
       nvl(`c4`.`cpcode`, `c4`.`cpid`) as `cpCode`,  --局数据计费点CPID
       `c4`.`contracttype`,     --合约类型  MOBILE_BOSS：BOSS手机号码包月合约                   
       `c4`.`msisdn`,           --合约归属手机号码                   
       `c4`.`productid`,                           
       `c4`.`resource_id`,                         
       `c4`.`resource_type`,                       
       `c4`.`accounttype`,                         
       `c4`.`amount` as `data_amount`,             
       `c4`.`externalorderid`,   
	   
       `p`.`src_file_day`,                         
       `p`.`src_file_hour`,                        
       `p`.`src_file_row_id` 
FROM `odsdata`.`ugc_50201_order` `p`           
LATERAL VIEW posexplode(split(regexp_replace(regexp_replace(`p`.`orderitems`,'\\}\\,\\{','\\}\u0031\\{'),'\\[|\\]',''), '\u0031')) `c1` as `orderItem_seq`, `orderItem_json` 
LATERAL VIEW json_tuple(`c1`.`orderitem_json`, 'authorization',  
                                           'data',  
                                           'desc',  
                                           'handler',  
                                           'itemId',  
                                           'main',  
                                           'quantity',  
                                           'unitPrice') `c2` as `authorization_json`, `data_json`, `desc`, `handler`, `itemId`, `main`, `quantity`, `unitPrice` 
LATERAL VIEW json_tuple(`c2`.`authorization_json`, 'amount', 'authType', 'periodUnit') `c3` as `amount`, `authType`, `periodUnit` 
LATERAL VIEW json_tuple(`c2`.`data_json`, 'cpCode',  
                                      'cpID',      
                                      'opCode',    
                                      'operCode',  
                                      'contractType',  
                                      'msisdn',    
                                      'productId', 
                                      'resource_id', 
                                      'resource_type', 
                                      'accountType', 
                                      'amount',    
                                      'externalOrderId' 
                                      ) `c4` as `cpCode`, `cpID`, `opCode`, `operCode`, `contractType`, `msisdn`, `productId`, `resource_id`, `resource_type`, `accountType`, `amount`, `externalOrderId` 
WHERE `p`.`orderstatus` in ('5', '9', '12', '14', '15', '16')