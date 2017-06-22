




============================================================================================================================================

--合约是订购关系，只要不取消，就会一直存在。
--在合约产生的那一天，订单话单中是存在该订购信息，但只是在当天存在

set hive.exec.parallel=true; 
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

INSERT overwrite TABLE intdata.contract_monthly_snapshot PARTITION (src_file_month='${month}')
SELECT regexp_replace(id, '(ObjectId\\()|\\)', '') contract_id,  --合约id（主键）
       case when signTime in ('0', '-1') then null else signTime end as sign_unix_time, --签约时间
       case when createTime in ('0', '-1') then null else createTime end as crt_unix_time, --合约创建时间
       case when effectTime in ('0', '-1') then null else effectTime end as effect_unix_time,  --集团生效时间
       case when endTime in ('0', '-1') then null else endTime end as end_unix_time,  --合约结束时间
       case when lastSuspendTime in ('0', '-1') then null else lastSuspendTime end as last_suspend_unix_time, --上次暂停时间
       case when lastResumeTime in ('0', '-1') then null else lastResumeTime end as last_resume_unix_time,   --上次恢复时间
       orderId as order_id,   --订单ID
       msisdn as payment_msisdn,  --手机号
       userId as order_user_id,  --订单userID
       deliveryId as delivery_id,
       provinceId as province_id,   --省份代码     
       productId as product_id,   --产品ID
       cpId as sp_id,           --cpId
       operCode as operation_code,      --operCode
       type as contract_payment_type,    --合约类型  MOBILE_BOSS、ALI_PAY
       contractStatus as contract_status,   --合约状态
       operateStatus as contract_sub_status,   --业务状态 = 合约子状态
       suspendReason as suspend_reason  ---暂停原因
  from ods.ugc_20102_contract_ex where src_file_month='${month}';
  
==========================================================================================================================================
+---------------------------------------+----------------------------------------------------+--------------+---------------+---------+-------------+--------------+-----------+--------------+------------------+-------------------+------------------+-------------+--------------------+-------------------+----------------+----------------+----------------+---------------------------------------+-------------------+--+
|                 a.id                  |                     a.orderid                      |   a.msisdn   | a.provinceid  | a.cpid  | a.opercode  | a.productid  | a.userid  |    a.type    | a.operatestatus  | a.contractstatus  | a.suspendreason  | a.signtime  | a.lastsuspendtime  | a.lastresumetime  |  a.createtime  |   a.endtime    |  a.effecttime  |             a.deliveryid              | a.src_file_month  |
+---------------------------------------+----------------------------------------------------+--------------+---------------+---------+-------------+--------------+-----------+--------------+------------------+-------------------+------------------+-------------+--------------------+-------------------+----------------+----------------+----------------+---------------------------------------+-------------------+--+
| 9b74917d-ff72-495e-9e96-7eec3e43f466  | 215228011_fba29991-8db4-4aeb-a2e0-8f34ec6a7b11_ItemPurchaseOrder | 13816350192  | 210           | 698040  | 30830591    |              |           | MOBILE_BOSS  | NORMAL           | CANCELLED         |                  |             | 1482757245264      | 1482757646357     | 1482755396385  | 1487322544212  | 1487322544212  | 76f27a08-489f-48c1-997a-49c540590ca9  | 201705            |
| 9ff67a13-57af-4ca9-9847-51c328f18870  | 686742177_935f8e84-d4d7-4bcc-ab26-61c3bab2a93a_ItemPurchaseOrder | 18439944214  | 371           | 698040  | 30830591    |              |           | MOBILE_BOSS  | SUSPENDED        | CANCELLED         |                  |             | 1482867008573      | 0                 | 1482866995816  | 1482867210751  | 0              | 3101bd75-ba8f-4964-a80f-8c910b0e5349  | 201705            |
| 44c38822-d5bc-48ac-9ad7-6729acde001d  | 686742177_5be92e1c-ca26-4276-b57c-0be7e79285b7_ItemPurchaseOrder | 18439944214  | 371           | 698040  | 30830591    |              |           | MOBILE_BOSS  | NORMAL           | CANCELLED         |                  |             | 1482872396748      | 1482872996837     | 1482868196098  | 1482882026131  | 0              | 15567def-0449-40fa-bfb1-d9e86c14bc96  | 201705            |
| f671c23c-40d6-4e41-a1d8-32f75546e4cf  | 193048367_51cf9ce9-6f3d-477f-8d32-190ee1a64593_ItemPurchaseOrder | 15057176484  | 571           | 698040  | 30830591    |              |           | MOBILE_BOSS  | NORMAL           | CANCELLED         |                  |             | 0                  | 0                 | 1482869610891  | 1482869623707  | 0              | 81c65608-21be-49de-ae98-6a3f0b9b9c1c  | 201705            |
| cafa5141-7183-41d1-b1d5-bd434eab6435  | 193048367_a0ee5be3-6951-4e9c-b3cd-7d4271878966_ItemPurchaseOrder | 15057176484  | 571           | 698040  | 30830591    |              |           | MOBILE_BOSS  | NORMAL           | CANCELLED         |                  |             | 0                  | 0                 | 1482870210679  | 1482870224063  | 0              | 664946f7-a787-4fbb-8db8-3943545558f5  | 201705            |
| f2e05fdc-f32f-4091-829c-6c20c9c06333  | 193048367_1bf1375c-1b8f-480e-97e8-487829d19f4e_ItemPurchaseOrder | 15057176484  | 571           | 698040  | 30830591    |              |           | MOBILE_BOSS  | NORMAL           | CANCELLED         |                  |             | 0                  | 0                 | 1482870913860  | 1482871060846  | 0              | 3f84c220-9b80-42aa-97f9-87bfa96f7dd2  | 201705            |
| 868a6bfc-ae15-45e6-9866-4d404b6e2df8  | 193048367_86e2adde-f3c0-4c9f-941e-5a871f860461_ItemPurchaseOrder | 15057176484  | 571           | 698040  | 30830591    |              |           | MOBILE_BOSS  | NORMAL           | CANCELLED         |                  |             | 0                  | 0                 | 1482872010772  | 1482872273173  | 0              | e5828241-37cb-497e-9679-597048633804  | 201705            |
| b7e62f4f-d2a6-45ec-b7e4-4ba555d18df8  | 178934519_a231dbeb-f409-4101-bba0-cada2b1313bf_ItemPurchaseOrder | 13970007697  | 791           | 698040  | 30830591    |              |           | MOBILE_BOSS  | NORMAL           | CANCELLED         |                  |             | 0                  | 0                 | 1482872547394  | 1482872725144  | 0              | d4436311-e662-427d-aa21-14a7e358ccc0  | 201705            |
| ea4a25dc-6ff2-4d1a-8f11-5092bf01b5a0  | 484485583_97af3da3-f95c-44cf-833a-1a40e30a22ce_ItemPurchaseOrder | 13735512424  | 571           | 698040  | 30830591    |              |           | MOBILE_BOSS  | NORMAL           | CANCELLED         |                  |             | 0                  | 0                 | 1482872620055  | 1482873211007  | 0              | cc3a05ca-d3ca-450a-8b6a-e98b8a0d03c0  | 201705            |
| f0f883b6-b6f8-4adf-88d6-174dab08e38a  | 193048367_1c0103bf-2f24-4b58-aeb9-2b480d2d4b7d_ItemPurchaseOrder | 15057176484  | 571           | 698040  | 30830591    |              |           | MOBILE_BOSS  | NORMAL           | CANCELLED         |                  |             | 0                  | 0                 | 1482873211075  | 1482873473589  | 0              | 815725ec-6b9f-4844-8c4c-460382463792  | 201705            |
+---------------------------------------+----------------------------------------------------+--------------+---------------+---------+-------------+--------------+-----------+--------------+------------------+-------------------+------------------+-------------+--------------------+-------------------+----------------+----------------+----------------+---------------------------------------+-------------------+--+
  
  
  
set hive.exec.parallel=true; 
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

INSERT overwrite TABLE intdata.contract_hist PARTITION (src_file_day='${date}')
SELECT id as contract_id, 
       from_unixtime(unix_timestamp(last_update_time), 'yyyyMMddHHmmss') as contract_last_upt_time,
       case when signTime in ('0', '-1') then null else signTime end as sign_unix_time,
       case when createTime in ('0', '-1') then null else createTime end as crt_unix_time,
       case when effectTime in ('0', '-1') then null else effectTime end as effect_unix_time,
       case when endTime in ('0', '-1') then null else endTime end as end_unix_time,
       case when lastSuspendTime in ('0', '-1') then null else lastSuspendTime end as last_suspend_unix_time,
       case when lastResumeTime in ('0', '-1') then null else lastResumeTime end as last_resume_unix_time,
       orderId as order_id, 
       msisdn as payment_msisdn, 
       userId as order_user_id, 
       deliveryId as delivery_id,
       provinceId as province_id,        
       productId as product_id, 
       cpId as sp_id,
       operCode as operation_code,      
       type as contract_payment_type,      
       contractStatus as contract_status, 
       operateStatus as contract_sub_status,   
       suspendReason as suspend_reason
from ods.ugc_20101_contract_v
where src_file_day='${date}';  

============================================================================================================

CREATE VIEW `ods.ugc_20101_contract_v` AS SELECT --concat(s.INPUT__FILE__NAME, ':', s.BLOCK__OFFSET__INSIDE__FILE) as rowkey,
       `p`.`id`, 
       `s`.`last_update_time`,
       `p`.`orderid`, 
       `p`.`msisdn`, 
       `p`.`provinceid`, 
       `p`.`cpid`, 
       `p`.`opercode`, 
       `p`.`productid`, 
       `p`.`userid`, 
       `p`.`type`, 
       `p`.`operatestatus`, 
       `p`.`contractstatus`, 
       `p`.`suspendreason`, 
       `p`.`signtime`,
       `p`.`lastsuspendtime`,
       `p`.`lastresumetime`,
       `p`.`createtime`,
       `p`.`endtime`,
       `p`.`effecttime`,
       `p`.`deliveryid`,
       `s`.`src_file_day`
FROM `ods`.`ugc_20101_contract_json_v` `s`
LATERAL VIEW json_tuple(`s`.`json`, 'id',  
                                'orderId', 
                                'msisdn',
                                'provinceId',
                                'cpId',
                                'operCode',
                                'productId',
                                'userId',
                                'type',
                                'operateStatus',
                                'contractStatus',
                                'suspendReason',
                                'signTime',
                                'lastSuspendTime',
                                'lastResumeTime',
                                'createTime',
                                'endTime',
                                'effectTime',
                                'deliveryId'
                                ) `p` as `id`, 
                                       `orderId`, 
                                       `msisdn`, 
                                       `provinceId`, 
                                       `cpId`, 
                                       `operCode`, 
                                       `productId`, 
                                       `userId`, 
                                       `type`, 
                                       `operateStatus`, 
                                       `contractStatus`, 
                                       `suspendReason`, 
                                       `signTime`,
                                       `lastSuspendTime`,
                                       `lastResumeTime`,
                                       `createTime`,
                                       `endTime`,
                                       `effectTime`,
                                       `deliveryId`
    
============================================================================================================
   
CREATE VIEW `ods.ugc_20101_contract_json_v` AS 
select substr(`ugc_20101_contract_ex`.`row`, 1, 19) `last_update_time`,
       case when instr(`ugc_20101_contract_ex`.`row`, '{') is null or instr(`ugc_20101_contract_ex`.`row`, '{') = 0 then ''
            else substr(`ugc_20101_contract_ex`.`row`, instr(`ugc_20101_contract_ex`.`row`, '{')) end `json`,
       `ugc_20101_contract_ex`.`src_file_day`
from `ods`.`ugc_20101_contract_ex`  --合约表（日增量）



2017-06-20 00:00:07.919 [http-nio-18777-exec-15] BIGDATA cn.cmvideo.ugc.contract.dao.imp.ContractDaoImp - {"contractStatus":"NORMAL","cpId":"699201","createTime":1439129124000,"deliveryId":"252678560_8615086818502_699201_1520230047_20150809220524_CUTOVER_CONTRACTDELIVERY","effectTime":1497888007918,"endTime":-1,"id":"8615086818502_699201_1520230047_20150809220524_cutover_contractID","lastResumeTime":1497888007918,"lastSuspendTime":1497887500293,"lastUpdateTime":1497888007918,"msisdn":"8615086818502","operCode":"1520230047","operateStatus":"NORMAL","orderId":"252678560_2028595051-cutover_ItemPurchaseOrder","provinceId":"230","signTime":0,"type":"MOBILE_BOSS"}	
20170620
2017-06-20 00:00:07.950 [http-nio-18777-exec-14] BIGDATA cn.cmvideo.ugc.contract.dao.imp.ContractDaoImp - {"contractStatus":"CANCELLED","cpId":"699009","createTime":1469857000000,"deliveryId":"673391423_8613965192409_699009_30830153_20160730133640_CUTOVER_CONTRACTDELIVERY","effectTime":1497888007950,"endTime":1497888007950,"id":"8613965192409_699009_30830153_20160730133640_cutover_contractID","lastResumeTime":0,"lastSuspendTime":0,"lastUpdateTime":1497888007950,"msisdn":"8613965192409","operCode":"30830153","operateStatus":"SUSPENDED","orderId":"673391423_2028597606-cutover_ItemPurchaseOrder","provinceId":"551","signTime":0,"type":"MOBILE_BOSS"}	
20170620