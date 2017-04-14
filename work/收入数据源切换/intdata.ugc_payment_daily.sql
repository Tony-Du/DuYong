set mapreduce.job.name=intdata.ugc_payment_daily${SRC_FILE_DAY};

set hive.groupby.skewindata=true;
set hive.optimize.skewjoin=true;
set hive.merge.mapredfiles = true;
insert overwrite table intdata.ugc_payment_daily partition (src_file_day='${SRC_FILE_DAY}')
select
	 paymentid,			--支付交易编号/流水号
	 userid,            --支付用户ID，也是支付账号，即userAccount
	 mobile,            --手机号码
	 createtime,        --支付创建时间
	 preapprovaltime,   --虚拟货币预支付时间
	 lastupdatetime,    --支付订单最后一次更新时间
	 finishtime,        --交易完成时间
	 result,            --支付状态，同支付状态Status
	 productorderid,    --商品在商家订单系统的订单号
	 merchantaccount,   --商户账号
	 thirdtoken,        --第三方支付系统产生的交易token用来映射到第三方的某个交易记录上
	 serviceid,         --计费点
	 channelid,         --渠道ID
	 appname            --APP名称
from odsdata.ugc_payment
where source_file_create_day='${SRC_FILE_DAY}';

----------------------------------------------------------

set mapreduce.job.name=odsdata.ugc_payment_${SRC_FILE_DAY};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

INSERT OVERWRITE TABLE odsdata.ugc_payment PARTITION(source_file_create_day)
  SELECT paymentID,
         userID,
         mobile,
         createTime,
         preapprovalTime,
         lastUpdateTime,
         finishTime,
         result,
         productOrderId,
         merchantAccount,
         thirdToken,
         serviceID,
         channelID,
         appName,
         source_file_create_day
FROM ods.ugc_payment_v
WHERE source_file_create_day = '${SRC_FILE_DAY}';

-----------------------------------------------------------------

CREATE VIEW `ods.ugc_payment_v` AS SELECT `p`.`paymentid`,
       `p`.`productorderid`,
       `p`.`merchantaccount`,
       `p`.`userid`, 
       `p`.`mobile`, 
       `p`.`result`,
       `p`.`createtime`,
       `p`.`preapprovaltime`,
       `p`.`lastupdatetime`,
       `p`.`finishtime`,
       `p`.`channelid`,
       `p`.`appname`,
       `p`.`thirdtoken`,
       `p`.`serviceid`,
       `s`.`src_file_day` as `source_file_create_day`
FROM `ods`.`ugc_50101_payment_json_ex` `s`
LATERAL VIEW json_tuple(`s`.`json`, 'paymentID', 'productOrderId', 'merchantAccount', 'userID', 'mobile', 'result', 'createTime', 'preapprovalTime', 'lastUpdateTime', 'finishTime', 'channelID', 'appName', 'thirdToken', 'serviceID') `p` 
as `paymentID`, `productOrderId`, `merchantAccount`, `userID`, `mobile`, `result`, `createTime`, `preapprovalTime`, `lastUpdateTime`, `finishTime`, `channelID`, `appName`, `thirdToken`, `serviceID`

------------------------------------------------------------------

`ods`.`ugc_50101_payment_json_ex`		--UGC支付组件支付话单：支付订单的状态为最终状态时，支付组件出支付话单
hdfs://ns1/user/hadoop/ods/huawei/ugc/50101


{
"IP":"",
"appName":"wuyekuaibo",				--
"channelID":"306500010000000",		--
"createTime":"20170409100420",		--
"extensioninfo":"{\"Version\":\"v2.5.1020\",
				  \"acceptancetime\":\"\",
				  \"bankCode\":\"\",
				  \"extendAttr\":\"eyJyZXF1ZXN0RnJvbSI6IlNESyIsInJlcXVlc3RUaW1lIjoiMTQ5MTcwMzQ1NjAwNCIsInBheW1lbnRJZCI6IjA1MDAyMDQwOTAyMDQxOTU5MDUwMDAiLCJ0cmFjZUlkIjoiZTIwZTExMjAtNjgzMC00MjEwLWIxMGEtY2QxYmQwNDMxNGNmIiwib3BlckNvZGUiOiIzMDgzMTAwMCIsImNwQ29kZSI6IjY5OTA2NSIsImNoYXJnZU1vZGUiOiJCT1NTX01PTlRIIiwidmVyc2lvbiI6InYyLjUuKiJ9\",
				  \"orderId\":\"\",
				  \"otherType\":\"\",
				  \"payType\":\"306\",
				  \"portalType\":\"4\",
				  \"reason\":\"\",
				  \"resultSource\":\"\",
				  \"storablePan\":\"\"}",
"finishTime":"20170409100431",		--
"lastUpdateTime":"20170409100431",	--
"merchantAccount":"ORDER_ADAPTER_SERVER",	--
"mobile":"",								--
"payChannelList":[{"amount":"1000",
				   "currency":"156",
				   "payType":"306",
				   "rechargeNum":""}
				 ],
"paymentID":"0500204090204195905000",	--
"preapprovalTime":"",		--
"productOrderId":"46334715_afeb1f2b-0899-4eb8-ad76-18006e397dc3_ItemPurchaseOrder",	--
"result":"3",	--
"serviceID":"",	--
"thirdToken":"23BIP2B12020170409100424312734",	--
"userID":"46334715"		--
}