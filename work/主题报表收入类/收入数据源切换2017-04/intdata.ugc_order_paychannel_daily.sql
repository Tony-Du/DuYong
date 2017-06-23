set mapreduce.job.name=intdata.ugc_order_paychannel_daily${SRC_FILE_DAY};

set hive.groupby.skewindata=true;
set hive.optimize.skewjoin=true;
set hive.merge.mapredfiles = true;
insert overwrite table intdata.ugc_order_paychannel_daily partition (src_file_day='${SRC_FILE_DAY}')
select
	orderid,		--订单id
	paychannelid,
	channeltype,	--渠道类型（boss、支付宝、微信等）
	currency,		--货币类型（现金或者券）
	payamount		--支付金额
from odsdata.ugc_order_paychannel
where source_file_create_day='${SRC_FILE_DAY}';

-------------------------------------------------------------------------------

set mapreduce.job.name=odsdata.ugc_order_paychannel_${SRC_FILE_DAY};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

INSERT OVERWRITE TABLE odsdata.ugc_order_paychannel PARTITION(source_file_create_day)
  SELECT orderID,
         paychannelid,
         channeltype,
         currency,
         payAmount,
         source_file_create_day
FROM ods.ugc_order_paychannel_v
WHERE source_file_create_day = '${SRC_FILE_DAY}';

-------------------------------------------------------------------------------

CREATE VIEW `ods.ugc_order_paychannel_v` AS SELECT `p`.`orderid`,
       `c1`.`paychannelid`,
       `c2`.`channeltype`,
       `c2`.`currency`,
       `c2`.`payamount`,
       `s`.`src_file_day` as `source_file_create_day`
FROM `ods`.`ugc_50201_order_json_ex` `s`
LATERAL VIEW json_tuple(`s`.`json`, 'orderID', 'payType') `p` as `orderID`, `payType_json`
LATERAL VIEW json_tuple(`p`.`paytype_json`, 'payChannels') `c` as `payChannels`
LATERAL VIEW posexplode(split(regexp_replace(regexp_replace(`c`.`paychannels`,'\\}\\,\\{','\\}\u0031\\{'),'\\[|\\]',''), '\u0031')) `c1` as `payChannelID`, `payChannel_json`
LATERAL VIEW json_tuple(`c1`.`paychannel_json`, 'channelType', 'currency', 'payAmount') `c2` as `channelType`, `currency`, `payAmount`;



`ods`.`ugc_50201_order_json_ex`
hdfs://ns1/user/hadoop/ods/huawei/ugc/50201


{"IP":"",
 "appName":"lztv",		--ugc_order_daily订单(汇总)信息
 "cancelOrderID":"",	--ugc_order_daily订单(汇总)信息
 "channelID":"308500040040001",		--ugc_order_daily订单(汇总)信息
 "count":"1",						--ugc_order_daily订单(汇总)信息
 "createTime":"20170409103203",		--ugc_order_daily订单(汇总)信息
 "deliveryResults":[{"deliveryId":"6fcb7c22-e8e3-4add-8132-376986a63de7",
 					"deliveryStatus":"5",
 					"itemId":"29083f83-1764-4aae-8a8f-cd7e60fa870e",
 					"resultData":"{\"endTime\":\"19700101080000\",\"startTime\":\"20170409103452\"}"}
 				   ],
 "extInfo":{"OperaCode":"",
 			"SPID":"",
 			"acceptancetime":"",
 			"cancelOperatorUserID":"",
 			"operatorUserID":"544995005",
 			"portalType":"4",
 			"resourceID":"616975302",			--ugc_order_daily订单(汇总)信息
 			"resourceType":"POMS_PROGRAM_ID",	--ugc_order_daily订单(汇总)信息
 			"subscribeTime":"20170409103221",
 			"unsubscribeTime":"",
 			"userNum":""
 		  },
 "externalOrderId":"",		--ugc_order_daily订单(汇总)信息
 "goodsID":"1002663",		--ugc_order_daily订单(汇总)信息
 "goodsType":"MIGU_PACKAGE_PROGRAM",			--ugc_order_daily订单(汇总)信息
 "lastUpdateTime":"20170409103221",
 "merchantAccount":"ORDER_ADAPTER_SERVER",		--ugc_order_daily订单(汇总)信息
 "orderID":"544995005_f49bfece-e1a6-4adc-9626-3a39a1723f0e_ItemPurchaseOrder",		--支付渠道ugc_order_paychannel_daily --ugc_order_daily订单(汇总)信息 --ugc_order_item_daily订单详情信息
 "orderItems":[{"authorization":{"amount":1,"authType":"BOSS_MONTH","periodUnit":"MONTH"},	--ugc_order_item_daily订单详情信息
 				"data":{"autoRelet":"1",
 						"channelId":"308500040040001",	
 						"contentID":"2028597247",
 						"content_node":"2028597247",
 						"extendauthorize":"0",
 						"productID":"2028597247",
 						"subtype":"05",
 						"totalamount":"1000",		
 						"traceId":"4657901519831183",
 						"usernum":"8615848383208"},
 				"desc":"",
 				"extInfo":{},
 				"handler":"AAA",
 				"itemId":"29083f83-1764-4aae-8a8f-cd7e60fa870e",
 				"main":"0",
 				"quantity":1,
 				"unitPrice":1000
 			 }],
 "orderStatus":"5",			--ugc_order_daily订单(汇总)信息
 "orderUserID":"544995005",	--ugc_order_daily订单(汇总)信息
 "payType":{"payChannels":[{"channelType":306,		--支付渠道ugc_order_paychannel_daily
 							"currency":156,
 							"payAmount":1000
						  }],
 			"payType":1},					--ugc_order_daily订单(汇总)信息
 "paymentID":"0500304090232057117000",		--ugc_order_daily订单(汇总)信息
 "rawAmount":"1000",
 "serviceCode":"LIZHITVYIDONGHUAFEIBAOYUE-ISBF5GAZ-BAVDVPCD",	--ugc_order_daily订单(汇总)信息
 "subcycle":"",
 "totalAmount":"1000"	--ugc_order_daily订单(汇总)信息
}