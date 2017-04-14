set mapreduce.job.name=intdata.ugc_order_item_daily${SRC_FILE_DAY};

set hive.groupby.skewindata=true;
set hive.optimize.skewjoin=true;
set hive.merge.mapredfiles = true;
insert overwrite table intdata.ugc_order_item_daily partition (src_file_day='${SRC_FILE_DAY}')
select
	 a.orderid,			--订单编号
	 a.itemid,			--交付物编号
	 a.amount,          --授权周期
	 a.authtype,        --授权类型:BOSS_MONTH, NULL, PERIOD, TIMES
	 a.periodunit,      --周期单位        
	 a.autorelet,       --自动续订      
	 a.channelid,       --渠道ID        
	 a.expiretime,      --过期时间       
	 a.extendauthorize, --
	 a.productid,       --当交付物为pms产品时填写id
	 a.producttype,     --pms产品类型
	 a.subtype,         --子状态
	 a.usernum,         --授权用户
	 a.validstarttime,  --授权开始时间
	 a.desc,            --描述
	 a.handler,         --授权交付系统:MIGU_AUTH, AAA, CONTRACT, MIGU_ACCOUNT
	 a.main,            --是否主交付物
	 a.quantity,        --数量
	 a.unitprice,       --单价
	 a.resource_id,     --当交付物为节目时填写节目id
	 '-998' as cp_id--b.sp_id as cp_id
from odsdata.ugc_order_item a
--left outer join rptdata.dim_charge_product b
--on a.productid = b.chrgprod_id --and b.dw_delete_flag = 'N'
where source_file_create_day='${SRC_FILE_DAY}';

---------------------------------------------------------

set mapreduce.job.name=odsdata.ugc_order_item_${SRC_FILE_DAY};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
INSERT OVERWRITE TABLE odsdata.ugc_order_item PARTITION(source_file_create_day)
  SELECT orderID,
         itemId,
         amount,
         authType,
         periodUnit,
         autorelet,
         channelid,
         expiretime,
         extendauthorize,
         productID,
         productType,
         subtype,
         usernum,
         validstarttime,
         desc,
         handler,
         main,
         quantity,
         unitPrice,
         resource_id,
         source_file_create_day
FROM ods.ugc_order_item_v
WHERE source_file_create_day = '${SRC_FILE_DAY}';

------------------------------------------------------------

CREATE VIEW `ods.ugc_order_item_v` AS SELECT `p`.`orderid`,
       `c2`.`itemid`,
       `c2`.`main`,
       `c2`.`quantity`,
       `c2`.`unitprice`,
       `c2`.`desc`,
       `c2`.`handler`,
       `c3`.`amount`,
       `c3`.`authtype`,
       `c3`.`periodunit`,
       `c4`.`autorelet`,
       `c4`.`channelid`,
       `c4`.`expiretime`,
       `c4`.`extendauthorize`,
       `c4`.`productid`,
       `c4`.`producttype`,
       `c4`.`subtype`,
       `c4`.`usernum`,
       `c4`.`validstarttime`,
       `c4`.`resource_id`,
       `s`.`src_file_day` as `source_file_create_day`
FROM `ods`.`ugc_50201_order_json_ex` `s`
LATERAL VIEW json_tuple(`s`.`json`, 'orderID', 'orderItems') `p` 
as `orderID`, `orderItems`
LATERAL VIEW posexplode(split(regexp_replace(regexp_replace(`p`.`orderitems`,'\\}\\,\\{','\\}\u0031\\{'),'\\[|\\]',''), '\u0031')) `c1` 
as `orderItemID`, `orderItem_json`
LATERAL VIEW json_tuple(`c1`.`orderitem_json`, 'itemId', 'main', 'quantity', 'unitPrice', 'desc', 'handler', 'authorization', 'data') `c2` 
as `itemId`, `main`, `quantity`, `unitPrice`, `desc`, `handler`, `authorization_json`, `data_json`
LATERAL VIEW json_tuple(`c2`.`authorization_json`, 'amount', 'authType', 'periodUnit') `c3` 
as `amount`, `authType`, `periodUnit`
LATERAL VIEW json_tuple(`c2`.`data_json`, 'autorelet', 'channelId', 'expireTime', 'extendauthorize', 'productID', 'productType', 'subtype', 'usernum', 'validstarttime', 'resource_id') `c4` 
as `autorelet`, `channelId`, `expireTime`, `extendauthorize`, `productID`, `productType`, `subtype`, `usernum`, `validstarttime`, `resource_id`


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

