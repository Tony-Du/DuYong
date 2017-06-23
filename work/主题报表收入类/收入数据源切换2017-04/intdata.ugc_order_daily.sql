

set mapreduce.job.name=intdata.ugc_order_daily${SRC_FILE_DAY};

set hive.groupby.skewindata=true;
set hive.optimize.skewjoin=true;
set hive.merge.mapredfiles = true;
insert overwrite table intdata.ugc_order_daily partition (src_file_day='${SRC_FILE_DAY}')
select
	bb.orderid,
	bb.orderuserid,                                                                        
	bb.phone_number,                                                                      
	bb.phone_range,                                                                        
	bb.createtime,                                                                         
	bb.orderstatus,                                                                        
	bb.merchantaccount,                                                                    
	bb.cancelorderid,                                                                      
	bb.totalamount,                                                                        
	bb.servicecode,                                                                        
	bb.count,                                                                              
	bb.paytype,                                                                            
	bb.chn_id_source,		--渠道id                                                       
	bb.paymentid,			--支付交易编号/流水号                                          
	bb.appname,                                                                           
	bb.externalorderid,		--外部订单id
	bb.goodsid,				--商品id
	bb.goodstype,			--商品类型
	bb.resourceid,			--触发订单的资源id，代表触发订购的节目
	bb.resourcetype,		--触发订单的资源类型
	'-998' as cp_id, --case when (bb.goodstype = 'MIGU_PMS' or bb.goodstype = 'MIGU_PROGRAM') then b.sp_id else NULL end as cp_id,--(根据goodsid关联rptdata.dim_charge_product获取sp_id)
	nvl(nvl(c.sub_busi_id, d.sub_busi_id), '-998') as sub_busi_id, --(	用goodsid关联cdmp_ods.to_busi_product_cfg的product_id得到sub_busi_id，如果为空，继续用goodsid关联cdmp_ods.to_new_product_busi_rlt_cfg的new_product_id得到sub_busi_id)
	nvl(bb.chn_values[0], '-998') as version_id,	--(解析chn_id_source，终端版本号对应，chn_id_source中"-"分割的part1)
	nvl(bb.chn_values[2], '-998') as chn_id_new,	--(解析渠道id所得)
	nvl(bb.chn_values[1], '-998') as chn_busi_type,	--渠道业务类型-99000 (解析渠道id所得)
	nvl(nvl(if((i.chn_id2 is not null and i.is_wap=1), i.term_prod_id, NULL), f.term_prod_id), '-998') as term_prod_id,--(根据version_id关联rptdata.dim_prod_id)
    --nvl(f.term_prod_id, '-998') as term_prod_id,
	nvl(g.region_id, '-998') as city_id,	--通过phone_range来判断
	nvl(substr(bb.chn_values[0], 1, 6), '-998') as new_version_id,	--(截取version_id前6位)
	'1' as user_type,	--(默认1，注册用户)
	'-998' as network_type,
	'-998' as use_type,
	nvl(h.comp_id, '-998') as company_id
from (
	select	
		a.orderid,
		a.orderuserid,
		nvl(e.usernum_02, '-998') as phone_number,--nvl(a.orderuserid, '-998') as phone_number,
		if(e.usernum_02 is not null, if(substr(e.usernum_02, 1, 2)='86', substr(e.usernum_02, 3, 7), substr(e.usernum_02, 1, 7)), '-998') as phone_range,--号码属性为02时，才是移动号码
		a.createtime,
		a.orderstatus,
		a.merchantaccount,
		a.cancelorderid,
		a.totalamount,
		a.servicecode,
		a.count,
		a.paytype,
		a.chn_id_source as chn_id_source,
		a.paymentid,
		a.appname,
		a.externalorderid,
		a.goodsid,
		a.goodstype,
		a.resourceid,
		a.resourcetype,
		split(--version-chn99000-chn_id
		case when chn_source_mask = '0-00000000000' then concat_ws('_', '-998', '-998', substr(a.chn_id_source, 3), concat('000', substr(a.chn_id_source, 1, 1)))
		     when chn_source_mask = '0000-00000000000' then concat_ws('_', '-998', '-998', substr(a.chn_id_source, 6), substr(a.chn_id_source, 1, 4))
		     when chn_source_mask = '0000-000000000000000' then concat_ws('_', '-998', '-998', substr(a.chn_id_source, 6), substr(a.chn_id_source, 1, 4))	-- -998_-998_304800000030000_7001
		     when chn_source_mask = '0000-00000000-00000-000000000000000' then concat_ws('_', substr(a.chn_id_source, 6, 8), substr(a.chn_id_source, 15, 5), substr(a.chn_id_source, 21), substr(a.chn_id_source, 1, 4))	-- 31090200_99000_304800000030000_0113
		     when chn_source_mask in ('00000000000', '000000000000000') then concat_ws('_', '-998', '-998', a.chn_id_source, '-998')		-- -998_-998_308500040040001_-998
		     when chn_source_mask = '00000000-00000-000000000000000' then concat_ws('_', substr(a.chn_id_source, 1, 8), substr(a.chn_id_source, 10, 5), substr(a.chn_id_source, 16), '-998')
		     when chn_source_mask like '0-00000000000-%' then concat_ws('_', '-998', '-998', substr(a.chn_id_source, 3, 11), concat('000', substr(a.chn_id_source, 1, 1)))
		     when chn_source_mask like '0000-00000000000-%' then concat_ws('_', '-998', '-998', substr(a.chn_id_source, 6, 11), substr(a.chn_id_source, 1, 4))    
		     else concat_ws('_', '-998', '-998', '-998', '-998')
		end, '_') as chn_values
	from (
		select 
			t1.orderid as orderid,
			t1.orderuserid as orderuserid,
			t1.createtime as createtime,
			t1.orderstatus as orderstatus,
			t1.merchantaccount as merchantaccount,
			t1.cancelorderid as cancelorderid,
			t1.totalamount as totalamount,
			t1.servicecode as servicecode,
			t1.count as count,
			t1.paytype as paytype,
			nvl(t2.dest_chn_id, t1.channelid) as chn_id_source,	--连接t2表，取t2的dest_chn_id  0113-31090200-99000-304800000030000 7001_304800000030000
			t1.paymentid as paymentid,
			t1.appname as appname,
			t1.externalorderid as externalorderid,
			t1.goodsid as goodsid,
			t1.goodstype as goodstype,
			t1.resourceid as resourceid,
			t1.resourcetype as resourcetype,
			regexp_replace(translate(nvl(t2.dest_chn_id, t1.channelid), '1234567899', '000000000'), '_', '-') as chn_source_mask	-- 308500040040001，0113-31090200-99000-304800000030000
		from odsdata.ugc_order t1
        left outer join rptdata.dim_chn_adjust_cfg t2
        on t1.channelid = t2.src_chn_id
		where source_file_create_day = '${SRC_FILE_DAY}'		
	) a
	left outer join rptdata.dim_userid_info e		--取e.usernum_02即phone_number
	on a.orderuserid = e.user_id -- and e.dw_delete_flag='N'
) bb
left outer join rptdata.dim_charge_product b		--取sp_id即cp_id
  on bb.goodsid = b.chrgprod_id --and b.dw_delete_flag = 'N'
left outer join rptdata.dim_product_busi c			--取sub_busi_id
  on bb.goodsid = c.product_id --and c.dw_delete_flag = 'N'
left outer join rptdata.dim_product_busi_new d		--取sub_busi_id
  on bb.goodsid = d.new_product_id --and d.dw_delete_flag = 'N'
left outer join rptdata.dim_term_prod f				--取term_prod_id
  on substr(bb.chn_values[0], 1, 6) = f.term_version_id and bb.chn_values[0]<>'-998' --and f.dw_delete_flag = 'N'
left outer join rptdata.dim_phone_belong g			--取region_id即city_id
  on bb.phone_range = g.phone_range --and g.dw_delete_flag = 'N'
left outer join rptdata.dim_comp_busi h				--取comp_id即company_id
  on (nvl(c.sub_busi_id, d.sub_busi_id)) = h.sub_busi_id --and h.dw_delete_flag = 'N'
left outer join rptdata.dim_chn_id i				--取term_prod_id
  on bb.chn_values[3]=i.chn_id2 --and i.dw_delete_flag='N';

----------------------------------------------------------------

set mapreduce.job.name=odsdata.ugc_order_${SRC_FILE_DAY};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
INSERT OVERWRITE TABLE odsdata.ugc_order PARTITION(source_file_create_day)
  SELECT orderID,		--订单id
         orderUserID,	--订单用户id
         createTime,	--订单创建时间
         orderStatus,	--订单状态
         merchantAccount,	--商户账号
         cancelOrderID,		--订单取消id
         totalAmount,		--订单总额，即指所有订单子项的实际金额之和
         serviceCode,		--计费点
         count,				--计费点数量，目前：均为1
         payType,			--支付方式：1、直接支付；2、短信支付；3、页面支付；4、找人代付
         channelID,			--渠道id
         paymentID,			--支付交易编号/流水号
         appName,			--app名称
         externalOrderId,   --外部订单id
         goodsID,           --商品id 
         goodsType,         --商品类型
         resourceID,        --触发订单的资源id，代表触发订购的节目
         resourceType,      --触发订单的资源类型 
         source_file_create_day
FROM ods.ugc_order_v
WHERE source_file_create_day = '${SRC_FILE_DAY}';

-----------------------------------------------------------

CREATE VIEW `ods.ugc_order_v` AS SELECT `p`.`appname`,
       `p`.`channelid`,
       `p`.`createtime`,
       `p`.`externalorderid`,
       `p`.`merchantaccount`,
       `p`.`orderid`,
       `p`.`paymentid`,
       `p`.`orderstatus`,
       `p`.`orderuserid`,
       `p`.`cancelorderid`,
       `p`.`totalamount`,
       `p`.`servicecode`,
       `p`.`count`,
       `d`.`paytype`,
       `p`.`goodsid`,
       `p`.`goodstype`,
       `e`.`resourceid`,
       `e`.`resourcetype`,
       `s`.`src_file_day` as `source_file_create_day`
FROM `ods`.`ugc_50201_order_json_ex` `s`
LATERAL VIEW json_tuple(`s`.`json`, 'appName', 'channelID', 'createTime', 'externalOrderId', 'merchantAccount', 'orderID', 'paymentID', 'orderStatus', 'orderUserID', 'cancelOrderID', 'totalAmount', 'serviceCode', 'count', 'payType', 'goodsID', 'goodsType', 'extInfo') `p` 
AS `appName`, `channelID`, `createTime`, `externalOrderId`, `merchantAccount`, `orderID`, `paymentID`, `orderStatus`, `orderUserID`, `cancelOrderID`, `totalAmount`, `serviceCode`, `count`, `paytype_json`, `goodsID`, `goodsType`, `extInfo_json`
LATERAL VIEW json_tuple(`p`.`paytype_json`, 'payType') `d` AS `payType`
LATERAL VIEW json_tuple(`p`.`extinfo_json`, 'resourceID', 'resourceType') `e` AS `resourceID`, `resourceType`


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

select * from rptdata.dim_chn_adjust_cfg t limit 10;
+--------------------------+-----------------------+--------------------------------------+-----------------+-------------------+-----------------+-------------------+-------------------+-------------+--+
| t.term_prod_belong_name  |     t.src_chn_id      |            t.dest_chn_id             | t.dw_create_by  | t.dw_create_time  | t.dw_update_by  | t.dw_update_time  | t.dw_delete_flag  |  t.dw_crc   |
+--------------------------+-----------------------+--------------------------------------+-----------------+-------------------+-----------------+-------------------+-------------------+-------------+--+
| 和视频                      | 7001_304800000030000  | 0113_31090200-99000-304800000030000  | sys             | 20170217111633    | sys             | 20170217111633    | N                 | 150749105   |
| 和视频                      | 7001_304800010030000  | 0113_31090200-99000-304800010030000  | sys             | 20170217111633    | sys             | 20170217111633    | N                 | 4001236813  |
| 和视频                      | 7001_304800020000000  | 0113_31090200-99000-304800020000000  | sys             | 20170217111633    | sys             | 20170217111633    | N                 | 3126887802  |
| 和视频                      | 7001_304800030000000  | 0113_31090200-99000-304800030000000  | sys             | 20170217111633    | sys             | 20170217111633    | N                 | 1558363526  |
| 和视频                      | 7001_304800040000000  | 0113_31090200-99000-304800040000000  | sys             | 20170217111633    | sys             | 20170217111633    | N                 | 2164202929  |
| 和视频                      | 7001_304900000030000  | 0113_31090200-99000-304900000030000  | sys             | 20170217111633    | sys             | 20170217111633    | N                 | 2934480642  |
| 和视频                      | 7001_304900000030002  | 0113_31090200-99000-304900000030002  | sys             | 20170217111633    | sys             | 20170217111633    | N                 | 2591544913  |
| 和视频                      | 7001_304900010000000  | 0113_31090200-99000-304900010000000  | sys             | 20170217111633    | sys             | 20170217111633    | N                 | 3968035468  |
| 和视频                      | 7001_304900020000000  | 0113_31090200-99000-304900020000000  | sys             | 20170217111633    | sys             | 20170217111633    | N                 | 477373897   |
| 和视频                      | 7001_304900030000000  | 0113_31090200-99000-304900030000000  | sys             | 20170217111633    | sys             | 20170217111633    | N                 | 4210433333  |
+--------------------------+-----------------------+--------------------------------------+-----------------+-------------------+-----------------+-------------------+-------------------+-------------+--+



+------------+--------------+------------+---------------+-----------+-----------------+-----------+-----------------+-------------------+-----------------+-------------------+-------------------+-------------+--+
| t.chn_id1  | t.chn_name1  | t.chn_id2  |  t.chn_name2  | t.remark  | t.term_prod_id  | t.is_wap  | t.dw_create_by  | t.dw_create_time  | t.dw_update_by  | t.dw_update_time  | t.dw_delete_flag  |  t.dw_crc   |
+------------+--------------+------------+---------------+-----------+-----------------+-----------+-----------------+-------------------+-----------------+-------------------+-------------------+-------------+--+
| -1         | 全部           | -1         | 全部            | null      | null            | 0.0       | sys             | 20161223134516    | sys             | 20161223134516    | N                 | 1192602948  |
| B001       | wap          | 0000       | wap 1.0       | null      | P0000001        | 1.0       | sys             | 20161223134516    | sys             | 20161223134516    | N                 | 3116873972  |
| A001       | 客户端          | 0002       | 富媒体客户端        | null      | null            | 0.0       | sys             | 20161223134516    | sys             | 20161223134516    | N                 | 4158747958  |
| A001       | 客户端          | 0009       | 简化UI          | null      | null            | 0.0       | sys             | 20161223134516    | sys             | 20161223134516    | N                 | 536209698   |
| A001       | 客户端          | 0101       | MM客户端         | null      | P0000009        | 1.0       | sys             | 20161223134516    | sys             | 20161223134516    | N                 | 2895807768  |
| A001       | 客户端          | 0109       | 客户端3.0        | null      | null            | 0.0       | sys             | 20161223134516    | sys             | 20161223134516    | N                 | 1126976223  |
| A001       | 客户端          | 0110       | 悦听客户端         | null      | null            | 0.0       | sys             | 20161223134516    | sys             | 20161223134516    | N                 | 3818589734  |
| A001       | 客户端          | 0111       | i视界手机客户端1.0   | null      | null            | 0.0       | sys             | 20161223134516    | sys             | 20161223134516    | N                 | 3818168507  |
| A001       | 客户端          | 0112       | 和视频能力开放平台     | null      | null            | 0.0       | sys             | 20161223134516    | sys             | 20161223134516    | N                 | 1623580738  |
| 9999       | 其它           | 0113       | 和视界客户端能力开放平台  | null      | null            | 0.0       | sys             | 20161223134516    | sys             | 20161223134516    | N                 | 1870417494  |
+------------+--------------+------------+---------------+-----------+-----------------+-----------+-----------------+-------------------+-----------------+-------------------+-------------------+-------------+--+