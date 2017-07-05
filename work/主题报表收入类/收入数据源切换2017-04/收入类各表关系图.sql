
ugc_order_*		包月部分、按次全部
aaa_10105		boss包月
ugc_90104_union	包月订购关系（快照）

ugc_order:所有按次每日新增
ugc_order(包月)		|->每日新增包月
aaa_10105(BOSS包月) |

-------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------

ods.ugc_50201_order_json_ex   -> ods.ugc_order_v            -> odsdata.ugc_order            -> intdata.ugc_order_daily            |
                              -> ods.ugc_order_item_v       -> odsdata.ugc_order_item       -> intdata.ugc_order_item_daily       |
							  -> ods.ugc_order_paychannel_v -> odsdata.ugc_order_paychannel -> intdata.ugc_order_paychannel_daily |-> rptdata.fact_ugc_order_detail_daily (ugc按次all + ugc包月 + aaa包月) |-> rptdata.fact_ugc_order_amount_daily
ods.ugc_50101_payment_json_ex -> ods.ugc_payment_v          -> odsdata.ugc_payment          -> intdata.ugc_payment_daily          |
							                                   ods.aaa_10105_order_log_ex   -> intdata.aaa_order_log              |
											(用户定购信息)				   
									ods.aaa_10104_order_ex (union all)  |-> intdata.ugc_90104_monthorder_union |-> intdata.ugc_order_relation (存量) |-> rptdata.fact_ugc_order_relation_detail_daily |-> rptdata.fact_ugc_order_relation_amount_daily 
ods.ugc_50201_order_ex(订单组件话单)|-> intdata.ugc_90104_MonthOrder    |      
ods.ugc_10106_goodsinfo_raw_ex(销售系统商品信息)      |
ods.aaa_10112_userid_usernum_ex(userid_usernum对应表) |

intdata.ugc_order_daily            |
intdata.ugc_order_item_daily       |-> intdata.ugc_tpp_contract (第三方包月新增)|->   rptdata.fact_ugc_tpp_in_order_detail_daily (该天第三方包月在定订单) -- 第三方
intdata.ugc_order_paychannel_daily |

-------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------

ods.ugc_50201_order_ex			-- 订单组件在订单状态为最终状态时，订单组件出话单 	hdfs://ns1/user/hadoop/ods/huawei/ugc/50201
ods.aaa_10112_userid_usernum_ex	-- UserID-UserNum对应表		hdfs://ns1/user/hadoop/ods/huawei/aaa/aaa-10112
ods.ugc_10106_goodsinfo_raw_ex	-- 销售系统全量商品信息		hdfs://ns1/user/hadoop/ods/huawei/ugc/ugc-10106

ods.aaa_10104_order_ex			-- AAA-10104 用户订购信息：提供用户在AAA平台的全量订购关系信息	hdfs://ns1/user/hadoop/ods/huawei/aaa/aaa-10104

ods.aaa_10105_order_log_ex		-- AAA-10105 用户订购日志：提供用户在AAA平台的订购日志			hdfs://ns1/user/hadoop/ods/huawei/aaa/aaa-10105