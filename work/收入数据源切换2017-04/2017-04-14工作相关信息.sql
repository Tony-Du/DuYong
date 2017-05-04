

1.	今天工作事项的进度及目标达成情况

	(1)熟悉主题报表收入类指标的部分第三方、新增和存量的代码，从ODS层到RPT层相关表的SQL代码,所涉及的表如下所示：
	
		ods.ugc_50201_order_json_ex   |-> ods.ugc_order_v            |-> odsdata.ugc_order            |-> intdata.ugc_order_daily            |
									  |-> ods.ugc_order_item_v       |-> odsdata.ugc_order_item       |-> intdata.ugc_order_item_daily       |
									  |-> ods.ugc_order_paychannel_v |-> odsdata.ugc_order_paychannel |-> intdata.ugc_order_paychannel_daily |-> rptdata.fact_ugc_order_detail_daily (ugc按次all + ugc包月 + aaa包月) |-> rptdata.fact_ugc_order_amount_daily
		ods.ugc_50101_payment_json_ex |-> ods.ugc_payment_v          |-> odsdata.ugc_payment          |-> intdata.ugc_payment_daily          |
																		ods.aaa_10105_order_log_ex    |-> intdata.aaa_order_log              |
																	
		ods.ugc_50201_order_ex			|-> intdata.ugc_90104_MonthOrder		|-> intdata.ugc_90104_monthorder_union |-> intdata.ugc_order_relation (存量) |-> rptdata.fact_ugc_order_relation_detail_daily |-> rptdata.fact_ugc_order_relation_amount_daily 
		ods.ugc_10106_goodsinfo_raw_ex  |  	ods.aaa_10104_order_ex (union all)  |      
		ods.aaa_10112_userid_usernum_ex |
			
		intdata.ugc_order_daily            |-> intdata.ugc_tpp_contract (第三方包月新增)|->   rptdata.fact_ugc_tpp_in_order_detail_daily (该天第三方包月在定订单) -- 第三方
		intdata.ugc_order_item_daily       |
		intdata.ugc_order_paychannel_daily |
		
		注：当然这并不是今天一天所完成的
	
	(2)熟悉模拟扣费话单逻辑,所涉及oracle中的存储过程如下：
	
		a.cdmp_mk.pm_month_fee_exception_m	(包月费用异常)
		b.cdmp_mk.pm_month_fee_order_log_d 	(当月新增收入)
		c.cdmp_mk.pm_month_fee_order_m		(当月存量收入)

	上述两项工作事项基本完成，事项(2)有很多不理解的地方，有待解决
	
2.	下一天的工作计划及目标

	被安排到另外的工作（接入数据源相关事项）

3.	工作中遇到需要团队协助解决的问题

	工作事项(2)中所涉及的oracle中的表有很多不知道具体含义，对实际业务的理解上存在不足，希望提供以下oracle表的相关文档
	cdmp_mk.tm_month_fee_info_d
	cdmp_dw.td_aaa_bill_d
	cdmp_dw.td_aaa_order_log_d
	cdmp_dw.td_pms_product_d
	cdmp_ods.to_business_info
	cdmp_dw.td_aaa_order_fact_d
	
	
	