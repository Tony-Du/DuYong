set mapreduce.job.name=rptdata.fact_ugc_order_amount_daily${SRC_FILE_DAY};

set hive.groupby.skewindata=true;
set hive.optimize.skewjoin=true;
set hive.merge.mapredfiles = true;
insert overwrite table rptdata.fact_ugc_order_amount_daily partition (src_file_day='${SRC_FILE_DAY}')
select  
	 user_type,                                      
	 network_type,                                      
	 cp_id,                                      
	 use_type,                                      
	 term_prod_id, 
	 term_version_id,                                     
	 chn_id,                                      
	 city_id,                                      
	 sub_busi_id,                                      
	 period_unit,                                      
	 order_status,                                      
	 authorize_type, 
	 currency,
	 pay_chn_type,
	 company_id,                                      
	 goods_type,
	 main,
	 SUM(amount) as amount
from rptdata.fact_ugc_order_detail_daily
where src_file_day='${SRC_FILE_DAY}'
group by user_type, network_type, cp_id, use_type, term_prod_id, term_version_id, chn_id, city_id, sub_busi_id, 
    period_unit, order_status, authorize_type, currency, pay_chn_type, company_id, goods_type, main;

---------------------------------------------------------------------

add jar hdfs:/user/hadoop/ods/udfjar/json-serde.jar;
set hive.auto.convert.join=false;
set mapreduce.job.name=rptdata.fact_ugc_order_detail_daily${SRC_FILE_DAY};

set hive.merge.mapredfiles = true;																			-- 今天产生的订单放在今天的分区里，所以为每天的新增订单
insert overwrite table rptdata.fact_ugc_order_detail_daily partition (src_file_day='${SRC_FILE_DAY}')		-- ugc_按次 + ugc_包月（BOSS+第三方） + aaa_BOSS包月
select 
	aa.user_type as user_type,               --用户类型,默认1，注册用户
	aa.network_type as network_type,         --网络类型
	aa.cp_id as cp_id,                       --cp_id
	aa.use_type as use_type,                 --使用方式
	aa.term_prod_id as term_prod_id,         --终端产品ID
    aa.term_version_id as term_version_id,   --终端产品版本id
	aa.chn_id as chn_id,                     --渠道ID
	aa.city_id as city_id,                   --地市
	aa.sub_busi_id as sub_busi_id,           --子业务
	aa.user_id as user_id,                   --用户标识
	aa.period_unit as period_unit,           --周期单位
	aa.pay_chn_type as pay_chn_type,         --支付渠道类型
	aa.order_status as order_status,         --订单状态
	aa.authorize_type as authorize_type,     --授权类型
	aa.currency as currency,                 --货币类型（现金或者券）
	aa.company_id as company_id,             --合作公司
    aa.goods_type as goods_type,             --商品类型
    aa.main as main,                         --是否主交付物
	SUM(aa.amount) as amount                 --支付金额
from (
	select 
		a.user_type as user_type,
		a.network_type as network_type,
		nvl(nvl(a.cp_id, b.cp_id), '-998') as cp_id,
		a.use_type as use_type,
		a.term_prod_id as term_prod_id,
		a.new_version_id as term_version_id,
		a.chn_id as chn_id,
		a.city_id as city_id,
		a.sub_busi_id as sub_busi_id,
		a.phone_number as user_id,--a.order_user_id as user_id,
		b.period_unit as period_unit,
		c.chn_type as pay_chn_type,
		a.order_status as order_status,
		b.authorize_type as authorize_type,
		c.currency as currency,
		a.company_id as company_id,
		a.goods_type as goods_type,
		b.main as main,
		c.pay_amount as amount
	from intdata.ugc_order_daily a 
	join intdata.ugc_order_item_daily b
	on a.order_id = b.order_id and b.src_file_day='${SRC_FILE_DAY}'	
	join intdata.ugc_order_paychannel_daily c
	on a.order_id = c.order_id and c.src_file_day='${SRC_FILE_DAY}'
    join intdata.ugc_payment_daily d
    on a.payment_id = d.payment_id and d.src_file_day='${SRC_FILE_DAY}'
	join (
		select 
		t4.service.code as program,
		t4.service.payment[0].charge.cpcode,
		t4.service.payment[0].charge.opercode,
		t4.service.payment[0].charge.productcode
		from (select * from ods.ugc_10106_goodsinfo_raw_ex t1 where t1.src_file_day ='${SRC_FILE_DAY}') t3
		LATERAL VIEW explode(service) t4 as service
	) j
	on a.service_code = j.program
	join rptdata.dim_productid_productname k
	on k.chrgprod_id = j.productcode and k.chrg_type_id='1'		--计费产品类型：按次
	where a.src_file_day = '${SRC_FILE_DAY}' and a.resource_type = 'POMS_PROGRAM_ID'	-- 触发订单的资源类型 为 “POMS_PROGRAM_ID”

    union all

	select 
		a.user_type as user_type,
		a.network_type as network_type,
		nvl(nvl(a.cp_id, b.cp_id), '-998') as cp_id,
		a.use_type as use_type,
		a.term_prod_id as term_prod_id,
		a.new_version_id as term_version_id,
		a.chn_id as chn_id,
		a.city_id as city_id,
		a.sub_busi_id as sub_busi_id,
		a.phone_number as user_id,--a.order_user_id as user_id,
		b.period_unit as period_unit,
		c.chn_type as pay_chn_type,
		a.order_status as order_status,
		b.authorize_type as authorize_type,
		c.currency as currency,
		a.company_id as company_id,
		a.goods_type as goods_type,
		b.main as main,
		c.pay_amount as amount
	from intdata.ugc_order_daily a 
	join intdata.ugc_order_item_daily b
	on a.order_id = b.order_id and b.src_file_day='${SRC_FILE_DAY}' and b.handler <>'AAA'	--授权交付系统不为 AAA
	join intdata.ugc_order_paychannel_daily c
	on a.order_id = c.order_id and c.src_file_day='${SRC_FILE_DAY}'	-- c.chn_type in (50,58,59,302,304,306) 表示boos包月
    join intdata.ugc_payment_daily d
    on a.payment_id = d.payment_id and d.src_file_day='${SRC_FILE_DAY}'
	join (
		select 
		t4.service.code as program,
		t4.service.payment[0].charge.cpcode,
		t4.service.payment[0].charge.opercode,
		t4.service.payment[0].charge.productcode
		from (select * from ods.ugc_10106_goodsinfo_raw_ex t1 where t1.src_file_day ='${SRC_FILE_DAY}') t3
		LATERAL VIEW explode(service) t4 as service
	) j
	on a.service_code = j.program		--a.service_code：计费点
	join rptdata.dim_productid_productname k
	on k.chrgprod_id = j.productcode and k.chrg_type_id='0'		--收费产品类型：包月
	where a.src_file_day='${SRC_FILE_DAY}'
	
	union all
	
	select
		'1' as user_type,	--注册用户
		'-998' as network_type,
		'-998' as cp_id,
		'-998' as use_type,
		nvl(nvl(if((i.chn_id2 is not null and i.is_wap=1), i.term_prod_id, NULL), f.term_prod_id), '-998') as term_prod_id,
		a.term_version_id as term_version_id,
		a.chn_id as chn_id,
		nvl(g.region_id, '-998') as city_id,
		nvl(nvl(c.sub_busi_id, d.sub_busi_id), '-998') as sub_busi_id,
		if(a.serv_number is not null, if(length(a.serv_number) = 13 and substr(a.serv_number, 1, 2) = '86', substr(a.serv_number, 3, 11), a.serv_number), '-998') as user_id,--a.order_user_id as user_id,
		'MONTH' as period_unit,
		'50' as pay_chn_type,	--支付渠道：话费点播支付
		'5' as order_status,	--订单状态：订单完成状态
		'BOSS_MONTH' as authorize_type,	-- boss包月
		'156' as currency,	--支付货币类型：人民币
		nvl(h.comp_id, '-998') as company_id,
		'AAA' as goods_type,	-- 产品类型：
		0 as main,
		nvl(cast(b.chrgprod_price as double), 0) as amount
	from intdata.aaa_order_log a
	left join rptdata.dim_charge_product b
	on (case when nvl(a.product_id, '-998') = '-998' then concat('',rand()) else a.product_id end) = b.chrgprod_id	--rand() 取0-1内的随机数
	left join rptdata.dim_product_busi c
	on (case when nvl(a.product_id, '-998') = '-998' then concat('',rand()) else a.product_id end) = c.product_id --and c.dw_delete_flag = 'N'
	left join rptdata.dim_product_busi_new d
	on (case when nvl(a.product_id, '-998') = '-998' then concat('',rand()) else a.product_id end) = d.new_product_id
	left outer join rptdata.dim_term_prod f
	on a.term_version_id = f.term_version_id and a.term_version_id<>'-998'
	left join rptdata.dim_phone_belong g
	on (if(a.serv_number is not null, if(substr(a.serv_number, 1, 2)='86', substr(a.serv_number, 3, 7), substr(a.serv_number, 1, 7)), concat('', rand()))) = g.phone_range
	left outer join rptdata.dim_comp_busi h
	on b.sub_busi_bdid = h.sub_busi_id
	left outer join rptdata.dim_chn_id i
	on a.chn_busi_type	= i.chn_id2   --临时使用chn_busi_type作为PlatID
	where a.src_file_day='${SRC_FILE_DAY}'
	
) aa
group by aa.user_type,aa.network_type,aa.cp_id,aa.use_type,aa.term_prod_id,aa.term_version_id,aa.chn_id,aa.city_id,aa.sub_busi_id,aa.user_id,aa.period_unit,
    aa.pay_chn_type,aa.order_status,aa.authorize_type, aa.currency,aa.company_id,aa.goods_type,aa.main;

	
	
+-----------------------+--+            +------------------+--+
|       goodstype       |               |  resource_type   |
+-----------------------+--+            +------------------+--+
| CP_OPERATION          |               | NULL             |
| MIGU_COUPON           |               | POMS_ALBUM_ID    |
| MIGU_LIVE             |               | POMS_PROGRAM_ID  |
| MIGU_MOVIE_CARD       |               +------------------+--+
| MIGU_PACKAGE          |
| MIGU_PACKAGE_PROGRAM  |
| MIGU_PMS              |
| MIGU_PROGRAM          |
+-----------------------+--+

-- 数据源-------------------------------------	
desc intdata.aaa_order_log;
OK
serv_number         	string              	                    
cp_id               	string              	                    
product_id          	string              	                    
opr_login           	string              	                    
opr_time            	string              	                    
order_type          	string              	                    
order_chn           	string              	                    
chn_id_source       	string              	                    
product_name        	string              	                    
product_type        	string              	                    
node_id             	string              	                    
program_id          	string              	                    
substatus           	string              	                    
rollback_flag       	string              	                    
boss_id             	string              	                    
user_id             	string              	                    
usernum_type        	string              	                    
broadcast_ip        	string              	                    
session_id          	string              	                    
page_id             	string              	                    
pose_id             	string              	                    
imei                	string              	                    
chn_id              	string              	                    
term_version_id     	string              	                    
chn_busi_type       	string              	                    
src_file_day        	string