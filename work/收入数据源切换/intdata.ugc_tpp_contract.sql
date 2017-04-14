set mapreduce.job.name=rptdata.fact_ugc_tpp_in_order_detail_daily${SRC_FILE_DAY};

set hive.groupby.skewindata=true;
set hive.optimize.skewjoin=true;
set hive.merge.mapredfiles = true;
insert overwrite table rptdata.fact_ugc_tpp_in_order_detail_daily partition (src_file_day='${SRC_FILE_DAY}')	--该天在定订单
select 
	user_type_id,
	net_type_id,
	cp_id,
	broadcast_type_id,
	term_prod_id,
	term_version_id,
	chn_id,
	city_id,
	sub_busi_id,
	user_id,
	company_id
from intdata.ugc_tpp_contract
where substr(start_time, 1, 8) <='${SRC_FILE_DAY}' and substr(expire_time, 1, 8) >= '${SRC_FILE_DAY}'
group by user_type_id, net_type_id, cp_id, broadcast_type_id, term_prod_id, term_version_id, chn_id, city_id, sub_busi_id, user_id, company_id;

----------------------------------------------------------------------

set mapreduce.job.name=intdata.ugc_tpp_contract${SRC_FILE_DAY};

insert overwrite table intdata.ugc_tpp_contract partition (src_file_day='${SRC_FILE_DAY}')	--第三方支付包月记录
select 
    a.user_type as user_type_id,	--合约用户id
    a.network_type as net_type_id,	
    nvl(nvl(a.cp_id, b.cp_id), '-998') as cp_id,	
    a.use_type as broadcast_type_id,
    a.term_prod_id as term_prod_id,
    a.new_version_id as term_version_id,
    a.chn_id as chn_id,	--订购渠道
    a.city_id as city_id,
    a.sub_busi_id as sub_busi_id,
    a.company_id as company_id,
    a.phone_number as user_id,--a.order_user_id as user_id, 
    a.goods_id as goods_id, 	--商品id
    c.chn_type as pay_chn_type,	--付款渠道
    b.valid_start_time as start_time, 	--合约创建日期
    b.expire_time as expire_time,		--合约到期日期
    b.period_unit as period_unit,		--授权时间单位
    b.amount as authorize_period,		--授权周期
    b.unit_price * b.quantity as total_amount,	--合约周期总价
    ((b.unit_price * b.quantity) / cast(b.amount as int)) as period_amount	--合约周期单价
from intdata.ugc_order_daily a
join intdata.ugc_order_item_daily b
on a.order_id = b.order_id and b.period_unit = 'MONTH' and b.main = 0 and b.src_file_day='${SRC_FILE_DAY}'
join intdata.ugc_order_paychannel_daily c
on a.order_id = c.order_id and c.chn_type not in (50,58,59,302,304,306) and c.src_file_day='${SRC_FILE_DAY}'
where a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')
and a.order_status = 5
and a.src_file_day='${SRC_FILE_DAY}';


-- a.goods_type in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE') 表示 咪咕电影卡，咪钻等代币类商品的销售收入
-- c.chn_type in (50,58,59,302,304,306) 表示 boss付费

支付渠道：
编号 名称
16  payment虚拟货币支付方式
17  支付宝手机客户端支付
23  支付宝web即时到账支付
26  Appstore支付（验证receipt）
28  联通号码话费扣费
44  支付宝SDK支付
50  话费点播支付
55  微信SDK支付
57  Payment虚拟货币支付（预留+确认）
58  咪咕一级支付
59  咪咕SDK支付
60  电信IAP   SDK支付
61  支付宝代扣费
62  银视通支付
63  浦发银行先看后付
300 支付宝扫码支付
301 微信扫码支付
302 阳光计划话费支付(新六套)
303 支付宝扫码即时到账支付
304 网状网支付
306	阳光计划话费支付(老五套)