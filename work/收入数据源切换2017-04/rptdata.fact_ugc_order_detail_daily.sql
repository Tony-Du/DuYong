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


--add jar hdfs:/user/hadoop/ods/udfjar/json-serde.jar;
set hive.auto.convert.join=false;
set mapreduce.job.name=rptdata.fact_ugc_order_detail_daily_${SRC_FILE_DAY};

set hive.merge.mapredfiles = true;

insert overwrite table rptdata.fact_ugc_order_detail_daily partition (src_file_day='${SRC_FILE_DAY}')
select 
    aa.user_type as user_type,			
    aa.network_type as network_type,	
    aa.cp_id as cp_id,					
    aa.use_type as use_type,				
    aa.term_prod_id as term_prod_id,
    aa.term_version_id as term_version_id,
    aa.chn_id as chn_id,
    aa.city_id as city_id,
    aa.sub_busi_id as sub_busi_id,
    aa.user_id as user_id,
    aa.period_unit as period_unit,
    aa.pay_chn_type as pay_chn_type,
    aa.order_status as order_status,
    aa.authorize_type as authorize_type,
    aa.currency as currency,
    aa.company_id as company_id,
    aa.goods_type as goods_type,
    aa.product_id as product_id,
    aa.product_type as product_type,
    aa.main as main,
    aa.program_id as program_id,
    aa.opr_time as opr_time,
    aa.substatus as substatus,
    aa.order_id as order_id,
    aa.payment_id as payment_id,
    aa.amount as amount,
    aa.payment_result,
	aa.serv_number,
	aa.sp_id,
	aa.authorize_period,
	aa.valid_start_time,
	aa.expire_time,
	aa.opr_login,
	aa.totalamount
from (
  select 
        a.user_type as user_type,                     --用户类型
        a.network_type as network_type,               --网络类型
        nvl(nvl(a.cp_id, b.cp_id), '-998') as cp_id,  --CP_ID
        a.use_type as use_type,                       --使用方式	
        a.term_prod_id as term_prod_id,				  --终端产品ID
        a.new_version_id as term_version_id,		  --终端版本ID
        a.chn_id as chn_id,							  --渠道ID
        a.city_id as city_id,						  --城市ID
        a.sub_busi_id as sub_busi_id,				  --子业务ID
        a.order_user_id as user_id,					--用户ID
        b.period_unit as period_unit,				--授权时间单位: hour(按次) day month(包周期)
        c.chn_type as pay_chn_type,					--支付渠道类型
        a.order_status as order_status,				--订单状态
        b.authorize_type as authorize_type,			--授权类型：BOSS_MONTH, NULL, PERIOD, TIMES
        c.currency as currency,						--货币类型（现金或者券）
        a.company_id as company_id,					--合作公司
        a.goods_type as goods_type,					--商品类型
        case when a.goods_type='MIGU_PMS' then goods_id else coalesce(b.product_id, j.productcode, '-998') end as product_id,		--a.goods_id
        b.product_type as product_type,
        b.main as main,
        a.resource_id as program_id,
        a.create_time as opr_time,
        null as substatus,
        a.order_id as order_id,
        a.payment_id as payment_id,
        c.pay_amount as amount,
		'3' as payment_result,--d.result as payment_result,
		a.phone_number as serv_number,
		k.sp_id as sp_id,
		b.amount as authorize_period,		--授权周期
		b.valid_start_time as valid_start_time,
		b.expire_time as expire_time,
		a.phone_number as opr_login,
		a.total_amount as totalamount
    from intdata.ugc_order_daily a 
    join intdata.ugc_order_item_daily b
    on a.order_id = b.order_id and b.src_file_day='${SRC_FILE_DAY}'
    join intdata.ugc_order_paychannel_daily c
    on a.order_id = c.order_id and c.src_file_day='${SRC_FILE_DAY}'
    --join intdata.ugc_payment_daily d
    --on a.payment_id = d.payment_id and d.src_file_day='${SRC_FILE_DAY}'
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
    join rptdata.dim_charge_product k
    on k.chrgprod_id = j.productcode and k.chrg_type_id='1'			--chrg_type_id='1'
    where a.src_file_day ='${SRC_FILE_DAY}'--a.src_file_day in ('${yesterday}', '${SRC_FILE_DAY}') --and a.resource_type='POMS_PROGRAM_ID'

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
        a.order_user_id as user_id,
        b.period_unit as period_unit,
        c.chn_type as pay_chn_type,
        a.order_status as order_status,
        b.authorize_type as authorize_type,
        c.currency as currency,
        a.company_id as company_id,
        a.goods_type as goods_type,
        case when a.goods_type='MIGU_PMS' then goods_id else coalesce(b.product_id, j.productcode, '-998') end as product_id,
        b.product_type as product_type,
        b.main as main,
        if(a.resource_type='POMS_PROGRAM_ID', a.resource_id, '-998') as program_id,
        a.create_time as opr_time,
        null as substatus,
        a.order_id as order_id,
        a.payment_id as payment_id,
        c.pay_amount as amount,
		'3' as payment_result,--d.result as payment_result,
		a.phone_number as serv_number,
		k.sp_id as sp_id,
		b.amount as authorize_period,
		b.valid_start_time as valid_start_time,
		b.expire_time as expire_time,
		a.phone_number as opr_login,
		a.total_amount as totalamount
    from intdata.ugc_order_daily a 
    join intdata.ugc_order_item_daily b
    on a.order_id = b.order_id and b.src_file_day='${SRC_FILE_DAY}' --and b.handler <>'AAA'
    join intdata.ugc_order_paychannel_daily c
    on a.order_id = c.order_id and c.src_file_day='${SRC_FILE_DAY}'
    --join intdata.ugc_payment_daily d
    --on a.payment_id = d.payment_id and d.src_file_day='${SRC_FILE_DAY}'
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
    join (select * from rptdata.dim_charge_product where chrg_type_id='0' and chrgprod_id in (		--chrg_type_id='0'
'2028595246',
'2028599910',
'2028599925',
'2028600184',
'2028597820',
'2028595240',
'2028598635',
'2028600090',
'2028595052',
'2028593132',
'2028595272',
'2028597743',
'2028593490',
'2028593268',
'2028595377',
'2028595249',
'2028600181',
'2028600252',
'2028600253',
'2028600254',
'2028600260',
'2028600262',
--2017/5/2
'2028593662',
'2028595110',
-- 2017/5/3
'2028593493', 
'2028597454',
'2028597606',
'2028595248',
'2028597730',
'2028595273',
'2028597613',
'2028595242',
'2028596750',
'2028597610',
'2028597610',
'2009093100',
'2028595050',
'2028593180',
'2028593814',
'2028593162',
'2028593498',
'2028596850',
'2028593340',
'2028597239',
'2028595241',
'2028593160',
'2028593158',
'2028594150',
'2028597336',
'2028599071',
'2028593130',
'2028596355',
'2028595245',
-- 2017/5/4
'2028596097',
'1082380100',
'2005568400',
'1073841900',
'2028593270',
'2028593142',
'2028596831',
'2028593345',
'2028596873',
'2028593134',
'2028593136',
'2028593128',
'2028595051',
'2006632800',
'2002218100',
'2028595271',
'2028595243',
'2028596874',
'2028595270',
'2028596050',
'2028593336',
'2028595572',
'2028595384',
'2028596832',
--2017/5/8
'2028593529',
'2028596511',
--2017/5/12
'2028593549',
--2017/5/15
'2028593910',
'2028599936',
'2028593174',
'2028599891',
'2028600181',
'2028600177',
'2028599948',
'2028595550',
'2028596512',
'2028597209',
'2028599947',
'2028600178',
'2028596870',
'2028600180',
'2028600179',
'2028597184'
)

) k on (j.productcode=k.chrgprod_id)
    where a.src_file_day ='${SRC_FILE_DAY}'--a.src_file_day in ('${yesterday}', '${SRC_FILE_DAY}')
    
    union all
    
    select
        '1' as user_type,
        '-998' as network_type,
        '-998' as cp_id,
        '-998' as use_type,
        nvl(nvl(if((i.chn_id2 is not null and i.is_wap=1), i.term_prod_id, NULL), f.term_prod_id), '-998') as term_prod_id,
        a.term_version_id as term_version_id,
        a.chn_id as chn_id,
        nvl(g.region_id, '-998') as city_id,
        nvl(nvl(c.sub_busi_id, d.sub_busi_id), '-998') as sub_busi_id,
        a.user_id as user_id,
        'MONTH' as period_unit,
        '50' as pay_chn_type,
        case a.order_type 
            when '1' then '5'
            when '2' then '14'
            when '5' then '998'
            else '999'
        end as order_status,--'5' as order_status,
        'BOSS_MONTH' as authorize_type,
        '156' as currency,
        nvl(h.comp_id, '-998') as company_id,
        'AAA' as goods_type,
        a.product_id as product_id,
        a.product_type as product_type,
        0 as main,
        if(nvl(a.program_id, '')='', '-998', a.program_id) as program_id,
        a.opr_time as opr_time,
        case when a.substatus = '' then null else a.substatus end as substatus,
        '-998' as order_id,
        '-998' as payment_id,
        nvl(cast(b.chrgprod_price as double), 0) as amount,
		'3' as payment_result,
		if(a.serv_number is not null, if(length(a.serv_number) = 13 and substr(a.serv_number, 1, 2) = '86', substr(a.serv_number, 3, 11), a.serv_number), '-998') as serv_number,
		b.sp_id as sp_id,
		'-998' as authorize_period,
		'' as valid_start_time,
		'' as expire_time,
		a.opr_login as opr_login,
		b.chrgprod_price as totalamount
    from intdata.aaa_order_log a
    left join rptdata.dim_charge_product b
    on (case when nvl(a.product_id, '-998') = '-998' then concat('',rand()) else a.product_id end) = b.chrgprod_id
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
    on a.chn_busi_type  = i.chn_id2   -- 临时使用chn_busi_type作为PlatID
    where a.src_file_day='${SRC_FILE_DAY}'
    and a.product_id not in 
    (
'2028599910','2028599925','2028600181'
        )

	union all
	
	--第三方
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
        a.order_user_id as user_id,
        b.period_unit as period_unit,
        c.chn_type as pay_chn_type,
        a.order_status as order_status,
        b.authorize_type as authorize_type,
        c.currency as currency,
        a.company_id as company_id,
        a.goods_type as goods_type,
        case when a.goods_type='MIGU_PMS' then goods_id else coalesce(b.product_id, j.productcode, '-998') end as product_id,
        b.product_type as product_type,
        b.main as main,
        a.resource_id as program_id,
        a.create_time as opr_time,
        null as substatus,
        a.order_id as order_id,
        a.payment_id as payment_id,
        c.pay_amount as amount,
		'3' as payment_result,--d.result as payment_result,
		a.phone_number as serv_number,
		'-998' as sp_id,
		b.amount as authorize_period,
		b.valid_start_time as valid_start_time,
		b.expire_time as expire_time,
		a.phone_number as opr_login,
		a.total_amount as totalamount
    from intdata.ugc_order_daily a 
    join intdata.ugc_order_item_daily b
    on a.order_id = b.order_id and b.src_file_day='${SRC_FILE_DAY}'
    join intdata.ugc_order_paychannel_daily c
    on a.order_id = c.order_id and c.src_file_day='${SRC_FILE_DAY}'
    --join intdata.ugc_payment_daily d
    --on a.payment_id = d.payment_id and d.src_file_day='${SRC_FILE_DAY}'
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
    where a.src_file_day ='${SRC_FILE_DAY}'--a.src_file_day in ('${yesterday}', '${SRC_FILE_DAY}')
	and j.productcode is null		--
	--and a.resource_type='POMS_PROGRAM_ID'
) aa
;
    
    
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
serv_number             string                                      
cp_id                   string                                      
product_id              string                                      
opr_login               string                                      
opr_time                string                                      
order_type              string                                      
order_chn               string                                      
chn_id_source           string                                      
product_name            string                                      
product_type            string                                      
node_id                 string                                      
program_id              string                                      
substatus               string                                      
rollback_flag           string                                      
boss_id                 string                                      
user_id                 string                                      
usernum_type            string                                      
broadcast_ip            string                                      
session_id              string                                      
page_id                 string                                      
pose_id                 string                                      
imei                    string                                      
chn_id                  string                                      
term_version_id         string                                      
chn_busi_type           string                                      
src_file_day            string