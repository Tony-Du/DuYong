



--set hive.auto.convert.join=false;
set mapreduce.job.name=stg.dmt_fact_divide_revenue_daily_${STATIS_DAY};

--set hive.exec.dynamic.partition.mode=nonstrict;
--set hive.exec.dynamic.partition=true;
set hive.merge.mapredfiles=true;

insert overwrite table stg.dmt_fact_divide_revenue_daily partition (statis_day = '${STATIS_DAY}')
select
    nvl(goods_name, '剔重汇总'),
    nvl(goods_type, '剔重汇总'),
    expire_month,
    nvl(auto_renew_flag, '剔重汇总'),
    nvl(payment_type, '剔重汇总'),
    nvl(currency_type, '剔重汇总'),
    nvl(virtual_operation_flag, '剔重汇总'),
    nvl(dept_name, '剔重汇总'),
    nvl(business_type, '剔重汇总'),
    nvl(business_id, '剔重汇总'),
    nvl(business_name, '剔重汇总'),
    nvl(sub_business_name, '剔重汇总'),
    nvl(term_prod_type, '剔重汇总'),
    nvl(chn_attr_1_name, '剔重汇总'),
    nvl(chn_attr_2_name, '剔重汇总'),
    nvl(channel_id, '剔重汇总'),
    nvl(phone_province_name, '剔重汇总'),
    nvl(phone_city_name, '剔重汇总'),
    count(distinct in_order_user_id) as in_order_user_num,
    sum(mtd_new_add_divide_revenue_amt) as mtd_new_add_divide_revenue_amt,
    sum(mtd_stock_divide_revenue_amt) as mtd_stock_divide_revenue_amt,
    sum(boss_resume_amt) as boss_resume_amt,
    grouping__id as grain_ind
from
(
    select
        nvl(a.goods_name, 'NA') as goods_name,
        nvl(a.goods_type, 'NA') as goods_type,
        nvl(substr(a.order_item_order_end_time, 1, 6), 'NA') as expire_month,
        --case when a.order_item_auth_type = 'BOSS_MONTH' or a.payment_type = '309' then 'Y' else 'N' end as auto_renew_flag,
        nvl(a.auto_renew_flag, 'NA') as auto_renew_flag,
        nvl(a.payment_type, 'NA') as payment_type,
        if(nvl(a.currency_type, '')='', 'NA', a.currency_type) as currency_type,
        nvl(a.virtual_operation_flag, 'NA') as virtual_operation_flag,
        nvl(c.dept_name, 'NA') as dept_name,  --对于算钱，部门只需要从业务部门表关联即可
        nvl(h.b_type, 'NA') as business_type,
        nvl(h.business_id, 'NA') as business_id,
        nvl(h.business_name, 'NA') as business_name,
        nvl(h.sub_busi_name, 'NA') as sub_business_name,  
        nvl(e.term_video_type_name, 'NA') as term_prod_type,
        nvl(g.chn_attr_1_name, 'NA') as chn_attr_1_name,
        nvl(g.chn_attr_2_name, 'NA') as chn_attr_2_name,
        nvl(a.channel_id, 'NA') as channel_id,
        nvl(i.prov_name, nvl(j.prov_name, 'NA')) as phone_province_name,
        nvl(i.region_name, nvl(j.region_name, 'NA')) as phone_city_name,
        a.order_user_id as in_order_user_id,
        0 as mtd_new_add_divide_revenue_amt,
        0 as mtd_stock_divide_revenue_amt,
        0 as boss_resume_amt
    from rptdata.fact_order_daily_snapshot a
    left join rptdata.dim_server h
        on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
    left join rptdata.dim_dept_busi c
        on (case when h.business_id = '-998' then concat('', rand()) else h.business_id end) = c.business_id
    left join rpt.dim_term_prod_v e
        on (case when a.term_prod_id = '-998' then concat('', rand()) else a.term_prod_id end) = e.term_prod_id
    left join rptdata.dim_charge_product f
        on (case when a.product_id = '-998' then concat('', rand()) else a.product_id end) = f.chrgprod_id
    left join rptdata.dim_chn g
        on (case when a.channel_id = '-998' then concat('', rand()) else a.channel_id end) = g.chn_id
    left join rptdata.dim_region i
        on (case when a.order_msisdn_region_id = '-998' then concat('', rand()) else a.order_msisdn_region_id end) = i.region_id
    left join rptdata.dim_region j
        on (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) = j.region_id
    --算月累计值
    where snapshot_day between concat(substr('${STATIS_DAY}',1,6), '01') and '${STATIS_DAY}'
        --and order_item_delivery_handler = 'MIGU_AUTH'
        
    union all
    
    select
        nvl(a.goods_name, 'NA') as goods_name,
        nvl(a.goods_type, 'NA') as goods_type,
        nvl(substr(a.order_item_order_end_time, 1, 6), 'NA') as expire_month,
        nvl(a.auto_renew_flag, 'NA') as auto_renew_flag,
        nvl(a.payment_type, 'NA') as payment_type,
        if(nvl(a.currency_type, '')='', 'NA', a.currency_type) as currency_type,
        nvl(a.virtual_operation_flag, 'NA') as virtual_operation_flag,
        nvl(c.dept_name, 'NA') as dept_name,
        nvl(h.b_type, 'NA') as business_type,
        nvl(h.business_id, 'NA') as business_id,
        nvl(h.business_name, 'NA') as business_name,
        nvl(h.sub_busi_name, 'NA') as sub_business_name,
        nvl(e.term_video_type_name, 'NA') as term_prod_type,
        nvl(g.chn_attr_1_name, 'NA') as chn_attr_1_name,
        nvl(g.chn_attr_2_name, 'NA') as chn_attr_2_name,
        nvl(a.channel_id, 'NA') as channel_id,
        nvl(i.prov_name, nvl(j.prov_name, 'NA')) as phone_province_name,
        nvl(i.region_name, nvl(j.region_name, 'NA')) as phone_city_name,
        null as in_order_user_id,
        --要考虑：上月是否成功，订购关系是否重复
        --if(boss_repeat_order_flag = 'Y' or boss_last_success_bill_flag = 'N',  0 , if(src_file_day = '${STATIS_DAY}', payment_amt, 0))
        case when a.new_add_flag = 'Y' then if(boss_repeat_order_flag = 'Y' or boss_last_success_bill_flag = 'N', 0, a.payment_amt) else 0 end as mtd_new_add_divide_revenue_amt,
        case when a.new_add_flag = 'N' then if(boss_repeat_order_flag = 'Y' or boss_last_success_bill_flag = 'N', 0, if(a.boss_resume_flag = 'Y' , 0, a.payment_amt)) else 0 end as mtd_stock_divide_revenue_amt,
        case when a.boss_resume_flag = 'Y' then a.payment_amt else 0 end as boss_resume_amt
    from rptdata.fact_sim_bill_detail a
    left join rptdata.dim_server h
        on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
    left join rptdata.dim_dept_busi c
        on (case when h.business_id = '-998' then concat('', rand()) else h.business_id end) = c.business_id
    left join rpt.dim_term_prod_v e
        on (case when a.term_prod_id = '-998' then concat('', rand()) else a.term_prod_id end) = e.term_prod_id
    left join rptdata.dim_charge_product f
        on (case when a.product_id = '-998' then concat('', rand()) else a.product_id end) = f.chrgprod_id
    left join rptdata.dim_chn g
        on (case when a.channel_id = '-998' then concat('', rand()) else a.channel_id end) = g.chn_id
    left join rptdata.dim_region i
        on (case when a.order_msisdn_region_id = '-998' then concat('', rand()) else a.order_msisdn_region_id end) = i.region_id
    left join rptdata.dim_region j
        on (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) = j.region_id
    where bill_day between concat(substr('${STATIS_DAY}',1,6), '01') and '${STATIS_DAY}'
        --and order_item_delivery_handler = 'MIGU_AUTH'
) all
group by dept_name, business_type, business_id, business_name, sub_business_name, term_prod_type, phone_province_name, phone_city_name, chn_attr_1_name, chn_attr_2_name, channel_id,
    goods_name, goods_type, expire_month, auto_renew_flag, payment_type, currency_type, virtual_operation_flag 
grouping sets(

--业务5: dept_name, business_type, business_id, business_name, sub_busi_name
--终端1: term_prod_type
--地域2: province_name, region_name
--渠道3: channel_l1_attr, channel_l2_attr, chn_id
--支付属性（来自payment）3: payment_type, currency_type, virtual_operation_flag
--商品属性（来自order）2: goods_type, goods_name
--交付属性（来自item）2: expire_month, auto_renew_flag

--产品部门看
--业务 x 交付属性
(dept_name, expire_month, auto_renew_flag),
(dept_name, business_type, expire_month, auto_renew_flag),
(dept_name, business_type, business_id, business_name, expire_month, auto_renew_flag),
(dept_name, business_type, business_id, business_name, sub_business_name, expire_month, auto_renew_flag),
--业务 x 支付属性 x 交付属性
(dept_name, business_type, business_id, business_name, virtual_operation_flag, expire_month, auto_renew_flag),
(dept_name, business_type, business_id, business_name, payment_type, currency_type, expire_month, auto_renew_flag),
--业务 x 支付属性 x 交付属性
(dept_name, business_type, business_id, business_name, goods_name, goods_type, expire_month, auto_renew_flag, payment_type, currency_type, virtual_operation_flag),
--业务 x 终端 x 交付属性
(dept_name, business_type, business_id, business_name, term_prod_type, expire_month, auto_renew_flag),
--业务 x 渠道 x 交付属性
(dept_name, business_type, business_id, business_name, chn_attr_1_name, chn_attr_2_name, channel_id, expire_month, auto_renew_flag),
--*****合作本次新增
(dept_name, business_type, business_id, business_name, sub_business_name, chn_attr_1_name, chn_attr_2_name, channel_id, expire_month, auto_renew_flag),
--业务 x 地域 x 支付属性 x 交付属性
(dept_name, business_type, business_id, business_name, sub_business_name, phone_province_name, phone_city_name, payment_type, expire_month, auto_renew_flag),

--商拓看
--地域 x 支付属性
(phone_province_name, payment_type, expire_month, auto_renew_flag),
(phone_province_name, phone_city_name, payment_type, expire_month, auto_renew_flag),
(phone_province_name, payment_type, virtual_operation_flag, expire_month, auto_renew_flag),
(phone_province_name, phone_city_name, payment_type, virtual_operation_flag, expire_month, auto_renew_flag),
--地域 x 渠道
(phone_province_name, chn_attr_1_name, chn_attr_2_name, payment_type, expire_month, auto_renew_flag),
(phone_province_name, phone_city_name, chn_attr_1_name, chn_attr_2_name, payment_type, expire_month, auto_renew_flag),

--产品商拓交叉
--业务 x 地域
--changed on 2017/07/10
--(business_type, business_id, business_name, sub_business_name, phone_province_name, phone_city_name, expire_month, auto_renew_flag),
(dept_name, business_type, business_id, business_name, sub_business_name, phone_province_name, phone_city_name, expire_month, auto_renew_flag),
--业务 x 地域 x 支付属性
--(business_type, business_id, business_name, phone_province_name, payment_type, currency_type, virtual_operation_flag, expire_month, auto_renew_flag),
(dept_name, business_type, business_id, business_name, phone_province_name, payment_type, currency_type, virtual_operation_flag, expire_month, auto_renew_flag),
--(business_type, business_id, business_name, phone_province_name, phone_city_name, expire_month, auto_renew_flag, payment_type, currency_type, virtual_operation_flag),
(dept_name, business_type, business_id, business_name, phone_province_name, phone_city_name, expire_month, auto_renew_flag, payment_type, currency_type, virtual_operation_flag),
--业务 x 地域 x 渠道
--remove below combination as it creates huge result and it's confirmed from business by xiaoshun
--(business_type, business_id, business_name, sub_business_name, phone_province_name, phone_city_name, chn_attr_1_name, chn_attr_2_name, channel_id, expire_month, auto_renew_flag),
--业务 x 地域 x 渠道 x 支付属性
--(business_type, business_id, business_name, phone_province_name, chn_attr_1_name, chn_attr_2_name, payment_type, currency_type, virtual_operation_flag, expire_month, auto_renew_flag)
(dept_name, business_type, business_id, business_name, phone_province_name, chn_attr_1_name, chn_attr_2_name, payment_type, currency_type, virtual_operation_flag, expire_month, auto_renew_flag),

--added on 2017/07/10
(dept_name, business_type, business_id, business_name, sub_business_name, phone_province_name, chn_attr_1_name, chn_attr_2_name, channel_id, expire_month, auto_renew_flag)
);


