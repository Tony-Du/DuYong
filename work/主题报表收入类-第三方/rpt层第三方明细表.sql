create table rptdata.fact_ugc_tpp_contract_detail_daily (
user_type_id       string,
net_type_id        string,
cp_id              string,
broadcast_type_id  string,
term_prod_id       string,
term_version_id    string,
chn_id             string,
city_id            string,
sub_busi_id        string,
company_id         string,
user_id            string,
goods_id           string,
pay_chn_type       string,
start_time         string,
expire_time        string,
period_unit        string,
authorize_period   string,
total_amount       decimal(38,4),
period_amount      decimal(38,4)
)
partitioned by (src_file_day string)
stored as parquet;




insert overwrite table rptdata.fact_ugc_tpp_contract_detail_daily partition (src_file_day='${SRC_FILE_DAY}')
select 
    a.user_type as user_type_id,
    a.network_type as net_type_id,
    nvl(nvl(a.cp_id, b.cp_id), '-998') as cp_id,
    a.use_type as broadcast_type_id,
    a.term_prod_id as term_prod_id,
    a.new_version_id as term_version_id,
    a.chn_id as chn_id,
    a.city_id as city_id,
    a.sub_busi_id as sub_busi_id,
    a.company_id as company_id,
    a.phone_number as user_id,
    a.goods_id as goods_id, 
    c.chn_type as pay_chn_type,
    b.valid_start_time as start_time, 
    b.expire_time as expire_time,
    b.period_unit as period_unit,   --授权时间单位: hour(按次) day month(包周期)
    b.amount as authorize_period,   --授权周期  
    b.unit_price * b.quantity as total_amount,  --合约周期总价       
    case when b.period_unit = 'MONTH' then ((b.unit_price * b.quantity) / cast(b.amount as int)) else 0 end as period_amount --(每个月费用）
from intdata.ugc_order_daily a
join intdata.ugc_order_item_daily b
on a.order_id = b.order_id and b.main = 0 and b.src_file_day='${SRC_FILE_DAY}'
join intdata.ugc_order_paychannel_daily c
on a.order_id = c.order_id and c.chn_type not in (50,58,59,302,304,306) and c.src_file_day='${SRC_FILE_DAY}'
where a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')
and a.order_status in ('5', '9')
and a.src_file_day='${SRC_FILE_DAY}';




--测试
select 
    a.user_type as user_type_id,
    a.network_type as net_type_id,
    nvl(nvl(a.cp_id, b.cp_id), '-998') as cp_id,
    a.use_type as broadcast_type_id,
    a.term_prod_id as term_prod_id,
    a.new_version_id as term_version_id,
    a.chn_id as chn_id,
    a.city_id as city_id,
    a.sub_busi_id as sub_busi_id,
    a.company_id as company_id,
    a.phone_number as user_id,
    a.goods_id as goods_id, 
    c.chn_type as pay_chn_type,
    b.valid_start_time as start_time, 
    b.expire_time as expire_time,
    b.period_unit as period_unit,   --授权时间单位: hour(按次) day month(包周期)
    b.amount as authorize_period,   --授权周期  
    b.unit_price * b.quantity as total_amount,  --合约周期总价       
    case when b.period_unit = 'MONTH' then ((b.unit_price * b.quantity) / cast(b.amount as int)) else 0 end as period_amount --(每个月费用）
from intdata.ugc_order_daily a
join intdata.ugc_order_item_daily b
on a.order_id = b.order_id and b.main = 0 and b.src_file_day='20170519'
join intdata.ugc_order_paychannel_daily c
on a.order_id = c.order_id and c.chn_type not in (50,58,59,302,304,306) and c.src_file_day='20170519'
where a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')
and a.order_status in ('5', '9')
and a.src_file_day='20170519';
