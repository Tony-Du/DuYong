
create table app.user_busi_stat_sx0290_monthly (      --用户使用月汇总
phone_number   string,             --手机号码
city_name      string,             --地市
busi_prod_name string,             --业务产品名称，从rpt.dim_term_prod_v取
info_fee       decimal(38,4),      --信息费
login_cnt      int,                --登录频次:访问天数
duration_min   decimal(38,4),      --播放时长
flow_mb        decimal(38,4)       --播放流量
)
partitioned by (src_file_month string)
row format delimited fields terminated by '31'
stored as textfile;