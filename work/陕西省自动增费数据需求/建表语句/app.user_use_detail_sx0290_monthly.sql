
create table app.user_use_detail_sx0290_monthly (      --用户使用月明细
phone_number   string,             --手机号码
use_begin_time string,             --使用时间
busi_prod_name string,             --业务产品名称
content_id     string,
content_name   string,             --播放内容
duration_min   decimal(38,4),      --播放时长
flow_mb        decimal(38,4)       --播放流量
)
partitioned by (src_file_month string)
row format delimited fields terminated by '31'
stored as textfile;