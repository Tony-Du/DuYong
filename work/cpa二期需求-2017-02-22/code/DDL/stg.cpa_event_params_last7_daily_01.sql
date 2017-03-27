
drop table if exists stg.cpa_event_params_last7_daily_01;


create table stg.cpa_event_params_last7_daily_01 (
app_channel_id string,
product_key    string,
app_ver_code   string,
event_name     string,
param_name     string,
param_val      string,
val_cnt		   bigint
);