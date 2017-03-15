drop table if exists app.cpa_sec_event_params_daily;

create table app.cpa_sec_event_params_daily(
product_key     string,
product_name    string,
app_ver_code    string,
app_channel_id  string,
event_name      string,
param_name      string,
param_val       string,
val_cnt         bigint,
val_pct			decmal(8,4)
)
partitioned by (src_file_day string);