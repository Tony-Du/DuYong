
drop table if exists app.cpa_sec_event_occur_daily;

create table app.cpa_sec_event_occur_daily(
product_key      string,
product_name     string,
app_ver_code     string,
app_channel_id   string,
event_name       string,
event_cnt        bigint,
sum_du           decimal(20,2),
avg_du           decimal(20,2)
) partitioned by (src_file_day string);