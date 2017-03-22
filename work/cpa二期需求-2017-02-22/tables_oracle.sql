
-- oracle中的表
create table app.cpa_sec_event_occur_daily (
product_key varchar2(200)  
product_name varchar2(200) 
app_ver_code varchar2(200) 
app_channel_id varchar2(200)
event_name varchar2(200)
event_cnt number
sum_du number(38,2)
avg_du number(38,2)
src_file_day as (to_date(src_file_day,'YYYYMMDD')) 
)
partition by range (src_file_day) interval (numtodsinterval(1, 'day'))
(PARTITION P20170320 VALUES LESS THAN (TO_DATE('20170321','YYYYMMDD')));


create table app.cpa_sec_event_params_daily (
product_key varchar2(200)  
product_name varchar2(200) 
app_ver_code varchar2(200) 
app_channel_id varchar2(200)
event_name varchar2(200)
param_name varchar2(200)
param_val  varchar2(200)
val_cnt number
val_pct number(38,2)
src_file_day as (to_date(src_file_day,'YYYYMMDD')) 
)
partition by range (src_file_day) interval (numtodsinterval(1, 'day'))
(PARTITION P20170320 VALUES LESS THAN (TO_DATE('20170321','YYYYMMDD')));


create table app.cpa_sec_event_params_last7_daily (
product_key varchar2(200)  
product_name varchar2(200) 
app_ver_code varchar2(200) 
app_channel_id varchar2(200)
event_name varchar2(200)
param_name varchar2(200)
param_val  varchar2(200)
val_cnt number
val_pct number(38,2)
src_file_day as (to_date(src_file_day,'YYYYMMDD')) 
)
partition by range (src_file_day) interval (numtodsinterval(7, 'day'))
(PARTITION P20170320 VALUES LESS THAN (TO_DATE('20170321','YYYYMMDD')));


create table app.cpa_sec_event_params_last7_daily (
product_key varchar2(200)  
product_name varchar2(200) 
app_ver_code varchar2(200) 
app_channel_id varchar2(200)
event_name varchar2(200)
param_name varchar2(200)
param_val  varchar2(200)
val_cnt number
val_pct number(38,2)
src_file_day as (to_date(src_file_day,'YYYYMMDD')) 
)
partition by range (src_file_day) interval (numtodsinterval(30, 'day'))
(PARTITION P20170320 VALUES LESS THAN (TO_DATE('20170321','YYYYMMDD'))); -- 20170320近30天的信息





