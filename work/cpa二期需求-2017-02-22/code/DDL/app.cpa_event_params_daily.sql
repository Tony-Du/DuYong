drop table if exists app.cpa_event_params_daily;

create table app.cpa_event_params_daily(
product_key     string,
product_name    string,
app_ver_code    string,
app_channel_id  string,
event_name      string,
param_name      string,
param_val       string,
val_cnt         bigint,
val_pct			decimal(8,4)
)
partitioned by (src_file_day string);

/*+------------------------------------------------------
-- 同步到oracle的目标表
drop table cdmp_dmt.cpa_event_params_daily;

create table cdmp_dmt.cpa_event_params_daily
(
   src_file_day         char(8),
   product_key          varchar2(4),
   product_name         varchar2(12),
   app_ver_code         varchar2(12),
   app_channel_id       varchar2(20),
   event_name           varchar2(32),
   param_name           varchar2(32),
   param_val            varchar2(64),
   val_cnt              number(32),
   val_pct              number(8,4)
)
nologging
partition by range (src_file_day)
  (partition p_max values less than (maxvalue));

create index idx_cpa_event_params_d on cdmp_dmt.cpa_event_params_daily
(
   src_file_day,
   app_channel_id,
   event_name,
   product_name,
   app_ver_code
);

------------------------------------------------------+*/