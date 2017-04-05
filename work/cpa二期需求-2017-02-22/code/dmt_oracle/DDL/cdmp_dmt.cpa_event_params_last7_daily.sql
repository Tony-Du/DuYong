drop index idx_cpa_event_params_last7_d;

drop table cpa_event_params_last7_daily cascade constraints;

/*==============================================================*/
/* Table: cpa_event_params_last7_daily                        */
/*==============================================================*/
create table cpa_event_params_last7_daily 
(
   src_file_day         char(8),
   product_key          number(4),
   product_name         nvarchar2(12),
   app_ver_code         nvarchar2(12),
   app_channel_id       nvarchar2(20),
   event_name           nvarchar2(32),
   param_name           nvarchar2(32),
   param_val            nvarchar2(64),
   val_cnt              number(32),
   val_pct              number(8,4),
   flow_time            date default sysdate
)
nologging
partition by range (src_file_day)
  (partition p_max values less than (maxvalue));
  
comment on table cpa_event_params_last7_daily is
'kesheng事件参数最近7天统计(日)';

comment on column cpa_event_params_last7_daily.src_file_day is
'数据周期(天)';

comment on column cpa_event_params_last7_daily.product_key is
'产品KEY';

comment on column cpa_event_params_last7_daily.product_name is
'产品名称';

comment on column cpa_event_params_last7_daily.app_ver_code is
'客户端版本';

comment on column cpa_event_params_last7_daily.app_channel_id is
'推广渠道';

comment on column cpa_event_params_last7_daily.event_name is
'事件名称';

comment on column cpa_event_params_last7_daily.param_name is
'事件参数名称';

comment on column cpa_event_params_last7_daily.param_val is
'事件参数值';

comment on column cpa_event_params_last7_daily.val_cnt is
'参数值出现次数';

comment on column cpa_event_params_last7_daily.val_pct is
'参数值出现次数占比';

comment on column cpa_event_params_daily.flow_time is
'数据流入时间';


/*==============================================================*/
/* Index: idx_cpa_event_params_last7_d                          */
/*==============================================================*/
create index idx_cpa_event_params_last7_d on cpa_event_params_last7_daily (
   src_file_day asc,
   app_channel_id asc,
   event_name asc,
   product_name asc,
   app_ver_code asc
)
local;
