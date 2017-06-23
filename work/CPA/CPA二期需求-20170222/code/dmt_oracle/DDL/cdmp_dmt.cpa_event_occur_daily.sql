drop index uidx_cpa_event_occur_d;

drop table cpa_event_occur_daily cascade constraints;

/*==============================================================*/
/* Table: cpa_event_occur_daily                               */
/*==============================================================*/
create table cpa_event_occur_daily 
(
   src_file_day         char(8),
   product_key          number(4),
   product_name         nvarchar2(12),
   app_ver_code         nvarchar2(12),
   app_channel_id       nvarchar2(20),
   event_name           nvarchar2(32),
   event_cnt            number(32),
   sum_du               number(28,2),
   avg_du               number(28,2),
   flow_time            date default sysdate
)
nologging
partition by range (src_file_day)
  (partition p_max values less than (maxvalue));
  
comment on table cpa_event_occur_daily is
'kesheng事件发生统计(日)';


comment on column cpa_event_occur_daily.src_file_day is
'数据周期(天)';

comment on column cpa_event_occur_daily.product_key is
'产品KEY';

comment on column cpa_event_occur_daily.product_name is
'产品名称';

comment on column cpa_event_occur_daily.app_ver_code is
'客户端版本';

comment on column cpa_event_occur_daily.app_channel_id is
'推广渠道';

comment on column cpa_event_occur_daily.event_name is
'事件名称';

comment on column cpa_event_occur_daily.event_cnt is
'事件发生次数';

comment on column cpa_event_occur_daily.sum_du is
'计算型参数值和';

comment on column cpa_event_occur_daily.avg_du is
'计算型参数值均值';

comment on column cpa_event_params_daily.flow_time is
'数据流入时间';


/*==============================================================*/
/* Index: uidx_cpa_event_occur_d                                */
/*==============================================================*/
create unique index uidx_cpa_event_occur_d on cpa_event_occur_daily (
   src_file_day asc,
   app_channel_id asc,
   event_name asc,
   product_name asc,
   app_ver_code asc
) local;



/*

1、 varchar：
可变长度的非 Unicode 数据，最长为 8,000 个字符
2、nvarchar：
可变长度 Unicode 数据，其最大长度为 4,000 字符
3、char:
固定长度的非 Unicode 字符数据，最大长度为 8,000 个字符
4、nchar
固定长度的 Unicode 数据，最大长度为 4,000 个字符

*/