alter table rptdata.cdmp_rpt_cnt_daily rename to bkp.cdmp_rpt_cnt_daily_20170525;
-- drop table rptdata.cdmp_rpt_cnt_daily;
-- alter table temp.cdmp_rpt_cnt_daily_20170525 rename to rptdata.cdmp_rpt_cnt_daily;

CREATE  TABLE rptdata.cdmp_rpt_cnt_daily (
  dept_id                           string,
  term_prod_id                      string,
  term_video_type_id                string,
  term_video_soft_id                string,
  term_prod_type_id                 string,
  term_version_id                   string,
  term_os_type_id                   string,
  busi_id                           string,
  sub_busi_id                       string,
  net_type_id                       string,
  cp_id                             string,
  product_id                        string,
  content_id                        string,
  content_type                      string,
  broadcast_type_id                 string,
  user_type_id                      string,
  province_id                       string,
  city_id                           string,
  phone_province_id                 string,
  phone_city_id                     string,
  company_id                        string,
  chn_id                            string,
  chn_attr_1_id                     string,
  chn_attr_2_id                     string,
  chn_attr_3_id                     string,
  chn_attr_4_id                     string,
  chn_type                          string,
  con_class_1_name                  string,
  copyright_type                    string,
  busi_type_id                      string,
  program_id                        string,
  visit_cnt                         bigint,
  use_cnt                           bigint,
  use_flow_kb                       bigint,
  use_flow_kb_i                     bigint,
  use_flow_kb_e                     bigint,
  use_duration                      bigint,
  month_use_duration                bigint,
  month_use_cnt                     bigint,
  time_use_cnt                      bigint,
  time_use_duration                 bigint,
  boss_month_add_info_fee_amount    decimal(38,4),	--boss包月新增信息费收入 指标11
  boss_month_retention_info_amount  decimal(38,4), 	--boss包月存量信息费收入 指标9
  boss_time_amount                  decimal(38,4), 
  boss_month_info_amount            decimal(38,4), 	--boss包月信息费收入 指标8
  boss_time_info_amount             decimal(38,4), 	--boss按次信息费收入 指标2
  third_prepay_amount               decimal(38,4), 
  real_amount                       decimal(38,4), 	--第三方实际收入
  total_amount                      decimal(38,4), 	--(boss+第三方)总收入

  -- 新增字段
  --tpp_real_amount                   decimal(38,4),	--第三方实际收入		注意！！！！！！！！！！
  tpp_add_period_amount             decimal(38,4),	--第三方新增包周期收入
  tpp_add_time_amount               decimal(38,4),	--第三方新增按次收入
  tpp_subside_amount                decimal(38,4),	--第三方沉淀收入
  sales_amount                      decimal(38,4),	--销售收入

  dim_group_id                      bigint
)
PARTITIONED BY (src_file_day string)
STORED AS parquet ;

--------------------------------------------------------------------------------------------------------

set hive.merge.mapredfiles = true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

insert overwrite table rptdata.cdmp_rpt_cnt_daily partition (src_file_day)
select 
    dept_id,
    term_prod_id,
    term_video_type_id,
    term_video_soft_id,
    term_prod_type_id,
    term_version_id,
    term_os_type_id,
    busi_id,
    sub_busi_id,
    net_type_id,
    cp_id,
    product_id,
    content_id,
    content_type,
    broadcast_type_id,
    user_type_id,
    province_id,
    city_id,
    phone_province_id,
    phone_city_id,
    company_id,
    chn_id,
    chn_attr_1_id,
    chn_attr_2_id,
    chn_attr_3_id,
    chn_attr_4_id,
    chn_type,
    con_class_1_name,
    copyright_type,
    busi_type_id,
    program_id,
    visit_cnt,
    use_cnt,
    use_flow_kb,
    use_flow_kb_i,
    use_flow_kb_e,
    use_duration,
    month_use_duration,
    month_use_cnt,
    time_use_cnt,
    time_use_duration,
    boss_month_add_info_fee_amount,
    boss_month_retention_info_amount,
    boss_time_amount,
    boss_month_info_amount,
    boss_time_info_amount,
    third_prepay_amount,
    real_amount,
    total_amount,

    0 tpp_add_period_amount,
    0 tpp_add_time_amount,
    0 sales_amount,

    dim_group_id,
    src_file_day
from bkp.cdmp_rpt_cnt_daily_20170525 t ;