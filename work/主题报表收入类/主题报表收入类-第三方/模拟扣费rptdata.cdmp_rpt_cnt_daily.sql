+-----------------------------------+-----------------------+-----------------------+--+
|             col_name              |       data_type       |        comment        |
+-----------------------------------+-----------------------+-----------------------+--+
| dept_id                           | string                |                       |
| term_prod_id                      | string                |                       |
| term_video_type_id                | string                |                       |
| term_video_soft_id                | string                |                       |
| term_prod_type_id                 | string                |                       |
| term_version_id                   | string                |                       |
| term_os_type_id                   | string                |                       |
| busi_id                           | string                |                       |
| sub_busi_id                       | string                |                       |
| net_type_id                       | string                |                       |
| cp_id                             | string                |                       |
| product_id                        | string                |                       |
| content_id                        | string                |                       |
| content_type                      | string                |                       |
| broadcast_type_id                 | string                |                       |
| user_type_id                      | string                |                       |
| province_id                       | string                |                       |
| city_id                           | string                |                       |
| phone_province_id                 | string                |                       |
| phone_city_id                     | string                |                       |
| company_id                        | string                |                       |
| chn_id                            | string                |                       |
| chn_attr_1_id                     | string                |                       |
| chn_attr_2_id                     | string                |                       |
| chn_attr_3_id                     | string                |                       |
| chn_attr_4_id                     | string                |                       |
| chn_type                          | string                |                       |
| con_class_1_name                  | string                |                       |
| copyright_type                    | string                |                       |
| busi_type_id                      | string                |                       |
| program_id                        | string                |                       |
| visit_cnt                         | bigint                |                       |
| use_cnt                           | bigint                |                       |
| use_flow_kb                       | bigint                |                       |
| use_flow_kb_i                     | bigint                |                       |
| use_flow_kb_e                     | bigint                |                       |
| use_duration                      | bigint                |                       |
| month_use_duration                | bigint                |                       |
| month_use_cnt                     | bigint                |                       |
| time_use_cnt                      | bigint                |                       |
| time_use_duration                 | bigint                |                       |
| boss_month_add_info_fee_amount    | decimal(38,4)         |                       |
| boss_month_retention_info_amount  | decimal(38,4)         |                       |
| boss_time_amount                  | decimal(38,4)         |                       |
| boss_month_info_amount            | decimal(38,4)         |                       |
| boss_time_info_amount             | decimal(38,4)         |                       |
| third_prepay_amount               | decimal(38,4)         |                       |
| real_amount                       | decimal(38,4)         |                       |
| total_amount                      | decimal(38,4)         |                       |
| dim_group_id                      | bigint                |                       |
| src_file_day                      | string                |                       |
|                                   | NULL                  | NULL                  |
| # Partition Information           | NULL                  | NULL                  |
| # col_name                        | data_type             | comment               |
|                                   | NULL                  | NULL                  |
| src_file_day                      | string                |                       |
+-----------------------------------+-----------------------+-----------------------+--+


set mapreduce.job.name=rptdata_cdmp_rpt_cnt_daily_new_${SRC_FILE_DAY};
--set hive.groupby.skewindata=true;
--set hive.exec.dynamic.partition.mode=nonstrict;
--set hive.exec.dynamic.partition=true;
--set hive.optimize.skewjoin=true;
set hive.merge.mapredfiles=true;


with cdmp_cnt_src as (

select
     nvl(term_prod_id,'-998') as term_prod_id,
     nvl(city_id,'-998') as city_id,
     nvl(phone_city_id,'-998') as phone_city_id,
     nvl(net_type_id,'-998') as net_type_id,
     nvl(sub_busi_id,'-998') as sub_busi_id,
     '-998' as product_id,
     content_id,
     '-998' as broadcast_type_id,
     chn_id,
     nvl(user_type_id, '-998') as user_type_id,
     term_version_id,
     program_id,
     visit_cnt,
     0 as use_cnt,
     0 as use_flow_kb,
     0 as use_flow_kb_i,
     0 as use_flow_kb_e,
     0 as use_duration,
     0 as month_use_duration,
     0 as month_use_cnt,
     0 as time_use_cnt,
     0 as time_use_duration,
     0 as boss_month_add_info_fee_amount,
     0 as boss_month_retention_info_amount,
     0 as boss_time_amount,
     0 as boss_month_info_amount,
     0 as boss_time_info_amount,
     0 as third_prepay_Amount,
     0 as real_amount,
     0 as total_amount
from rptdata.fact_user_visit_cnt_daily
where src_file_day = '${SRC_FILE_DAY}'

union all

select
     nvl(term_prod_id,'-998') as term_prod_id,
     nvl(city_id,'-998') as city_id,
     nvl(city_id,'-998') as phone_city_id,
     nvl(network_type,'-998') as net_type_id,
     nvl(sub_busi_id,'-998') as sub_busi_id,
     '-998' as product_id,
     '-998' as content_id,
     nvl(use_type,'-998') as broadcast_type_id,
     chn_id,
     nvl(user_type,'-998') as user_type_id,
     term_version_id,
     program_id,
     0 as visit_cnt,
     0 as use_cnt,
     0 as use_flow_kb,
     0 as use_flow_kb_i,
     0 as use_flow_kb_e,
     0 as use_duration,
     0 as month_use_duration,
     0 as month_use_cnt,
     0 as time_use_cnt,
     0 as time_use_duration,
     0 as boss_month_add_info_fee_amount,
     0 as boss_month_retention_info_amount,
     case when (a.period_unit = 'HOUR' and a.pay_chn_type in (50,58,59,302,304,306) and a.order_status = 5 and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE') and a.main = 0) then a.amount else 0 end as boss_time_amount,
     0 as boss_month_info_amount,
     case when (a.period_unit = 'HOUR' and a.pay_chn_type in (50,58,59,302,304,306) and a.order_status = 5 and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE') and a.main = 0) then a.amount else 0 end as boss_time_info_amount,
     case when (a.pay_chn_type not in (50,58,59,302,304,306) and a.order_status = 5 and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE') and a.main = 0) then a.amount else 0 end as third_prepay_Amount,
     0 as real_amount,
     0 as total_amount
from rptdata.fact_ugc_order_amount_daily a
where src_file_day = '${SRC_FILE_DAY}'

union all

select
     nvl(term_prod_id,'-998') as term_prod_id,
     nvl(city_id,'-998') as city_id,
     nvl(city_id,'-998') as phone_city_id,
     nvl(network_type,'-998') as net_type_id,
     nvl(sub_busi_id,'-998') as sub_busi_id,
     '-998' as product_id,
     '-998' as content_id,
     nvl(use_type,'-998') as broadcast_type_id,
     chn_id,
     nvl(user_type,'-998') as user_type_id,
     term_version_id,
     '-998' as program_id,
     0 as visit_cnt,
     0 as use_cnt,
     0 as use_flow_kb,
     0 as use_flow_kb_i,
     0 as use_flow_kb_e,
     0 as use_duration,
     0 as month_use_duration,
     0 as month_use_cnt,
     0 as time_use_cnt,
     0 as time_use_duration,
     case when (src_file_day='${SRC_FILE_DAY}' and is_add = '1') then amount else 0 end as boss_month_add_info_fee_amount,
     case when (is_add = '0') then amount else 0 end as boss_month_retention_info_amount,
     0 as boss_time_amount,
     case when (src_file_day='${SRC_FILE_DAY}') then amount else 0 end as boss_month_info_amount,
     0 as boss_time_info_amount,
     0 as third_prepay_Amount,
     0 as real_amount,
     0 as total_amount
from rptdata.fact_user_month_fee_daily a
where src_file_day>='${SRC_FILE_MONTH}01' and src_file_day <= '${SRC_FILE_DAY}'

--union all

--select
--nvl(term_prod_id,'-998') as term_prod_id,
--nvl(city_id,'-998') as city_id,
--nvl(city_id,'-998') as phone_city_id,
--nvl(net_type_id,'-998') as net_type_id,
--nvl(sub_busi_id,'-998') as sub_busi_id,
--'-998' as product_id,
--'-998' as content_id,
--nvl(broadcast_type_id,'-998') as broadcast_type_id,
--chn_id,
--nvl(user_type_id,'-998') as user_type_id,
--term_version_id,
--program_id,
--0 as visit_cnt,
--0 as use_cnt,
--0 as use_flow_kb,
--0 as use_flow_kb_i,
--0 as use_flow_kb_e,
--0 as use_duration,
--0 as month_use_duration,
--0 as month_use_cnt,
--0 as time_use_cnt,
--0 as time_use_duration,
--0 as boss_month_add_info_fee_amount,
--case when (a.chrgprod_price > 0 and a.order_status <> '4' and a.sub_status is null) then a.chrgprod_price else 0 end as boss_month_retention_info_amount,
--0 as boss_time_amount,
--case when (a.chrgprod_price > 0 and a.order_status <> '4' and a.sub_status is null) then a.chrgprod_price else 0 end as boss_month_info_amount,
--0 as boss_time_info_amount,
--0 as third_prepay_Amount,
--0 as real_amount,
--0 as total_amount
--from rptdata.fact_ugc_order_relation_detail_daily a
--where src_file_day = '${SRC_FILE_DAY}'	

union all

select
     term_prod_id,
     nvl(city_id,'-998') as city_id,
     nvl(phone_city_id,'-998') as phone_city_id,
     net_type_id,
     sub_busi_id,
     product_id,
     content_id,
     broadcast_type_id,
     chn_id,
     user_type_id, 
     term_version_id,
     program_id,
     0 as visit_cnt,
     use_cnt,
     use_flow_kb,
     case when net_type_id in (1,2,5) then use_flow_kb else 0 end as use_flow_kb_i,
     case when net_type_id in (1,2,5) then 0 else use_flow_kb end as use_flow_kb_e,
     use_duration,
     month_use_duration,
     month_use_cnt,
     time_use_cnt,
     time_use_duration,
     0 as boss_month_add_info_fee_amount,
     0 as boss_month_retention_info_amount,
     0 as boss_time_amount,
     0 as boss_month_info_amount,
     0 as boss_time_info_amount,
     0 as third_prepay_Amount,
     0 as real_amount,
     0 as total_amount
from rptdata.fact_user_use_count_daily
where src_file_day = '${SRC_FILE_DAY}'
)

insert overwrite table rptdata.cdmp_rpt_cnt_daily partition(src_file_day='${SRC_FILE_DAY}')
select
nvl(dept_id           ,'-999') as dept_id            ,
nvl(term_prod_id      ,'-999') as term_prod_id       ,
nvl(term_video_type_id,'-999') as term_video_type_id ,
nvl(term_video_soft_id,'-999') as term_video_soft_id ,
nvl(term_prod_type_id ,'-999') as term_prod_type_id  ,
nvl(term_version_id   ,'-999') as term_version_id    , 
nvl(term_os_type_id   ,'-999') as term_os_type_id    ,
nvl(busi_id           ,'-999') as busi_id            ,
nvl(sub_busi_id       ,'-999') as sub_busi_id        ,
nvl(net_type_id       ,'-999') as net_type_id        ,
nvl(cp_id             ,'-999') as cp_id              ,
nvl(product_id        ,'-999') as product_id         ,
nvl(content_id        ,'-999') as content_id         ,
nvl(content_type      ,'-999') as content_type       ,
nvl(broadcast_type_id ,'-999') as broadcast_type_id  ,
nvl(user_type_id      ,'-999') as user_type_id       ,
nvl(province_id       ,'-999') as province_id        ,
nvl(city_id           ,'-999') as city_id            ,
nvl(phone_province_id ,'-999') as phone_province_id  ,
nvl(phone_city_id     ,'-999') as phone_city_id      ,
nvl(company_id        ,'-999') as company_id         ,
nvl(chn_id            ,'-999') as chn_id             ,
nvl(chn_attr_1_id     ,'-999') as chn_attr_1_id      ,
nvl(chn_attr_2_id     ,'-999') as chn_attr_2_id      ,
nvl(chn_attr_3_id     ,'-999') as chn_attr_3_id      ,
nvl(chn_attr_4_id     ,'-999') as chn_attr_4_id      ,
nvl(chn_type          ,'-999') as chn_type           ,
nvl(con_class_1_name  ,'-999') as con_class_1_name   ,
nvl(copyright_type    ,'-999') as copyright_type     ,
nvl(busi_type_id      ,'-999') as busi_type_id,
nvl(program_id        ,'-999') as program_id,
sum(visit_cnt) as visit_cnt,
sum(use_cnt) as use_cnt,
sum(use_flow_kb) as use_flow_kb,
sum(use_flow_kb_i) as use_flow_kb_i,
sum(use_flow_kb_e) as use_flow_kb_e,
sum(use_duration) as use_duration,
sum(month_use_duration) as month_use_duration,
sum(month_use_cnt) as month_use_cnt,
sum(time_use_cnt) as time_use_cnt,
sum(time_use_duration) as time_use_duration,
sum(boss_month_add_info_fee_amount) as boss_month_add_info_fee_amount,
sum(boss_month_retention_info_amount) as boss_month_retention_info_amount,
sum(boss_time_amount) as boss_time_amount,
sum(boss_month_info_amount) as boss_month_info_amount,
sum(boss_time_info_amount) as boss_time_info_amount,
sum(third_prepay_Amount) as third_prepay_Amount,
sum(real_amount) as real_amount,
--sum(total_amount) as total_amount,
sum(boss_time_amount + boss_month_info_amount + real_amount) as total_amount,
grouping__id
from
(
select
      CASE WHEN t2.term_prod_name IN ('和视频OPENAPI','和视频SDK') THEN nvl(t9.dept_id,'-998') ELSE nvl(t7.dept_id,nvl(t8.dept_id,nvl(t9.dept_id,'-998'))) END as dept_id,
      nvl(t1.term_prod_id,'-998') as term_prod_id, 
      case when nvl(t2.term_video_type_id,'null') ='null' then '-998' else t2.term_video_type_id end as term_video_type_id,
      case when nvl(t2.term_video_soft_id,'null') ='null' then '-998' else t2.term_video_soft_id end as term_video_soft_id,
      case when nvl(t2.term_prod_type_id,'null') ='null' then '-998' else t2.term_prod_type_id end as term_prod_type_id,
      t1.term_version_id as term_version_id,
      case when nvl(t2.term_os_type_id,'null') ='null' then '-998' else t2.term_os_type_id end as term_os_type_id,
      case when nvl(t4.business_id,'null') ='null' then '-998' else t4.business_id end as busi_id,
      nvl(t1.sub_busi_id,'-998') as sub_busi_id,
      nvl(t1.net_type_id,'-998') as net_type_id,
      nvl(t13.ncp_id,'-998') as cp_id,
      t1.product_id as product_id,
      nvl(t12.content_id,'-998') as content_id,
      '-998' as content_type,
      t1.broadcast_type_id as broadcast_type_id,
      nvl(t1.user_type_id, '-998') as user_type_id,
      case when nvl(t3.prov_id,'null') ='null' then '-998' else t3.prov_id end as province_id,
      nvl(t1.city_id,'-998') as city_id,
      case when nvl(t10.prov_id,'null') ='null' then '-998' else t10.prov_id end as phone_province_id,
      nvl(t1.phone_city_id,'-998') as phone_city_id,
      case when nvl(t6.comp_id,'null') ='null' then '-998' else t6.comp_id end as company_id,
      nvl(t5.chn_id,'-998') as chn_id,
      case when nvl(t5.chn_attr_1_id,'null') ='null' then '-998' else t5.chn_attr_1_id end as chn_attr_1_id,
      case when nvl(t5.chn_attr_2_id,'null') ='null' then '-998' else t5.chn_attr_2_id end as chn_attr_2_id,
      case when nvl(t5.chn_attr_3_id,'null') ='null' then '-998' else t5.chn_attr_3_id end as chn_attr_3_id,
      case when nvl(t5.chn_attr_4_id,'null') ='null' then '-998' else t5.chn_attr_4_id end as chn_attr_4_id,
      case when nvl(t5.chn_type,'null') ='null' then '-998' else t5.chn_type end as chn_type,
      case when nvl(t11.con_class_1_name,'null') ='null' then '-998' else t11.con_class_1_name end as con_class_1_name,
      case when nvl(t13.copyright_type,'null') ='null' then '-998' else t13.copyright_type end as copyright_type,
      case when nvl(t4.b_type_id,'null') ='null' then '-998' else t4.b_type_id end as busi_type_id,
      t1.program_id as program_id,
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
      third_prepay_Amount,
      real_amount,
      total_amount
from  cdmp_cnt_src t1
left join rpt.dim_term_prod_v t2 on t1.term_prod_id = t2.term_prod_id
left join rptdata.dim_region t3    on t1.city_id = t3.region_id
left join rptdata.dim_server t4    on t1.sub_busi_id = t4.sub_busi_id
left join rptdata.dim_chn t5       on t1.chn_id = t5.chn_id
left join rptdata.dim_comp_busi t6      on t1.sub_busi_id = t6.sub_busi_id
left join rptdata.dim_dept_term_prod t7 on t1.term_prod_id = t7.term_prod_id
left join rptdata.dim_dept_busi t8 on t4.business_id = t8.business_id
left join rptdata.dim_dept_chn  t9 on t1.chn_id = t9.chn_id
left join rptdata.dim_region t10    on t1.phone_city_id = t10.region_id
left join rptdata.dim_content_class t11 
on (case when t1.content_id = '-998' then concat('',rand()) else t1.content_id end) = t11.content_id
left join rptdata.dim_video_content t12 
on (case when t1.content_id = '-998' then concat('',rand()) else t1.content_id end) = t12.content_id
left join rptdata.dim_content_copyright t13 
on t12.copyright_id = t13.copyright_id
) tt
group by dept_id,term_prod_id,term_video_type_id,term_video_soft_id,term_prod_type_id,
		term_version_id,term_os_type_id,busi_id,sub_busi_id,net_type_id,cp_id,
		product_id,content_id,content_type,broadcast_type_id,user_type_id,
		province_id,city_id,phone_province_id,phone_city_id,company_id,
		chn_id,chn_attr_1_id,chn_attr_2_id,chn_attr_3_id,chn_attr_4_id,chn_type,
		con_class_1_name, copyright_type, busi_type_id, program_id
grouping sets (
-- 核心
(dept_id), 
(dept_id, term_video_type_id),
(dept_id, term_video_type_id, term_prod_id),
(term_video_type_id),

-- 终端
(term_video_type_id, user_type_id),
(net_type_id, term_video_type_id),
(broadcast_type_id, term_video_type_id),
(busi_id, term_video_type_id),
(term_video_type_id, term_prod_id),
(term_video_type_id, term_prod_id, user_type_id),
(term_prod_type_id),
(term_video_soft_id),
(term_video_type_id, term_prod_id, term_version_id, user_type_id),
(term_video_type_id, term_prod_id, term_version_id),

-- 地域
(phone_province_id),
(phone_province_id, phone_city_id),
(phone_province_id, term_video_type_id),
(phone_province_id, phone_city_id, term_video_type_id),
(phone_province_id, term_prod_type_id),
(phone_province_id, phone_city_id, term_prod_type_id),
(phone_province_id, net_type_id),
(phone_province_id, phone_city_id, net_type_id),
(phone_province_id, busi_id),
(phone_province_id, busi_id, sub_busi_id),
(phone_province_id, phone_city_id, busi_id),
(phone_province_id, phone_city_id, busi_id, sub_busi_id),
(phone_province_id, broadcast_type_id, net_type_id),
(phone_province_id, phone_city_id, broadcast_type_id, net_type_id),
(phone_province_id, term_video_type_id, net_type_id),
(phone_province_id, phone_city_id, term_video_type_id, net_type_id),
(phone_province_id, busi_id, net_type_id),
(phone_province_id, phone_city_id, busi_id, net_type_id),

--渠道
(term_video_type_id, user_type_id, chn_type),
(term_video_type_id, user_type_id, chn_attr_1_id, chn_attr_2_id),
(term_video_type_id, user_type_id, chn_attr_1_id),
(user_type_id, province_id, city_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type),
(user_type_id, province_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type),
(term_video_type_id, user_type_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type),
(term_prod_id, term_video_type_id, user_type_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type),
(term_prod_id, term_video_type_id, user_type_id, chn_type),
(user_type_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type),
(busi_id, user_type_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type),
(busi_id, user_type_id, province_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type),
(busi_id, user_type_id, province_id, city_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type),
(term_prod_id, user_type_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type),
(term_prod_id, busi_id, user_type_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type),
(term_prod_id, busi_id, user_type_id, province_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type),
(term_prod_id, user_type_id, province_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type),
(term_prod_id, busi_id, user_type_id, province_id, city_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type),
(term_prod_id, user_type_id, province_id, city_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type),
(user_type_id, province_id, chn_attr_1_id, chn_attr_2_id),
(user_type_id, chn_attr_1_id, chn_attr_2_id),
(user_type_id, province_id, chn_attr_1_id),
(user_type_id, chn_attr_1_id),
(busi_id, user_type_id, chn_attr_1_id, chn_attr_2_id),
(busi_id, user_type_id, chn_attr_1_id),
(busi_id, user_type_id, chn_type),
(sub_busi_id, user_type_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type),
(sub_busi_id, user_type_id, province_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type),

--内容引入
(term_video_type_id, term_prod_type_id, cp_id, product_id, content_id, con_class_1_name, copyright_type),
(term_video_type_id, term_prod_type_id, cp_id, content_id, con_class_1_name, copyright_type),
(cp_id),
(cp_id, con_class_1_name),
(con_class_1_name),
(cp_id, copyright_type),
(cp_id, product_id)
)

union all

select
nvl(dept_id           ,'-999') as dept_id            ,
nvl(term_prod_id      ,'-999') as term_prod_id       ,
nvl(term_video_type_id,'-999') as term_video_type_id ,
nvl(term_video_soft_id,'-999') as term_video_soft_id ,
nvl(term_prod_type_id ,'-999') as term_prod_type_id  ,
nvl(term_version_id   ,'-999') as term_version_id    , 
nvl(term_os_type_id   ,'-999') as term_os_type_id    ,
nvl(busi_id           ,'-999') as busi_id            ,
nvl(sub_busi_id       ,'-999') as sub_busi_id        ,
nvl(net_type_id       ,'-999') as net_type_id        ,
nvl(cp_id             ,'-999') as cp_id              ,
nvl(product_id        ,'-999') as product_id         ,
nvl(content_id        ,'-999') as content_id         ,
nvl(content_type      ,'-999') as content_type       ,
nvl(broadcast_type_id ,'-999') as broadcast_type_id  ,
nvl(user_type_id      ,'-999') as user_type_id       ,
nvl(province_id       ,'-999') as province_id        ,
nvl(city_id           ,'-999') as city_id            ,
nvl(phone_province_id ,'-999') as phone_province_id  ,
nvl(phone_city_id     ,'-999') as phone_city_id      ,
nvl(company_id        ,'-999') as company_id         ,
nvl(chn_id            ,'-999') as chn_id             ,
nvl(chn_attr_1_id     ,'-999') as chn_attr_1_id      ,
nvl(chn_attr_2_id     ,'-999') as chn_attr_2_id      ,
nvl(chn_attr_3_id     ,'-999') as chn_attr_3_id      ,
nvl(chn_attr_4_id     ,'-999') as chn_attr_4_id      ,
nvl(chn_type          ,'-999') as chn_type           ,
nvl(con_class_1_name  ,'-999') as con_class_1_name   ,
nvl(copyright_type    ,'-999') as copyright_type     ,
nvl(busi_type_id      ,'-999') as busi_type_id,
nvl(program_id        ,'-999') as program_id,
sum(case when mobile_city_id <> '-998' then visit_cnt else 0 end) as visit_cnt,
sum(case when mobile_city_id <> '-998' then use_cnt else 0 end) as use_cnt,
sum(case when mobile_city_id <> '-998' then use_flow_kb else 0 end) as use_flow_kb,
sum(case when mobile_city_id <> '-998' then use_flow_kb_i else 0 end) as use_flow_kb_i,
sum(case when mobile_city_id <> '-998' then use_flow_kb_e else 0 end) as use_flow_kb_e,
sum(case when mobile_city_id <> '-998' then use_duration else 0 end) as use_duration,
sum(case when mobile_city_id <> '-998' then month_use_duration else 0 end) as month_use_duration,
sum(case when mobile_city_id <> '-998' then month_use_cnt else 0 end) as month_use_cnt,
sum(case when mobile_city_id <> '-998' then time_use_cnt else 0 end) as time_use_cnt,
sum(case when mobile_city_id <> '-998' then time_use_duration else 0 end) as time_use_duration,
sum(case when mobile_city_id <> '-998' then boss_month_add_info_fee_amount else 0 end) as boss_month_add_info_fee_amount,
sum(case when mobile_city_id <> '-998' then boss_month_retention_info_amount else 0 end) as boss_month_retention_info_amount,
sum(case when mobile_city_id <> '-998' then boss_time_amount else 0 end) as boss_time_amount,
sum(case when mobile_city_id <> '-998' then boss_month_info_amount else 0 end) as boss_month_info_amount,
sum(case when mobile_city_id <> '-998' then boss_time_info_amount else 0 end) as boss_time_info_amount,
sum(case when mobile_city_id <> '-998' then third_prepay_Amount else 0 end) as third_prepay_Amount,
sum(case when mobile_city_id <> '-998' then real_amount else 0 end) as real_amount,
--sum(total_amount) as total_amount,
sum(case when mobile_city_id <> '-998' then (boss_time_amount + boss_month_info_amount + real_amount) else 0 end) as total_amount,
grouping__id
from
(
select
     CASE WHEN t2.term_prod_name IN ('和视频OPENAPI','和视频SDK') THEN nvl(t9.dept_id,'-998') ELSE nvl(t7.dept_id,nvl(t8.dept_id,nvl(t9.dept_id,'-998'))) END as dept_id,
     nvl(t1.term_prod_id,'-998') as term_prod_id, 
     case when nvl(t2.term_video_type_id,'null') ='null' then '-998' else t2.term_video_type_id end as term_video_type_id,
     case when nvl(t2.term_video_soft_id,'null') ='null' then '-998' else t2.term_video_soft_id end as term_video_soft_id,
     case when nvl(t2.term_prod_type_id,'null') ='null' then '-998' else t2.term_prod_type_id end as term_prod_type_id,
     t1.term_version_id as term_version_id,
     case when nvl(t2.term_os_type_id,'null') ='null' then '-998' else t2.term_os_type_id end as term_os_type_id,
     case when nvl(t4.business_id,'null') ='null' then '-998' else t4.business_id end as busi_id,
     nvl(t1.sub_busi_id,'-998') as sub_busi_id,
     nvl(t1.net_type_id,'-998') as net_type_id,
     nvl(t13.ncp_id,'-998') as cp_id,
     t1.product_id as product_id,
     nvl(t12.content_id,'-998') as content_id,
     '-998' as content_type,
     t1.broadcast_type_id as broadcast_type_id,
     nvl(t1.user_type_id, '-998') as user_type_id,
     case when nvl(t3.prov_id,'null') ='null' then '-998' else t3.prov_id end as province_id,
     nvl(t1.city_id,'-998') as city_id,
     case when nvl(t10.prov_id,'null') ='null' then '-998' else t10.prov_id end as phone_province_id,
     nvl(t1.phone_city_id,'-998') as phone_city_id,
     case when nvl(t6.comp_id,'null') ='null' then '-998' else t6.comp_id end as company_id,
     nvl(t5.chn_id,'-998') as chn_id,
     case when nvl(t5.chn_attr_1_id,'null') ='null' then '-998' else t5.chn_attr_1_id end as chn_attr_1_id,
     case when nvl(t5.chn_attr_2_id,'null') ='null' then '-998' else t5.chn_attr_2_id end as chn_attr_2_id,
     case when nvl(t5.chn_attr_3_id,'null') ='null' then '-998' else t5.chn_attr_3_id end as chn_attr_3_id,
     case when nvl(t5.chn_attr_4_id,'null') ='null' then '-998' else t5.chn_attr_4_id end as chn_attr_4_id,
     case when nvl(t5.chn_type,'null') ='null' then '-998' else t5.chn_type end as chn_type,
     case when nvl(t11.con_class_1_name,'null') ='null' then '-998' else t11.con_class_1_name end as con_class_1_name,
     case when nvl(t13.copyright_type,'null') ='null' then '-998' else t13.copyright_type end as copyright_type,
     case when nvl(t4.b_type_id,'null') ='null' then '-998' else t4.b_type_id end as busi_type_id,
     t1.program_id as program_id,
     nvl(t1.phone_city_id,'-998') as mobile_city_id,
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
     third_prepay_Amount,
     real_amount,
     total_amount
from cdmp_cnt_src t1
left join rpt.dim_term_prod_v t2 on t1.term_prod_id = t2.term_prod_id
left join rptdata.dim_region t3    on t1.city_id = t3.region_id
left join rptdata.dim_server t4    on t1.sub_busi_id = t4.sub_busi_id
left join rptdata.dim_chn t5       on t1.chn_id = t5.chn_id
left join rptdata.dim_comp_busi t6      on t1.sub_busi_id = t6.sub_busi_id
left join rptdata.dim_dept_term_prod t7 on t1.term_prod_id = t7.term_prod_id
left join rptdata.dim_dept_busi t8 on t4.business_id = t8.business_id
left join rptdata.dim_dept_chn  t9 on t1.chn_id = t9.chn_id
left join rptdata.dim_region t10    on t1.phone_city_id = t10.region_id
left join rptdata.dim_content_class t11 
on (case when t1.content_id = '-998' then concat('',rand()) else t1.content_id end) = t11.content_id
left join rptdata.dim_video_content t12 
on (case when t1.content_id = '-998' then concat('',rand()) else t1.content_id end) = t12.content_id
left join rptdata.dim_content_copyright t13 
on t12.copyright_id = t13.copyright_id
) tt
group by dept_id,term_prod_id,term_video_type_id,term_video_soft_id,term_prod_type_id,
		term_version_id,term_os_type_id,busi_id,sub_busi_id,net_type_id,cp_id,
		product_id,content_id,content_type,broadcast_type_id,user_type_id,
		province_id,city_id,phone_province_id,phone_city_id,company_id,
		chn_id,chn_attr_1_id,chn_attr_2_id,chn_attr_3_id,chn_attr_4_id,chn_type,
		con_class_1_name, copyright_type, busi_type_id, program_id
grouping sets (
-- 合作伙伴
(dept_id, busi_id, company_id, user_type_id),
(dept_id, busi_id, company_id),
(dept_id, busi_id, sub_busi_id, company_id),
(dept_id, busi_id, company_id, chn_id, chn_attr_1_id, chn_attr_2_id),
(dept_id, phone_province_id, company_id),
(dept_id, busi_id, phone_province_id, company_id, user_type_id),
(dept_id, busi_id, phone_province_id, company_id),
(dept_id, busi_id, phone_province_id, company_id, user_type_id, chn_id, chn_attr_1_id, chn_attr_2_id),
(dept_id, term_video_type_id, company_id),
(dept_id, term_video_type_id, company_id, user_type_id),
(dept_id, busi_id, phone_province_id, company_id, chn_id, chn_attr_1_id, chn_attr_2_id),

-- 客服
(busi_id, sub_busi_id, busi_type_id),
--(phone_province_id),
--(phone_province_id, phone_city_id),
(busi_id, sub_busi_id, phone_province_id, phone_city_id, busi_type_id),
(busi_id, sub_busi_id, phone_province_id, busi_type_id),
(chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id),
(phone_province_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id),
(phone_province_id, phone_city_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id),
(busi_type_id),

--NCP
(term_prod_id, term_prod_type_id, busi_id, cp_id, content_id, chn_id, program_id)
);
