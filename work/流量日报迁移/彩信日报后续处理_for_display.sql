create or replace view fact_mms_base_daily_v as
with mms_raw as (
select statis_day, metric_name, metric_value 
from cdmp_dmt.fact_high_level_metric_daily 
where metric_cat_name = '彩信日报' 
and dw_del_flag = 'N'
)
select "STATIS_DAY","DLY_VISIT_USER","DLY_VISIT_TRST","DLY_CLIENT_VISIT_USER","DLY_CLIENT_VISIT_TRST","MTD_VISIT_USER","MTD_VISIT_TRST",
		"DLY_USE_USER","DLY_USE_TRST","DLY_CLIENT_USE_USER","DLY_CLIENT_USE_TRST","MTD_USE_USER","MTD_USE_TRST",
		"DLY_CHRG_BY_TIME_USE_USER","DLY_CHRG_BY_MON_USE_USER","MTD_CHRG_BY_TIME_USE_USER","MTD_CHRG_BY_MON_USE_USER",
		"DLY_BOSS_NEWADD_ORDER_USER","DLY_BOSS_CANCEL_ORDER_USER","DLY_3RD_NEWADD_ORDER_USER","DLY_3RD_CANCEL_ORDER_USER",
		"MTD_CHRG_BY_TIME_INCOME","MTD_CHRG_BY_MON_INCOME","DLY_SP_VISIT_USER","MTD_SP_VISIT_USER","DLY_SP_PV","DLY_SP_VV",
		"DLY_YY_VISIT_USER","MTD_YY_VISIT_USER","DLY_YY_PV","DLY_YY_VV","DLY_ZB_VISIT_USER","MTD_ZB_VISIT_USER","DLY_ZB_PV",
		"DLY_ZB_VV","DLY_3RD_VISIT_USER","DLY_CO_CHN_VISIT_USER","DLY_CO_CHN_VISIT_TRST","DLY_EX_CHN_VISIT_USER",
		"DLY_EX_CHN_VISIT_TRST","DLY_OW_CHN_VISIT_USER","DLY_OW_CHN_VISIT_TRST","DLY_PR_CHN_VISIT_USER","DLY_PR_CHN_VISIT_TRST",
		"DLY_OT_CHN_VISIT_USER","DLY_OT_CHN_VISIT_TRST","DLY_CO_CHN_USE_USER","DLY_CO_CHN_USE_TRST","DLY_EX_CHN_USE_USER",
		"DLY_EX_CHN_USE_TRST","DLY_OW_CHN_USE_USER","DLY_OW_CHN_USE_TRST","DLY_PR_CHN_USE_USER","DLY_PR_CHN_USE_TRST",
		"DLY_OT_CHN_USE_USER","DLY_OT_CHN_USE_TRST","DLY_CO_CHN_USER_VV","DLY_CO_CHN_TRST_VV",
		"DLY_EX_CHN_USER_VV","DLY_EX_CHN_TRST_VV","DLY_OW_CHN_USER_VV","DLY_OW_CHN_TRST_VV",
		"DLY_PR_CHN_USER_VV","DLY_PR_CHN_TRST_VV","DLY_OT_CHN_USER_VV","DLY_OT_CHN_TRST_VV","DLY_CO_CHN_INCOME",
		"MTD_CO_CHN_INCOME","DLY_EX_CHN_INCOME","MTD_EX_CHN_INCOME","DLY_OW_CHN_INCOME","MTD_OW_CHN_INCOME","DLY_PR_CHN_INCOME",
		"MTD_PR_CHN_INCOME","DLY_OT_CHN_INCOME","MTD_OT_CHN_INCOME" from mms_raw
pivot (						--oracle 行转列
sum(metric_value) 
for metric_name 
in (--1.1
    '日_号码_活跃用户数'                    as dly_visit_user, 
    '日_游客_活跃用户数'                    as dly_visit_trst,
    '日_号码_客户端_活跃用户数'             as dly_client_visit_user,
    '日_游客_客户端_活跃用户数'             as dly_client_visit_trst,
    '月累计_号码_活跃用户数'                as mtd_visit_user,
    '月累计_游客_活跃用户数'                as mtd_visit_trst,
    --1.2
    '日_号码_使用用户数'                    as dly_use_user,
    '日_游客_使用用户数'                    as dly_use_trst,   
    '日_号码_客户端_使用用户数'             as dly_client_use_user,
    '日_游客_客户端_使用用户数'             as dly_client_use_trst,
    '月累计_号码_使用用户数'                as mtd_use_user,
    '月累计_游客_使用用户数'                as mtd_use_trst,
    --1.3
    '日_按次_付费_使用用户数'               as dly_chrg_by_time_use_user,
    '日_包月_付费_使用用户数'               as dly_chrg_by_mon_use_user,
    '月累计_按次_付费_使用用户数'           as mtd_chrg_by_time_use_user,
    '月累计_包月_付费_使用用户数'           as mtd_chrg_by_mon_use_user,
    
    --1.4
    '日_BOSS_新增_订购_用户数'              as dly_boss_newadd_order_user,
    '日_BOSS_退订_用户数'                   as dly_boss_cancel_order_user,      
    '日_非BOSS_新增_订购_用户数'            as dly_3rd_newadd_order_user,
    '日_非BOSS_退订_用户数'                 as dly_3rd_cancel_order_user, 
    
    --1.5
    '月累计_按次_信息费收入'                as mtd_chrg_by_time_income,
    '月累计_新增_包月_信息费收入'           as mtd_chrg_by_mon_income,
    
    --2.1
    '日_咪咕视频_活跃用户数'                as dly_sp_visit_user,
    '月累计_咪咕视频_活跃用户数'            as mtd_sp_visit_user,    
    '日_咪咕视频_PV'                        as dly_sp_pv,
    '日_咪咕视频_vv'                        as dly_sp_vv,
    
    --2.2
    '日_咪咕影院_活跃用户数'                as dly_yy_visit_user,
    '月累计_咪咕影院_活跃用户数'            as mtd_yy_visit_user,
    '日_咪咕影院_PV'                        as dly_yy_pv,
    '日_咪咕影院_vv'                        as dly_yy_vv,
    
    --2.3
    '日_咪咕直播_活跃用户数'                as dly_zb_visit_user,
    '月累计_咪咕直播_活跃用户数'            as mtd_zb_visit_user,
    '日_咪咕直播_PV'                        as dly_zb_pv,
    '日_咪咕直播_vv'                        as dly_zb_vv,   
    
    --2.4
    '日_内容输出_活跃用户数'               as dly_3rd_visit_user,
    
    --3.1
    '日_合作渠道_号码_访问用户数'           as dly_co_chn_visit_user,
    '日_合作渠道_游客_访问用户数'           as dly_co_chn_visit_trst,
    '日_拓展渠道_号码_访问用户数'           as dly_ex_chn_visit_user,
    '日_拓展渠道_游客_访问用户数'           as dly_ex_chn_visit_trst,
    '日_自有渠道_号码_访问用户数'           as dly_ow_chn_visit_user,
    '日_自有渠道_游客_访问用户数'           as dly_ow_chn_visit_trst,
    '日_省公司渠道_号码_访问用户数'         as dly_pr_chn_visit_user,
    '日_省公司渠道_游客_访问用户数'         as dly_pr_chn_visit_trst,
    '日_其他渠道_号码_访问用户数'           as dly_ot_chn_visit_user,
    '日_其他渠道_游客_访问用户数'           as dly_ot_chn_visit_trst,
    
    --3.2
    '日_合作渠道_号码_使用用户数'           as dly_co_chn_use_user,
    '日_合作渠道_游客_使用用户数'           as dly_co_chn_use_trst,
    '日_拓展渠道_号码_使用用户数'           as dly_ex_chn_use_user,
    '日_拓展渠道_游客_使用用户数'           as dly_ex_chn_use_trst,
    '日_自有渠道_号码_使用用户数'           as dly_ow_chn_use_user,
    '日_自有渠道_游客_使用用户数'           as dly_ow_chn_use_trst,
    '日_省公司渠道_号码_使用用户数'         as dly_pr_chn_use_user,
    '日_省公司渠道_游客_使用用户数'         as dly_pr_chn_use_trst,
    '日_其他渠道_号码_使用用户数'           as dly_ot_chn_use_user,
    '日_其他渠道_游客_使用用户数'           as dly_ot_chn_use_trst,   
    
    --3.3
    '日_合作渠道_号码_节目使用次数'         as dly_co_chn_user_vv,
    '日_合作渠道_游客_节目使用次数'         as dly_co_chn_trst_vv,
    '日_拓展渠道_号码_节目使用次数'         as dly_ex_chn_user_vv,
    '日_拓展渠道_游客_节目使用次数'         as dly_ex_chn_trst_vv,
    '日_自有渠道_号码_节目使用次数'         as dly_ow_chn_user_vv,
    '日_自有渠道_游客_节目使用次数'         as dly_ow_chn_trst_vv,
    '日_省公司渠道_号码_节目使用次数'       as dly_pr_chn_user_vv,
    '日_省公司渠道_游客_节目使用次数'       as dly_pr_chn_trst_vv,
    '日_其他渠道_号码_节目使用次数'         as dly_ot_chn_user_vv,
    '日_其他渠道_游客_节目使用次数'         as dly_ot_chn_trst_vv,   
   
     --3.4
    '日_合作渠道_新增_信息费收入'           as dly_co_chn_income,
    '月累计_合作渠道_新增_信息费收入'       as mtd_co_chn_income,
    '日_拓展渠道_新增_信息费收入'           as dly_ex_chn_income,
    '月累计_拓展渠道_新增_信息费收入'       as mtd_ex_chn_income,
    '日_自有渠道_新增_信息费收入'           as dly_ow_chn_income,
    '月累计_自有渠道_新增_信息费收入'       as mtd_ow_chn_income,
    '日_省公司渠道_新增_信息费收入'         as dly_pr_chn_income,
    '月累计_省公司渠道_新增_信息费收入'     as mtd_pr_chn_income,
    '日_其他渠道_新增_信息费收入'           as dly_ot_chn_income,
    '月累计_其他渠道_新增_信息费收入'       as mtd_ot_chn_income
    )
);

---------------------------------------------------------------------------------------------------------------

create or replace view fact_mms_full_daily_v as
with mms_base as 
(
select 
tm.*,
round(avg(dly_visit_user) over(partition by substr(statis_day, 1, 6) order by statis_day),0) mtd_avg_visit_user, 		 --月累计_平均_
round(avg(dly_visit_trst) over(partition by substr(statis_day, 1, 6) order by statis_day),0) mtd_avg_visit_trst,         --月累计_平均_
round(avg(dly_use_user) over(partition by substr(statis_day, 1, 6) order by statis_day),0) mtd_avg_use_user,             --月累计_平均_
round(avg(dly_use_trst) over(partition by substr(statis_day, 1, 6) order by statis_day),0) mtd_avg_use_trst,             --月累计_平均_
round(avg(dly_sp_visit_user) over(partition by substr(statis_day, 1, 6) order by statis_day),0) mtd_avg_sp_visit_user,   --月累计_平均_
round(avg(dly_yy_visit_user) over(partition by substr(statis_day, 1, 6) order by statis_day),0) mtd_avg_yy_visit_user,   --月累计_平均_
round(avg(dly_zb_visit_user) over(partition by substr(statis_day, 1, 6) order by statis_day),0) mtd_avg_zb_visit_user    --月累计_平均_
from 
cdmp_dmt.fact_mms_base_daily_v tm
)
select
t1.statis_day,
--------------------------------------------------------------------------------
--一、 全网业务发展情况 (47)
--1. 活跃用户数 (20)
--1.1.1 - 1.1.3
round((nvl(t1.dly_visit_user,0) + nvl(t1.dly_visit_trst,0))/10000, 2) as dly_visit_user_all,  
round(t1.dly_visit_user / 10000, 2) dly_visit_user, 
round(t1.dly_visit_trst / 10000, 2) dly_visit_trst,
  
--客户端用户数
--1.1.4 - 1.1.6
round((nvl(t1.dly_client_visit_user,0) + nvl(t1.dly_client_visit_trst,0))/10000, 2) as dly_client_visit_user_all, 
round(t1.dly_client_visit_user / 10000, 2) dly_client_visit_user,
round(t1.dly_client_visit_trst / 10000, 2) dly_client_visit_trst,

--月累计用户数
--1.1.7 - 1.1.9
round((nvl(t1.mtd_visit_user,0) + nvl(t1.mtd_visit_trst,0))/10000, 2) as mtd_visit_user_all, 
round(t1.mtd_visit_user/10000, 2) mtd_visit_user,
round(t1.mtd_visit_trst/10000, 2) mtd_visit_trst,

--环比上月
--1.1.10 - 1.1.12
round(
decode(
       nvl(t2.mtd_visit_user,0) + nvl(t2.mtd_visit_trst,0 ), 
       0, null,
       ((nvl(t1.mtd_visit_user,0) + nvl(t1.mtd_visit_trst,0)) - (nvl(t2.mtd_visit_user,0) + nvl(t2.mtd_visit_trst,0 ))) 
       / (nvl(t2.mtd_visit_user,0) + nvl(t2.mtd_visit_trst,0 ))
      ) * 100, 2) as mom_visit_user_all_pct,
round(
decode(
       nvl(t2.mtd_visit_user,0), 
       0, null,
      (nvl(t1.mtd_visit_user,0) - nvl(t2.mtd_visit_user,0)) / nvl(t2.mtd_visit_user,0)
      ) * 100, 2) as mom_visit_user_pct,
round(
decode(nvl(t2.mtd_visit_trst,0), 
       0, null,
      (nvl(t1.mtd_visit_trst,0) - nvl(t2.mtd_visit_trst,0)) / nvl(t2.mtd_visit_trst,0)
      ) * 100, 2) as mom_visit_trst_pct,

--日均访问用户数
--1.1.13 - 1.1.15
round((nvl(t1.mtd_avg_visit_user,0) + nvl(t1.mtd_avg_visit_trst,0))/10000, 2) as mtd_avg_visit_user_all, 
round(t1.mtd_avg_visit_user/10000, 2) mtd_avg_visit_user,
round(t1.mtd_avg_visit_trst/10000,2) mtd_avg_visit_trst,

--1.1.16 - 1.1.18
round(
decode(
       nvl(t2.mtd_avg_visit_user,0) + nvl(t2.mtd_avg_visit_trst,0 ), 
       0, null,
       ((nvl(t1.mtd_avg_visit_user,0) + nvl(t1.mtd_avg_visit_trst,0)) - (nvl(t2.mtd_avg_visit_user,0) + nvl(t2.mtd_avg_visit_trst,0 ))) 
       / (nvl(t2.mtd_avg_visit_user,0) + nvl(t2.mtd_avg_visit_trst,0 ))
      ) * 100, 2) as mom_avg_visit_user_all_pct,      
round(
decode(
       nvl(t2.mtd_avg_visit_user,0), 
       0, null,
      (nvl(t1.mtd_avg_visit_user,0) - nvl(t2.mtd_avg_visit_user,0)) / nvl(t2.mtd_avg_visit_user,0)
      ) * 100, 2) as mom_avg_visit_user_pct,
round(
decode(nvl(t2.mtd_avg_visit_trst,0), 
       0, null,
      (nvl(t1.mtd_avg_visit_trst,0) - nvl(t2.mtd_avg_visit_trst,0)) / nvl(t2.mtd_avg_visit_trst,0)
      ) * 100, 2) as mom_avg_visit_trst_pct,
 
--活跃系数
--1.1.19, 1.1.20  
round(decode((nvl(t1.mtd_visit_user,0) + nvl(t1.mtd_visit_trst,0)),
       0, null,
      (nvl(t1.mtd_avg_visit_user,0) + nvl(t1.mtd_avg_visit_trst,0)) / (nvl(t1.mtd_visit_user,0) + nvl(t1.mtd_visit_trst,0))
      ) * 100, 2) as mon_active_rate,      

round(
round(decode((nvl(t1.mtd_visit_user,0) + nvl(t1.mtd_visit_trst,0)),
       0, null,
      (nvl(t1.mtd_avg_visit_user,0) + nvl(t1.mtd_avg_visit_trst,0)) / (nvl(t1.mtd_visit_user,0) + nvl(t1.mtd_visit_trst,0))
      ) * 100, 2)
- 
round(decode((nvl(t2.mtd_visit_user,0) + nvl(t2.mtd_visit_trst,0)),
       0, null,
      (nvl(t2.mtd_avg_visit_user,0) + nvl(t2.mtd_avg_visit_trst,0)) / (nvl(t2.mtd_visit_user,0) + nvl(t2.mtd_visit_trst,0))
      ) * 100, 2), 2) as mom_active_pct,
--2.使用用户数 (12)
--1.2.1 - 1.2.3
round((nvl(t1.dly_use_user,0) + nvl(t1.dly_use_trst,0))/10000, 2) as dly_use_user_all,  
round(t1.dly_use_user/10000, 2) dly_use_user,
round(t1.dly_use_trst/10000, 2) dly_use_trst,

--客户端用户数
--1.2.4 - 1.2.6
round((nvl(t1.dly_client_use_user,0) + nvl(t1.dly_client_use_trst,0))/10000, 2) as dly_client_use_user_all, 
round(t1.dly_client_use_user/10000, 2) dly_client_use_user,
round(t1.dly_client_use_trst/10000, 2) dly_client_use_trst,

--月累计使用用户数
--1.2.7 - 1.2.9
round((nvl(t1.mtd_use_user,0) + nvl(t1.mtd_use_trst,0))/10000, 2) as mtd_use_user_all,   
round(t1.mtd_use_user/10000, 2) mtd_use_user,
round(t1.mtd_use_trst/10000, 2) mtd_use_trst,

--环比上月 (5)
--1.2.10 - 1.2.12
round(
decode(
       nvl(t2.mtd_use_user,0) + nvl(t2.mtd_use_trst,0 ), 
       0, null,
       ((nvl(t1.mtd_use_user,0) + nvl(t1.mtd_use_trst,0)) - (nvl(t2.mtd_use_user,0) + nvl(t2.mtd_use_trst,0 ))) 
       / (nvl(t2.mtd_use_user,0) + nvl(t2.mtd_use_trst,0 ))
      ) * 100, 2) as mom_use_user_all_pct,
round(
decode(
       nvl(t2.mtd_use_user,0), 
       0, null,
      (nvl(t1.mtd_use_user,0) - nvl(t2.mtd_use_user,0)) / nvl(t2.mtd_use_user,0)
      ) * 100, 2) as mom_use_user_pct,
round(
decode(nvl(t2.mtd_use_trst,0), 
       0, null,
      (nvl(t1.mtd_use_trst,0) - nvl(t2.mtd_use_trst,0)) / nvl(t2.mtd_use_trst,0)
      ) * 100, 2) as mom_use_trst_pct,

--3.付费使用用户数 (5)
--1.3.1 - 1.3.3
round((nvl(t1.dly_chrg_by_time_use_user,0) + nvl(t1.dly_chrg_by_mon_use_user,0))/10000, 2) as dly_chrg_use_user,
round(t1.dly_chrg_by_time_use_user/10000, 2) dly_chrg_by_time_use_user,
round(t1.dly_chrg_by_mon_use_user/10000, 2) dly_chrg_by_mon_use_user,

--月累计付费使用用户数
--1.3.4 - 1.3.5
round((nvl(t1.mtd_chrg_by_time_use_user,0) + nvl(t1.mtd_chrg_by_mon_use_user,0))/10000, 2) as mtd_chrg_use_user,
 --环比上月
round(
decode(
       nvl(t2.mtd_chrg_by_time_use_user,0) + nvl(t2.mtd_chrg_by_mon_use_user,0 ), 
       0, null,
       ((nvl(t1.mtd_chrg_by_time_use_user,0) + nvl(t1.mtd_chrg_by_mon_use_user,0)) - (nvl(t2.mtd_chrg_by_time_use_user,0) + nvl(t2.mtd_chrg_by_mon_use_user,0 ))) 
       / (nvl(t2.mtd_chrg_by_time_use_user,0) + nvl(t2.mtd_chrg_by_mon_use_user,0 ))
      ) * 100, 2) as mom_chrg_use_user_pct,
      
--4.订购/退订用户数 (4)
--1.4.1 - 1.4.2
round((nvl(t1.dly_boss_newadd_order_user,0) + nvl(t1.dly_3rd_newadd_order_user,0))/10000, 2) as dly_newadd_order_user,
round(t1.dly_boss_newadd_order_user/10000, 2) dly_boss_newadd_order_user,
--1.4.3 - 1.4.4
round((nvl(t1.dly_boss_cancel_order_user,0) + nvl(t1.dly_3rd_cancel_order_user,0))/10000, 2) as dly_cancel_order_user,
round(t1.dly_boss_cancel_order_user/10000, 2) dly_boss_cancel_order_user,

--5.信息费收入 (6)
--5.1.1 - 5.1.2
round((nvl(t1.mtd_chrg_by_time_income,0) + nvl(t1.mtd_chrg_by_mon_income,0))/10000, 2) as mtd_chrg_income,
round(
decode(
       nvl(t2.mtd_chrg_by_time_income,0) + nvl(t2.mtd_chrg_by_mon_income,0 ), 
       0, null,
       ((nvl(t1.mtd_chrg_by_time_income,0) + nvl(t1.mtd_chrg_by_mon_income,0)) - (nvl(t2.mtd_chrg_by_time_income,0) + nvl(t2.mtd_chrg_by_mon_income,0 ))) 
       / (nvl(t2.mtd_chrg_by_time_income,0) + nvl(t2.mtd_chrg_by_mon_income,0 ))
      ) * 100, 2) as mom_chrg_income_pct,

--5.1.3 - 5.1.4
round(t1.mtd_chrg_by_time_income/10000, 2) mtd_chrg_by_time_income,
round(
decode(nvl(t2.mtd_chrg_by_time_income,0), 
       0, null,
      (nvl(t1.mtd_chrg_by_time_income,0) - nvl(t2.mtd_chrg_by_time_income,0)) / nvl(t2.mtd_chrg_by_time_income,0)
      ) * 100, 2) as mtd_chrg_by_time_income_pct,

--5.1.5 - 5.1.6
round(t1.mtd_chrg_by_mon_income/10000, 2) mtd_chrg_by_mon_income,
round(
decode(nvl(t2.mtd_chrg_by_mon_income,0), 
       0, null,
      (nvl(t1.mtd_chrg_by_mon_income,0) - nvl(t2.mtd_chrg_by_mon_income,0)) / nvl(t2.mtd_chrg_by_mon_income,0)
      )*100, 2) as mtd_chrg_by_mon_income_pct,
--------------------------------------------------------------------------------
--二、重点客户端发展情况 (28)
--1.咪咕视频 (9) 
--2.1.1
round(t1.dly_sp_visit_user/10000, 2) dly_sp_visit_user,

--2.1.2 - 2.1.3
round(t1.mtd_sp_visit_user/10000, 2) mtd_sp_visit_user,
--环比上月
round(
decode(nvl(t2.mtd_sp_visit_user,0), 
       0, null,
      (nvl(t1.mtd_sp_visit_user,0) - nvl(t2.mtd_sp_visit_user,0)) / nvl(t2.mtd_sp_visit_user,0)
      )*100, 2) as mom_sp_visit_user_pct,

--活跃系数
--2.1.4 - 2.1.5
round(
decode(nvl(t1.mtd_sp_visit_user,0),
       0, null,
      nvl(t1.mtd_avg_sp_visit_user,0) / nvl(t1.mtd_sp_visit_user,0)
      )*100, 2) as mon_sp_active_rate,
  --活跃系数环比上月？
  
round(
round(
decode(nvl(t1.mtd_sp_visit_user,0),
       0, null,
      nvl(t1.mtd_avg_sp_visit_user,0) / nvl(t1.mtd_sp_visit_user,0)
      )*100, 2)
-  
round(
decode(nvl(t2.mtd_sp_visit_user,0),
       0, null,
      nvl(t2.mtd_avg_sp_visit_user,0) / nvl(t2.mtd_sp_visit_user,0)
      )*100, 2), 2) as mom_sp_active_rate,

--日PV
--2.1.6 - 2.1.7
round(t1.dly_sp_pv/10000, 2) dly_sp_pv,
--PV环比？
round(
decode(nvl(t2.dly_sp_pv,0), 
       0, null,
      (nvl(t1.dly_sp_pv,0) - nvl(t2.dly_sp_pv,0)) / nvl(t2.dly_sp_pv,0)
      )*100, 2) as mom_sp_pv_pct,

--日VV    
--2.1.8 - 2.1.9
round(t1.dly_sp_vv/10000, 2) dly_sp_vv,   
  --VV环比？
round(
decode(nvl(t2.dly_sp_vv,0), 
       0, null,
      (nvl(t1.dly_sp_vv,0) - nvl(t2.dly_sp_vv,0)) / nvl(t2.dly_sp_vv,0)
      )*100, 2) as mom_sp_vv_pct,
	  
--2.咪咕影院 (9)
round(t1.dly_yy_visit_user/10000, 2) dly_yy_visit_user,
round(t1.mtd_yy_visit_user/10000, 2) mtd_yy_visit_user,
  --环比上月
round(
decode(nvl(t2.mtd_yy_visit_user,0), 
       0, null,
      (nvl(t1.mtd_yy_visit_user,0) - nvl(t2.mtd_yy_visit_user,0)) / nvl(t2.mtd_yy_visit_user,0)
      )*100, 2) as mom_yy_visit_user_pct,
  --活跃系数
round(
decode(nvl(t1.mtd_yy_visit_user,0),
       0, null,
      nvl(t1.mtd_avg_yy_visit_user,0) / nvl(t1.mtd_yy_visit_user,0)
      )*100, 2) as mon_yy_active_rate,
  --活跃系数环比上月？
round(
round(
decode(nvl(t1.mtd_yy_visit_user,0),
       0, null,
      nvl(t1.mtd_avg_yy_visit_user,0) / nvl(t1.mtd_yy_visit_user,0)
      )*100, 2) 
-  
round(
decode(nvl(t2.mtd_yy_visit_user,0),
       0, null,
      nvl(t2.mtd_avg_yy_visit_user,0) / nvl(t2.mtd_yy_visit_user,0)
      )*100, 2) , 2) as mom_yy_active_rate,  
  

  --日PV
round(t1.dly_yy_pv/10000, 2) dly_yy_pv,
  --PV环比？
round(
decode(nvl(t2.dly_yy_pv,0), 
       0, null,
      (nvl(t1.dly_yy_pv,0) - nvl(t2.dly_yy_pv,0)) / nvl(t2.dly_yy_pv,0)
      )*100, 2) as mom_yy_pv_pct,
  --日VV    
round(t1.dly_yy_vv/10000, 2) dly_yy_vv,   
  --VV环比？
round(
decode(nvl(t2.dly_yy_vv,0), 
       0, null,
      (nvl(t1.dly_yy_vv,0) - nvl(t2.dly_yy_vv,0)) / nvl(t2.dly_yy_vv,0)
      )*100, 2) as mom_yy_vv_pct,

--3.咪咕直播 (9)
round(t1.dly_zb_visit_user/10000, 2) dly_zb_visit_user,
round(t1.mtd_zb_visit_user/10000, 2) mtd_zb_visit_user,
  --环比上月
round(
decode(nvl(t2.mtd_zb_visit_user,0), 
       0, null,
      (nvl(t1.mtd_zb_visit_user,0) - nvl(t2.mtd_zb_visit_user,0)) / nvl(t2.mtd_zb_visit_user,0)
      )*100, 2) as mom_zb_visit_user_pct,
  --活跃系数
round(
decode(nvl(t1.mtd_zb_visit_user,0),
       0, null,
      nvl(t1.mtd_avg_zb_visit_user,0) / nvl(t1.mtd_zb_visit_user,0)
      )*100, 2) as mon_zb_active_rate,
  
  --活跃系数环比上月？
round(
round(
decode(nvl(t1.mtd_zb_visit_user,0),
       0, null,
      nvl(t1.mtd_avg_zb_visit_user,0) / nvl(t1.mtd_zb_visit_user,0)
      )*100, 2) 
-  
round(
decode(nvl(t2.mtd_zb_visit_user,0),
       0, null,
      nvl(t2.mtd_avg_zb_visit_user,0) / nvl(t2.mtd_zb_visit_user,0)
      )*100, 2) , 2) as mom_zb_active_rate,  
      
  --日PV
round(t1.dly_zb_pv/10000, 2) dly_zb_pv,
  --PV环比？
round(
decode(nvl(t2.dly_zb_pv,0), 
       0, null,
      (nvl(t1.dly_zb_pv,0) - nvl(t2.dly_zb_pv,0)) / nvl(t2.dly_zb_pv,0)
      )*100, 2) as mom_zb_pv_pct,
  --日VV    
round(t1.dly_zb_vv/10000, 2) dly_zb_vv,   
  --VV环比？
round(
decode(nvl(t2.dly_zb_vv,0), 
       0, null,
      (nvl(t1.dly_zb_vv,0) - nvl(t2.dly_zb_vv,0)) / nvl(t2.dly_zb_vv,0)
      )*100, 2) as mom_zb_vv_pct,
      
--4.内容输出 
round(t1.dly_3rd_visit_user/10000, 2) dly_3rd_visit_user,
--------------------------------------------------------------------------------
--三、全网渠道拓展情况：

  --1.访问用户数 (15)
  --合作渠道
round((nvl(t1.dly_co_chn_visit_user,0) + nvl(t1.dly_co_chn_visit_trst,0))/10000, 2) dly_co_chn_visit_user_all,
round(t1.dly_co_chn_visit_user/10000, 2) dly_co_chn_visit_user,
round(t1.dly_co_chn_visit_trst/10000, 2) dly_co_chn_visit_trst,

  --拓展渠道
round((nvl(t1.dly_ex_chn_visit_user,0) + nvl(t1.dly_ex_chn_visit_trst,0))/10000, 2) dly_ex_chn_visit_user_all,
round(t1.dly_ex_chn_visit_user/10000, 2) dly_ex_chn_visit_user,
round(t1.dly_ex_chn_visit_trst/10000, 2) dly_ex_chn_visit_trst,

  --自有渠道
round((nvl(t1.dly_ow_chn_visit_user,0) + nvl(t1.dly_ow_chn_visit_trst,0))/10000, 2) dly_ow_chn_visit_user_all,
round(t1.dly_ow_chn_visit_user/10000, 2) dly_ow_chn_visit_user,
round(t1.dly_ow_chn_visit_trst/10000, 2) dly_ow_chn_visit_trst,

  --省公司
round((nvl(t1.dly_pr_chn_visit_user,0) + nvl(t1.dly_pr_chn_visit_trst,0))/10000, 2) dly_pr_chn_visit_user_all,  
round(t1.dly_pr_chn_visit_user/10000, 2) dly_pr_chn_visit_user,
round(t1.dly_pr_chn_visit_trst/10000, 2) dly_pr_chn_visit_trst,

 --其他
round((nvl(t1.dly_ot_chn_visit_user,0) + nvl(t1.dly_ot_chn_visit_trst,0))/10000, 2) dly_ot_chn_visit_user_all, 
round(t1.dly_ot_chn_visit_user/10000, 2) dly_ot_chn_visit_user,
round(t1.dly_ot_chn_visit_trst/10000, 2) dly_ot_chn_visit_trst,

  --2. 使用用户数 (15)
  --合作渠道
round((nvl(t1.dly_co_chn_use_user,0) + nvl(t1.dly_co_chn_use_trst,0))/10000, 2) dly_co_chn_use_user_all,
round(t1.dly_co_chn_use_user/10000, 2) dly_co_chn_use_user,
round(t1.dly_co_chn_use_trst/10000, 2) dly_co_chn_use_trst,
  
  --拓展渠道
round((nvl(t1.dly_ex_chn_use_user,0) + nvl(t1.dly_ex_chn_use_trst,0))/10000, 2) dly_ex_chn_use_user_all,
round(t1.dly_ex_chn_use_user/10000, 2) dly_ex_chn_use_user,
round(t1.dly_ex_chn_use_trst/10000, 2) dly_ex_chn_use_trst,

  --自有渠道
round((nvl(t1.dly_ow_chn_use_user,0) + nvl(t1.dly_ow_chn_use_trst,0))/10000, 2) dly_ow_chn_use_user_all,
round(t1.dly_ow_chn_use_user/10000, 2) dly_ow_chn_use_user,
round(t1.dly_ow_chn_use_trst/10000, 2) dly_ow_chn_use_trst,
  
  --省公司
round((nvl(t1.dly_pr_chn_use_user,0) + nvl(t1.dly_pr_chn_use_trst,0))/10000, 2) dly_pr_chn_use_user_all,  
round(t1.dly_pr_chn_use_user/10000, 2) dly_pr_chn_use_user,
round(t1.dly_pr_chn_use_trst/10000, 2) dly_pr_chn_use_trst,

  --其他
round((nvl(t1.dly_ot_chn_use_user,0) + nvl(t1.dly_ot_chn_use_trst,0))/10000, 2) dly_ot_chn_use_user_all, 
round(t1.dly_ot_chn_use_user/10000, 2) dly_ot_chn_use_user,
round(t1.dly_ot_chn_use_trst/10000, 2) dly_ot_chn_use_trst,

  --3.节目使用次数 (15)
  --合作渠道
round((nvl(t1.dly_co_chn_user_vv,0) + nvl(t1.dly_co_chn_trst_vv,0))/10000, 2) dly_co_chn_user_all_vv,
round(t1.dly_co_chn_user_vv/10000, 2) dly_co_chn_user_vv,
round(t1.dly_co_chn_trst_vv/10000, 2) dly_co_chn_trst_vv,
  
  --拓展渠道
round((nvl(t1.dly_ex_chn_user_vv,0) + nvl(t1.dly_ex_chn_trst_vv,0))/10000, 2) dly_ex_chn_user_all_vv,
round(t1.dly_ex_chn_user_vv/10000, 2) dly_ex_chn_user_vv,
round(t1.dly_ex_chn_trst_vv/10000, 2) dly_ex_chn_trst_vv,

  --自有渠道
round((nvl(t1.dly_ow_chn_user_vv,0) + nvl(t1.dly_ow_chn_trst_vv,0))/10000, 2) dly_ow_chn_user_all_vv,
round(t1.dly_ow_chn_user_vv/10000, 2) dly_ow_chn_user_vv,
round(t1.dly_ow_chn_trst_vv/10000, 2) dly_ow_chn_trst_vv,
  
  --省公司
round((nvl(t1.dly_pr_chn_user_vv,0) + nvl(t1.dly_pr_chn_trst_vv,0))/10000, 2) dly_pr_chn_user_all_vv,
round(t1.dly_pr_chn_user_vv/10000, 2) dly_pr_chn_user_vv,
round(t1.dly_pr_chn_trst_vv/10000, 2) dly_pr_chn_trst_vv,

  --其他
round((nvl(t1.dly_ot_chn_user_vv,0) + nvl(t1.dly_ot_chn_trst_vv,0))/10000, 2) dly_ot_chn_user_all_vv,
round(t1.dly_ot_chn_user_vv/10000, 2) dly_ot_chn_user_vv,
round(t1.dly_ot_chn_trst_vv/10000, 2) dly_ot_chn_trst_vv,

  
--4.新增收入 (15)    
round(t1.dly_co_chn_income/10000, 2) dly_co_chn_income,
round(t1.mtd_co_chn_income/10000, 2) mtd_co_chn_income,
round(
decode(nvl(t2.mtd_co_chn_income,0), 
       0, null,
      (nvl(t1.mtd_co_chn_income,0) - nvl(t2.mtd_co_chn_income,0)) / nvl(t2.mtd_co_chn_income,0)
      )*100, 2) as mom_co_chn_income_pct,


round(t1.dly_ex_chn_income/10000, 2) dly_ex_chn_income,
round(t1.mtd_ex_chn_income/10000, 2) mtd_ex_chn_income,
round(
decode(nvl(t2.mtd_ex_chn_income,0), 
       0, null,
      (nvl(t1.mtd_ex_chn_income,0) - nvl(t2.mtd_ex_chn_income,0)) / nvl(t2.mtd_ex_chn_income,0)
      )*100, 2) as mom_ex_chn_income_pct,

round(t1.dly_ow_chn_income/10000, 2) dly_ow_chn_income,
round(t1.mtd_ow_chn_income/10000, 2) mtd_ow_chn_income,
round(
decode(nvl(t2.mtd_ow_chn_income,0), 
       0, null,
      (nvl(t1.mtd_ow_chn_income,0) - nvl(t2.mtd_ow_chn_income,0)) / nvl(t2.mtd_ow_chn_income,0)
      )*100, 2) as mom_ow_chn_income_pct,      
      
round(t1.dly_pr_chn_income/10000, 2) dly_pr_chn_income,
round(t1.mtd_pr_chn_income/10000, 2) mtd_pr_chn_income,
round(
decode(nvl(t2.mtd_pr_chn_income,0), 
       0, null,
      (nvl(t1.mtd_pr_chn_income,0) - nvl(t2.mtd_pr_chn_income,0)) / nvl(t2.mtd_pr_chn_income,0)
      )*100, 2) as mom_pr_chn_income_pct,    
      
round(t1.dly_ot_chn_income/10000, 2) dly_ot_chn_income,
round(t1.mtd_ot_chn_income/10000, 2) mtd_ot_chn_income,
round(
decode(nvl(t2.mtd_ot_chn_income,0), 
       0, null,
      (nvl(t1.mtd_ot_chn_income,0) - nvl(t2.mtd_ot_chn_income,0)) / nvl(t2.mtd_ot_chn_income,0)
      ) * 100, 2) as mom_ot_chn_income_pct
from mms_base t1 left join mms_base t2 
on (to_char(ADD_MONTHS(to_date(t1.statis_day, 'yyyymmdd'), -1),'yyyymmdd') = t2.statis_day)
where t1.statis_day >= '20170501';


---------------------------------------------------------------------------------------------------------------


create or replace view fact_mms_text_daily_v as
select 
statis_day,
 '一、全网业务发展情况：' || chr(13) || chr(10) || 
'1.活跃用户数：' || chr(13) || chr(10) || 
'日' || dly_visit_user_all || '万户（号码' || dly_visit_user || '万，游客' || dly_visit_trst || '万）' || chr(13) || chr(10) || 
'客户端用户数：' || dly_client_visit_user_all || '万户（号码' || dly_client_visit_user || '万，游客' || dly_client_visit_trst || '万）' || chr(13) || chr(10) || 
'月累计访问用户数：' || mtd_visit_user_all || '万户（号码' || mtd_visit_user || '万，游客' || mtd_visit_trst || '万），同比上月' || case when mom_visit_user_all_pct >= 0 then '增' else '降' end  || abs(mom_visit_user_all_pct) || '%（号码' || case when mom_visit_user_pct >= 0 then '增' else '降' end  || abs(mom_visit_user_pct) || '%，游客' || case when mom_visit_trst_pct >= 0 then '增' else '降' end  || abs(mom_visit_trst_pct) || '%）' || chr(13) || chr(10) || 
'日均访问用户数：' || mtd_avg_visit_user_all || '万户（号码' || mtd_avg_visit_user || '万，游客' || mtd_avg_visit_trst || '万），同比上月' || case when mom_avg_visit_user_all_pct >= 0 then '增' else '降' end  || abs(mom_avg_visit_user_all_pct) || '%（号码' || case when mom_avg_visit_user_pct >= 0 then '增' else '降' end  || abs(mom_avg_visit_user_pct) || '%，游客' || case when mom_avg_visit_trst_pct >= 0 then '增' else '降' end  || abs(mom_avg_visit_trst_pct) || '%)，活跃系数' || mon_active_rate || '%，同比上月' || case when mom_active_pct >= 0 then '增' else '降' end  || abs(mom_active_pct) || '%' || chr(13) || chr(10) || 
'' || chr(13) || chr(10) || 

'2.使用用户数：' || chr(13) || chr(10) || 
'日' || dly_use_user_all || '万户（号码' || dly_use_user || '万，游客' || dly_use_trst || '万）' || chr(13) || chr(10) || 
'客户端用户数：' || dly_client_use_user_all || '万户（号码' || dly_client_use_user || '万，游客' || dly_client_use_trst || '万）' || chr(13) || chr(10) || 
'月累计使用用户数：' || mtd_use_user_all || '万户（号码' || mtd_use_user || '万，游客' || mtd_use_trst || '万），同比上月' || case when mom_use_user_all_pct >= 0 then '增' else '降' end  || abs(mom_use_user_all_pct) || '%（号码' || case when mom_use_user_pct >= 0 then '增' else '降' end  || abs(mom_use_user_pct) || '%，游客' || case when mom_use_trst_pct >= 0 then '增' else '降' end  || abs(mom_use_trst_pct) || '%）' || chr(13) || chr(10) || 
'' || chr(13) || chr(10) || 

'3.付费使用用户数:' || chr(13) || chr(10) || 
'日' || dly_chrg_use_user || '万户（按次使用用户数：' || dly_chrg_by_time_use_user || '万户，包月使用用户数' || dly_chrg_by_mon_use_user || '万户）' || chr(13) || chr(10) || 
'月累计付费使用用户数：' || mtd_chrg_use_user || '万户，同比上月' || case when mom_chrg_use_user_pct >= 0 then '增' else '降' end  || abs(mom_chrg_use_user_pct) || '%' || chr(13) || chr(10) || 
'' || chr(13) || chr(10) || 

'4.订购/退订用户数：' || chr(13) || chr(10) || 
'日新增订购用户:' || dly_newadd_order_user || '万户（BOSS订购用户数：' || dly_boss_newadd_order_user || '万户）' || chr(13) || chr(10) || 
'日退订用户数：' || dly_cancel_order_user || '万户（BOSS退订用户数：' || dly_boss_cancel_order_user || '万户）' || chr(13) || chr(10) || 
'' || chr(13) || chr(10) || 

'5.信息费收入（平台统计）:' || chr(13) || chr(10) || 
'月累计新增信息费收入：' || mtd_chrg_income || '万元，同比上月' || case when mom_chrg_income_pct >= 0 then '增' else '降' end  || abs(mom_chrg_income_pct) || '%；' || 
'按次收入：' || mtd_chrg_by_time_income || '万元，同比上月' || case when mtd_chrg_by_time_income_pct >= 0 then '增' else '降' end  || abs(mtd_chrg_by_time_income_pct)  || '%；' || 
'新增包月收入：' || mtd_chrg_by_mon_income || '万元，同比上月' || case when mtd_chrg_by_mon_income_pct >= 0 then '增' else '降' end  || abs(mtd_chrg_by_mon_income_pct) || '%；' || chr(13) || chr(10) || 
'' || chr(13) || chr(10) || 

'二、重点客户端发展情况：' || chr(13) || chr(10) || 
'1、咪咕视频' || chr(13) || chr(10) || 
'日活跃' || dly_sp_visit_user || '万户；' || chr(13) || chr(10) || 
'月累计' || mtd_sp_visit_user || '万户，同比上月' || case when mom_sp_visit_user_pct >= 0 then '增' else '降' end  || abs(mom_sp_visit_user_pct) || '%；' || chr(13) || chr(10) || 
'活跃系数' || mon_sp_active_rate  || '%，同比上月' || case when mom_sp_active_rate >= 0 then '增' else '降' end  || abs(mom_sp_active_rate) || '%；' || chr(13) || chr(10) || 
'日pv ' || dly_sp_pv || '万次，同比上月当日' || case when mom_sp_pv_pct >= 0 then '增' else '降' end  || abs(mom_sp_pv_pct) || '%；' || chr(13) || chr(10) || 
'日vv ' || dly_sp_vv || '万次，同比上月当日' || case when mom_sp_vv_pct >= 0 then '增' else '降' end  || abs(mom_sp_vv_pct) || '%；' || chr(13) || chr(10) || 
'' || chr(13) || chr(10) || 

'2、咪咕影院' || chr(13) || chr(10) || 
'日活跃' || dly_yy_visit_user || '万户；' || chr(13) || chr(10) || 
'月累计' || mtd_yy_visit_user || '万户，同比上月' || case when mom_yy_visit_user_pct >= 0 then '增' else '降' end  || abs(mom_yy_visit_user_pct) || '%；' || chr(13) || chr(10) || 
'活跃系数' || mon_yy_active_rate  || '%，同比上月' || case when mom_yy_active_rate >= 0 then '增' else '降' end  || abs(mom_yy_active_rate) || '%；' || chr(13) || chr(10) || 
'日pv ' || dly_yy_pv || '万次，同比上月当日' || case when mom_yy_pv_pct >= 0 then '增' else '降' end  || abs(mom_yy_pv_pct) || '%；' || chr(13) || chr(10) || 
'日vv ' || dly_yy_vv || '万次，同比上月当日' || case when mom_yy_vv_pct >= 0 then '增' else '降' end  || abs(mom_yy_vv_pct) || '%；' || chr(13) || chr(10) || 
'' || chr(13) || chr(10) || 

'3、咪咕直播' || chr(13) || chr(10) || 
'日活跃' || dly_zb_visit_user || '万户；' || chr(13) || chr(10) || 
'月累计' || mtd_zb_visit_user || '万户，同比上月' || case when mom_zb_visit_user_pct >= 0 then '增' else '降' end  || abs(mom_zb_visit_user_pct) || '%；' || chr(13) || chr(10) || 
'活跃系数' || mon_zb_active_rate  || '%，同比上月' || case when mom_zb_active_rate >= 0 then '增' else '降' end  || abs(mom_zb_active_rate) || '%；' || chr(13) || chr(10) || 
'日pv ' || dly_zb_pv || '万次，同比上月当日' || case when mom_zb_pv_pct >= 0 then '增' else '降' end  || abs(mom_zb_pv_pct) || '%；' || chr(13) || chr(10) || 
'日vv ' || dly_zb_vv || '万次，同比上月当日' || case when mom_zb_vv_pct >= 0 then '增' else '降' end  || abs(mom_zb_vv_pct) || '%；' || chr(13) || chr(10) || 
'' || chr(13) || chr(10) || 

'4、内容输出' || chr(13) || chr(10) || 
'日活跃' || dly_3rd_visit_user || '万户；' || chr(13) || chr(10) || 
 '' || chr(13) || chr(10) || 
 
'三、全网渠道拓展情况：' || chr(13) || chr(10) || 
'1、访问用户数：' || chr(13) || chr(10) || 
'合作渠道：' || dly_co_chn_visit_user_all || '万户（号码' || dly_co_chn_visit_user || '万，游客' || dly_co_chn_visit_trst || '万）；' || chr(13) || chr(10) || 
'拓展渠道：' || dly_ex_chn_visit_user_all || '万户（号码' || dly_ex_chn_visit_user || '万，游客' || dly_ex_chn_visit_trst || '万）；' || chr(13) || chr(10) || 
'自有渠道：' || dly_ow_chn_visit_user_all || '万户（号码' || dly_ow_chn_visit_user || '万，游客' || dly_ow_chn_visit_trst || '万）；' || chr(13) || chr(10) || 
'省公司：' || dly_pr_chn_visit_user_all || '万户（号码' || dly_pr_chn_visit_user || '万，游客' || dly_pr_chn_visit_trst || '万）；' || chr(13) || chr(10) || 
'其他：' || dly_ot_chn_visit_user_all || '万户（号码' || dly_ot_chn_visit_user || '万，游客' || dly_ot_chn_visit_trst || '万）；' || chr(13) || chr(10) || 
'' || chr(13) || chr(10) || 
 
'2、使用用户数：' || chr(13) || chr(10) || 
'合作渠道：' || dly_co_chn_use_user_all || '万户（号码' || dly_co_chn_use_user || '万，游客' || dly_co_chn_use_trst || '万）；' || chr(13) || chr(10) || 
'拓展渠道：' || dly_ex_chn_use_user_all || '万户（号码' || dly_ex_chn_use_user || '万，游客' || dly_ex_chn_use_trst || '万）；' || chr(13) || chr(10) || 
'自有渠道：' || dly_ow_chn_use_user_all || '万户（号码' || dly_ow_chn_use_user || '万，游客' || dly_ow_chn_use_trst || '万）；' || chr(13) || chr(10) || 
'省公司：' || dly_pr_chn_use_user_all || '万户（号码' || dly_pr_chn_use_user || '万，游客' || dly_pr_chn_use_trst || '万）；' || chr(13) || chr(10) || 
'其他：' || dly_ot_chn_use_user_all || '万户（号码' || dly_ot_chn_use_user || '万，游客' || dly_ot_chn_use_trst || '万）；' || chr(13) || chr(10) || 
'' || chr(13) || chr(10) || 


'3、节目使用次数：' || chr(13) || chr(10) || 
'合作渠道：' || dly_co_chn_user_all_vv || '万户（号码' || dly_co_chn_user_vv || '万，游客' || dly_co_chn_trst_vv || '万）；' || chr(13) || chr(10) || 
'拓展渠道：' || dly_ex_chn_user_all_vv || '万户（号码' || dly_ex_chn_user_vv || '万，游客' || dly_ex_chn_trst_vv || '万）；' || chr(13) || chr(10) || 
'自有渠道：' || dly_ow_chn_user_all_vv || '万户（号码' || dly_ow_chn_user_vv || '万，游客' || dly_ow_chn_trst_vv || '万）；' || chr(13) || chr(10) || 
'省公司：' || dly_pr_chn_user_all_vv || '万户（号码' || dly_pr_chn_user_vv || '万，游客' || dly_pr_chn_trst_vv || '万）；' || chr(13) || chr(10) || 
'其他：' || dly_ot_chn_user_all_vv || '万户（号码' || dly_ot_chn_user_vv || '万，游客' || dly_ot_chn_trst_vv || '万）；' || chr(13) || chr(10) || 
'' || chr(13) || chr(10) || 

'4、新增收入：' || chr(13) || chr(10) || 
'合作渠道：' || dly_co_chn_income || '万元，月累计：' || mtd_co_chn_income || '万元，同比' || case when mom_co_chn_income_pct >= 0 then '增' else '降' end || abs(mom_co_chn_income_pct) || '%；' || chr(13) || chr(10) || 
'拓展渠道：' || dly_ex_chn_income || '万元，月累计：' || mtd_ex_chn_income || '万元，同比' || case when mom_ex_chn_income_pct >= 0 then '增' else '降' end || abs(mom_ex_chn_income_pct) || '%；' || chr(13) || chr(10) || 
'自有渠道：' || dly_ow_chn_income || '万元，月累计：' || mtd_ow_chn_income || '万元，同比' || case when mom_ow_chn_income_pct >= 0 then '增' else '降' end || abs(mom_ow_chn_income_pct) || '%；' || chr(13) || chr(10) || 
'省公司：' || dly_pr_chn_income || '万元，月累计：' || mtd_pr_chn_income || '万元，同比' || case when mom_pr_chn_income_pct >= 0 then '增' else '降' end || abs(mom_pr_chn_income_pct) || '%；' || chr(13) || chr(10) || 
'其他：' || dly_ot_chn_income || '万元，月累计：' || mtd_ot_chn_income || '万元，同比' || case when mom_ot_chn_income_pct >= 0 then '增' else '降' end || abs(mom_ot_chn_income_pct) || '%；'
as mms
from cdmp_dmt.fact_mms_full_daily_v;



