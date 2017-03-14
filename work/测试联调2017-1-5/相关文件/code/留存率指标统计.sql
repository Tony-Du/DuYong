
留存率指标统计  
   rptdata.fact_kesheng_sdk_device_retention_daily 
   rptdata.fact_kesheng_sdk_device_retention_weekly
   rptdata.fact_kesheng_sdk_device_retention_monthly
   app.cpa_user_retention_daily
   app.cpa_user_retention_monthly
   app.cpa_user_retention_weekly


-- == app.cpa_user_retention_daily ======================================================================= 

set mapreduce.job.name=app.cpa_user_retention_daily_${SRC_FILE_DAY};
set hive.groupby.skewindata=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.optimize.skewjoin=true;
set hive.merge.mapredfiles=true;

INSERT OVERWRITE TABLE app.cpa_user_retention_daily partition(src_file_day='${SRC_FILE_DAY}')
select x.stat_day,
       if(x.product_key = -1, '-1', nvl(p.product_name, ''))  term_prod_name,
       x.product_key as term_prod_id,
       if(x.app_channel_id = '-1', '-1', nvl(c.chn_name, ''))  app_channel_name,
       x.app_ver_code,
       x.app_channel_id,
       sum(new_device_cnt) new_device_cnt,
       case when sum(new_device_cnt) = 0 then 0 else round(sum(d1_retention_cnt)*100 / sum(new_device_cnt), 2) end d1_retention_pct,
       case when sum(new_device_cnt) = 0 then 0 else round(sum(d2_retention_cnt)*100 / sum(new_device_cnt), 2) end d2_retention_pct,
       case when sum(new_device_cnt) = 0 then 0 else round(sum(d3_retention_cnt)*100 / sum(new_device_cnt), 2) end d3_retention_pct,
       case when sum(new_device_cnt) = 0 then 0 else round(sum(d4_retention_cnt)*100 / sum(new_device_cnt), 2) end d4_retention_pct,
       case when sum(new_device_cnt) = 0 then 0 else round(sum(d5_retention_cnt)*100 / sum(new_device_cnt), 2) end d5_retention_pct,
       case when sum(new_device_cnt) = 0 then 0 else round(sum(d6_retention_cnt)*100 / sum(new_device_cnt), 2) end d6_retention_pct,
       case when sum(new_device_cnt) = 0 then 0 else round(sum(d7_retention_cnt)*100 / sum(new_device_cnt), 2) end d7_retention_pct,
       case when sum(new_device_cnt) = 0 then 0 else round(sum(d14_retention_cnt)*100 / sum(new_device_cnt), 2) end d14_retention_pct,
       case when sum(new_device_cnt) = 0 then 0 else round(sum(d30_retention_cnt)*100 / sum(new_device_cnt), 2) end d30_retention_pct
from
(       
select src_file_day as stat_day, 
       app_channel_id, 
       product_key, 
       app_ver_code, 
       new_device_cnt, 
       0 as d1_retention_cnt,
       0 as d2_retention_cnt,
       0 as d3_retention_cnt,
       0 as d4_retention_cnt,
       0 as d5_retention_cnt,
       0 as d6_retention_cnt,
       0 as d7_retention_cnt,
       0 as d14_retention_cnt,
       0 as d30_retention_cnt,
       src_file_day
from rptdata.fact_kesheng_sdk_new_daily
where src_file_day in (from_unixtime((unix_timestamp('${SRC_FILE_DAY}', 'yyyyMMdd') - 1 * 86400), 'yyyyMMdd')	-- 一天
                      ,from_unixtime((unix_timestamp('${SRC_FILE_DAY}', 'yyyyMMdd') - 2 * 86400), 'yyyyMMdd')
                      ,from_unixtime((unix_timestamp('${SRC_FILE_DAY}', 'yyyyMMdd') - 3 * 86400), 'yyyyMMdd')
                      ,from_unixtime((unix_timestamp('${SRC_FILE_DAY}', 'yyyyMMdd') - 4 * 86400), 'yyyyMMdd')
                      ,from_unixtime((unix_timestamp('${SRC_FILE_DAY}', 'yyyyMMdd') - 5 * 86400), 'yyyyMMdd')
                      ,from_unixtime((unix_timestamp('${SRC_FILE_DAY}', 'yyyyMMdd') - 6 * 86400), 'yyyyMMdd')
                      ,from_unixtime((unix_timestamp('${SRC_FILE_DAY}', 'yyyyMMdd') - 7 * 86400), 'yyyyMMdd')
                      ,from_unixtime((unix_timestamp('${SRC_FILE_DAY}', 'yyyyMMdd') - 14 * 86400), 'yyyyMMdd')
                      ,from_unixtime((unix_timestamp('${SRC_FILE_DAY}', 'yyyyMMdd') - 30 * 86400), 'yyyyMMdd') )
union all
select
    translate(become_new_dw_day,'-','') as stat_day, 
    case when substr(grain_ind, 1, 1) = '1' then app_channel_id else '-1' end app_channel_id,
    case when substr(grain_ind, 2, 1) = '1' then product_key else -1 end product_key,    
    case when substr(grain_ind, 3, 1) = '1' then app_ver_code else '-1' end app_ver_code,  
    0 new_device_cnt, 
    case when unix_timestamp(src_file_day, 'yyyyMMdd') - unix_timestamp(become_new_dw_day, 'yyyy-MM-dd') = 1 * 86400 then retention_cnt else 0 end d1_retention_cnt,
    case when unix_timestamp(src_file_day, 'yyyyMMdd') - unix_timestamp(become_new_dw_day, 'yyyy-MM-dd') = 2 * 86400 then retention_cnt else 0 end d2_retention_cnt,
    case when unix_timestamp(src_file_day, 'yyyyMMdd') - unix_timestamp(become_new_dw_day, 'yyyy-MM-dd') = 3 * 86400 then retention_cnt else 0 end d3_retention_cnt,
    case when unix_timestamp(src_file_day, 'yyyyMMdd') - unix_timestamp(become_new_dw_day, 'yyyy-MM-dd') = 4 * 86400 then retention_cnt else 0 end d4_retention_cnt,
    case when unix_timestamp(src_file_day, 'yyyyMMdd') - unix_timestamp(become_new_dw_day, 'yyyy-MM-dd') = 5 * 86400 then retention_cnt else 0 end d5_retention_cnt,
    case when unix_timestamp(src_file_day, 'yyyyMMdd') - unix_timestamp(become_new_dw_day, 'yyyy-MM-dd') = 6 * 86400 then retention_cnt else 0 end d6_retention_cnt,
    case when unix_timestamp(src_file_day, 'yyyyMMdd') - unix_timestamp(become_new_dw_day, 'yyyy-MM-dd') = 7 * 86400 then retention_cnt else 0 end d7_retention_cnt,
    case when unix_timestamp(src_file_day, 'yyyyMMdd') - unix_timestamp(become_new_dw_day, 'yyyy-MM-dd') = 14 * 86400 then retention_cnt else 0 end d14_retention_cnt,
    case when unix_timestamp(src_file_day, 'yyyyMMdd') - unix_timestamp(become_new_dw_day, 'yyyy-MM-dd') = 30 * 86400 then retention_cnt else 0 end d30_retention_cnt,
    src_file_day
from rptdata.fact_kesheng_sdk_device_retention_daily
where src_file_day between from_unixtime(unix_timestamp('${SRC_FILE_DAY}', 'yyyyMMdd') - 30 * 86400, 'yyyyMMdd') and '${SRC_FILE_DAY}'
      and unix_timestamp(become_new_dw_day, 'yyyy-MM-dd')
                       in (unix_timestamp('${SRC_FILE_DAY}', 'yyyyMMdd') - 1 * 86400
                          ,unix_timestamp('${SRC_FILE_DAY}', 'yyyyMMdd') - 2 * 86400
                          ,unix_timestamp('${SRC_FILE_DAY}', 'yyyyMMdd') - 3 * 86400
                          ,unix_timestamp('${SRC_FILE_DAY}', 'yyyyMMdd') - 4 * 86400
                          ,unix_timestamp('${SRC_FILE_DAY}', 'yyyyMMdd') - 5 * 86400
                          ,unix_timestamp('${SRC_FILE_DAY}', 'yyyyMMdd') - 6 * 86400
                          ,unix_timestamp('${SRC_FILE_DAY}', 'yyyyMMdd') - 7 * 86400
                          ,unix_timestamp('${SRC_FILE_DAY}', 'yyyyMMdd') - 14 * 86400
                          ,unix_timestamp('${SRC_FILE_DAY}', 'yyyyMMdd') - 30 * 86400 )
) x left join mscdata.dim_kesheng_sdk_product p on (x.product_key = p.product_key)
    left join rptdata.dim_chn c on (x.app_channel_id = c.chn_id)
where (p.product_key is not null or x.product_key = -1)
  and (c.chn_id is not null or x.app_channel_id = '-1')
  and (x.app_ver_code rlike '^[\\w\\.]+$' or x.app_ver_code = '-1')
group by x.stat_day,
         if(x.product_key = -1, '-1', nvl(p.product_name, '')),
         x.product_key,
         if(x.app_channel_id = '-1', '-1', nvl(c.chn_name, '')),
         x.app_ver_code,
         x.app_channel_id;

-- == rptdata.fact_kesheng_sdk_device_retention_daily ======================================================================= 		 
		 
set mapreduce.job.name=rptdata.fact_kesheng_sdk_device_retention_daily_${SRC_FILE_DAY};
set hive.groupby.skewindata=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.optimize.skewjoin=true;
set hive.merge.mapredfiles=true;

INSERT OVERWRITE TABLE rptdata.fact_kesheng_sdk_device_retention_daily partition(src_file_day, grain_ind)
select
  from_unixtime(floor(max(upload_unix_time) / 1000), 'yyyyMMdd') upload_day,
  from_unixtime(floor(max(become_new_unix_time) / 1000), 'yyyyMMdd') become_new_day,
  become_new_dw_day,
  case when substr(grain_ind, 1, 1) = '1' then app_channel_id else '-1' end app_channel_id,
  case when substr(grain_ind, 2, 1) = '1' then product_key else -1 end product_key,
  case when substr(grain_ind, 3, 1) = '1' then app_ver_code else '-1' end app_ver_code,  
  count(distinct device_key) retention_cnt,		-- 留存数
  src_file_day,
  grain_ind
from rptdata.fact_kesheng_sdk_new_device_hourly
where new_cnt = 0				-- 非新增
  and src_file_day = '${SRC_FILE_DAY}'
group by
  -- from_unixtime(floor(upload_unix_time / 1000), 'yyyyMMdd'),
  -- from_unixtime(floor(become_new_unix_time / 1000), 'yyyyMMdd'),
  become_new_dw_day,
  case when substr(grain_ind, 1, 1) = '1' then app_channel_id else '-1' end,
  case when substr(grain_ind, 2, 1) = '1' then product_key else -1 end,
  case when substr(grain_ind, 3, 1) = '1' then app_ver_code else '-1' end,
  src_file_day,
  grain_ind;		 

-- == rptdata.fact_kesheng_sdk_new_daily =======================================================================  
		 
set mapreduce.job.name=rptdata.fact_kesheng_sdk_new_daily_${SRC_FILE_DAY};
set hive.groupby.skewindata=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.optimize.skewjoin=true;
set hive.merge.mapredfiles=true;


with new_device as 
(     
select
  from_unixtime(floor(upload_unix_time / 1000), 'yyyyMMdd') upload_day,
  case when substr(grain_ind, 1, 1) = '1' then app_channel_id else '-1' end app_channel_id,
  case when substr(grain_ind, 2, 1) = '1' then product_key else -1 end product_key,
  case when substr(grain_ind, 3, 1) = '1' then app_ver_code else '-1' end app_ver_code,  
  sum(new_cnt) new_device_cnt,
  0 new_user_cnt,
  src_file_day,
  grain_ind
from rptdata.fact_kesheng_sdk_new_device_hourly
where new_cnt = 1
  and src_file_day = '${SRC_FILE_DAY}'
group by
  from_unixtime(floor(upload_unix_time / 1000), 'yyyyMMdd'),				-- upload_unix_time 单位是什么？？？  floor():向下舍入
  case when substr(grain_ind, 1, 1) = '1' then app_channel_id else '-1' end,
  case when substr(grain_ind, 2, 1) = '1' then product_key else -1 end,
  case when substr(grain_ind, 3, 1) = '1' then app_ver_code else '-1' end,
  src_file_day,
  grain_ind      
),
new_user as 
(
select
  from_unixtime(floor(upload_unix_time / 1000), 'yyyyMMdd') upload_day,
  case when substr(grain_ind, 1, 1) = '1' then app_channel_id else '-1' end app_channel_id,
  case when substr(grain_ind, 2, 1) = '1' then product_key else -1 end product_key,
  case when substr(grain_ind, 3, 1) = '1' then app_ver_code else '-1' end app_ver_code,  
  0 new_device_cnt,
  sum(new_cnt) new_user_cnt,
  src_file_day,
  grain_ind
from rptdata.fact_kesheng_sdk_new_user_hourly
where new_cnt = 1
  and src_file_day = '${SRC_FILE_DAY}'
group by
  from_unixtime(floor(upload_unix_time / 1000), 'yyyyMMdd'),
  case when substr(grain_ind, 1, 1) = '1' then app_channel_id else '-1' end,
  case when substr(grain_ind, 2, 1) = '1' then product_key else -1 end,
  case when substr(grain_ind, 3, 1) = '1' then app_ver_code else '-1' end,
  src_file_day,
  grain_ind     
)
INSERT OVERWRITE TABLE rptdata.fact_kesheng_sdk_new_daily partition(src_file_day, grain_ind)
select upload_day,
       app_channel_id, 
       product_key, 
       app_ver_code, 
       sum(new_device_cnt) new_device_cnt, 		-- 新增设备数
       sum(new_user_cnt) new_user_cnt, 			-- 新增账户数
       src_file_day, 
       grain_ind
from 
(
select * from new_device
union all 
select * from new_user
) t1
group by upload_day, app_channel_id, product_key, app_ver_code, src_file_day, grain_ind;		 
		 