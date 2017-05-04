	新设备与新账号表及周/月指标统计
   rptdata.fact_kesheng_sdk_new_user_hourly 
   rptdata.fact_kesheng_sdk_new_device_hourly 
   rptdata.fact_kesheng_sdk_new_active_weekly
   rptdata.fact_kesheng_sdk_new_active_monthly
   app.cpa_new_active_monthly 
   app.cpa_new_active_weekly 
   
-- == app.cpa_new_active_weekly  =======================================================================   
   
set mapreduce.job.name=app.cpa_new_active_weekly_${WEEK_START_DAY}_${WEEK_END_DAY};
set hive.merge.mapredfiles=true;

insert overwrite table app.cpa_new_active_weekly partition(src_file_week='${WEEK_START_DAY}')
select '${WEEK_START_DAY}' week_start_day    
      ,'${WEEK_END_DAY}' week_end_day
      ,if(t1.product_key=-1,'-1',nvl(d1.product_name,'')) product_name                                     
      ,t1.product_key                                                                                      
      ,t1.app_ver_code                                                                                     
      ,if(t1.app_channel_id='-1','-1',nvl(d2.chn_name,'')) chn_name                                        
      ,t1.app_channel_id                                                                                   
      ,t1.add_device_num              -- 新增设备数	                                                            	
      ,t1.add_user_num                -- 新增账号数	                                                            
      ,t1.active_device_num           -- 活跃设备数	
      ,t1.active_user_num             -- 活跃账号数	
      ,t1.start_cnt                   -- 启动次数	
      ,if(t1.active_device_num = 0, 0, round(t1.add_device_num*100/t1.active_device_num,2)) add_device_rate      -- 新增设备占比
      ,if(t1.active_user_num = 0, 0, round(t1.add_user_num*100/t1.active_user_num,2)) add_user_rate              -- 新增账号占比 
  from rptdata.fact_kesheng_sdk_new_active_weekly t1
  left join mscdata.dim_kesheng_sdk_product d1
    on t1.product_key = d1.product_key
  left join rptdata.dim_chn d2
    on t1.app_channel_id = d2.chn_id
 where t1.src_file_week = '${WEEK_START_DAY}'
   and (d1.product_key is not null or t1.product_key = -1)
   and (d2.chn_id is not null or t1.app_channel_id = '-1')
   and (t1.app_ver_code rlike '^[\\w\\.]+$' or t1.app_ver_code = '-1');   

-- == rptdata.fact_kesheng_sdk_new_active_weekly  =======================================================================  

set mapreduce.job.name=rptdata.fact_kesheng_sdk_new_active_weekly_${WEEK_START_DAY}_${WEEK_END_DAY};

set hive.exec.dynamic.partition.mode=nonstrict;		-- 这个是设置自动分区

insert overwrite table rptdata.fact_kesheng_sdk_new_active_weekly partition(src_file_week='${WEEK_START_DAY}')
select t0.app_channel_id, nvl(t0.product_key,-998), t0.app_ver_code
      ,sum(t0.add_device_num) add_device_num
      ,sum(t0.add_user_num) add_user_num
      ,sum(t0.active_device_num) active_device_num
      ,sum(t0.active_user_num) active_user_num
      ,sum(t0.start_cnt) start_cnt
  from (select t1.app_channel_id, t1.product_key, t1.app_ver_code
              ,0 add_device_num
              ,0 add_user_num
              ,t1.active_device_num
              ,t1.active_user_num
              ,0 start_cnt
          from stg.fact_kesheng_sdk_new_active_weekly_01 t1
         union all
        select t2.app_channel_id, t2.product_key, t2.app_ver_code
              ,t2.add_device_num		--从新增活跃表中取 新增设备数
              ,t2.add_user_num			--从新增活跃表中取 新增账号数
              ,0 active_device_num
              ,0 active_user_num
              ,0 start_cnt
          from rptdata.fact_kesheng_sdk_new_active_daily t2
         where t2.src_file_day >= '${WEEK_START_DAY}'
           and t2.src_file_day <= '${WEEK_END_DAY}'
         union all
        select t3.app_channel_id, t3.product_key, t3.app_ver_code
              ,0 add_device_num
              ,0 add_user_num
              ,0 active_device_num
              ,0 active_user_num
              ,t3.start_cnt			-- 从错误表中取 启动次数
          from rptdata.fact_kesheng_sdk_session_error_daily t3
         where t3.src_file_day >= '${WEEK_START_DAY}'
           and t3.src_file_day <= '${WEEK_END_DAY}'
       ) t0
 group by t0.app_channel_id, nvl(t0.product_key,-998), t0.app_ver_code;
-----------------------------------------------------------------------------------------------------

set mapreduce.job.name=stg.fact_kesheng_sdk_new_active_weekly_01_${WEEK_START_DAY}_${WEEK_END_DAY}_user;

insert into table stg.fact_kesheng_sdk_new_active_weekly_01
select if(substr(t2.grain_ind,1,1) = '0', '-1', t2.app_channel_id) app_channel_id
      ,if(substr(t2.grain_ind,2,1) = '0', -1, t2.product_key) product_key
      ,if(substr(t2.grain_ind,3,1) = '0', '-1', t2.app_ver_code) app_ver_code     
      ,0 active_device_num
      ,count(distinct t2.user_id)  active_user_num	-- 从新增账号表中取 活跃账号数	
  from rptdata.fact_kesheng_sdk_new_user_hourly t2
 where t2.src_file_day >= '${WEEK_START_DAY}'
   and t2.src_file_day <= '${WEEK_END_DAY}'
 group by if(substr(t2.grain_ind,1,1) = '0', '-1', t2.app_channel_id)
         ,if(substr(t2.grain_ind,2,1) = '0', -1, t2.product_key)
         ,if(substr(t2.grain_ind,3,1) = '0', '-1', t2.app_ver_code) 
-----------------------------------------------------------------------------------------------------

set mapreduce.job.name=stg.fact_kesheng_sdk_new_active_weekly_01_${WEEK_START_DAY}_${WEEK_END_DAY}_device;

insert overwrite table stg.fact_kesheng_sdk_new_active_weekly_01
select 
       if(substr(t1.grain_ind,1,1) = '0', '-1', t1.app_channel_id) app_channel_id
      ,if(substr(t1.grain_ind,2,1) = '0', -1, t1.product_key) product_key
      ,if(substr(t1.grain_ind,3,1) = '0', '-1', t1.app_ver_code) app_ver_code     
      ,count(distinct t1.device_key) active_device_num		-- 从新增设备表中取 活跃设备数	
      ,0 active_user_num
  from rptdata.fact_kesheng_sdk_new_device_hourly t1
 where t1.src_file_day >= '${WEEK_START_DAY}'
   and t1.src_file_day <= '${WEEK_END_DAY}'
 group by if(substr(t1.grain_ind,1,1) = '0', '-1', t1.app_channel_id)
         ,if(substr(t1.grain_ind,2,1) = '0', -1, t1.product_key)
         ,if(substr(t1.grain_ind,3,1) = '0', '-1', t1.app_ver_code) 


################################################################################################################################

-- == app.cpa_new_active_monthly  =======================================================================

set mapreduce.job.name=app.cpa_new_active_monthly_${SRC_FILE_MONTH};
set hive.merge.mapredfiles=true;

insert overwrite table app.cpa_new_active_monthly partition(src_file_month='${SRC_FILE_MONTH}')
select '${MONTH_START_DAY}' week_start_day    
      ,'${MONTH_END_DAY}' week_end_day
      ,if(t1.product_key=-1,'-1',nvl(d1.product_name,'')) product_name
      ,t1.product_key
      ,t1.app_ver_code
      ,if(t1.app_channel_id='-1','-1',nvl(d2.chn_name,'')) chn_name
      ,t1.app_channel_id
      ,t1.add_device_num    
      ,t1.add_user_num      
      ,t1.active_device_num 
      ,t1.active_user_num   
      ,t1.start_cnt         
      ,if(t1.active_device_num = 0, 0, round(t1.add_device_num*100/t1.active_device_num,2)) add_device_rate   
      ,if(t1.active_user_num = 0, 0, round(t1.add_user_num*100/t1.active_user_num,2)) add_user_rate
  from rptdata.fact_kesheng_sdk_new_active_monthly t1
  left join mscdata.dim_kesheng_sdk_product d1
    on t1.product_key = d1.product_key
  left join rptdata.dim_chn d2
    on t1.app_channel_id = d2.chn_id
 where t1.src_file_month = '${SRC_FILE_MONTH}'
   and (d1.product_key is not null or t1.product_key = -1)
   and (d2.chn_id is not null or t1.app_channel_id = '-1')
   and (t1.app_ver_code rlike '^[\\w\\.]+$' or t1.app_ver_code = '-1');

-- == rptdata.fact_kesheng_sdk_new_active_monthly  =======================================================================

set mapreduce.job.name=rptdata.fact_kesheng_sdk_new_active_monthly_${SRC_FILE_MONTH};

set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table rptdata.fact_kesheng_sdk_new_active_monthly partition(src_file_month='${SRC_FILE_MONTH}')
select t0.app_channel_id, nvl(t0.product_key,-998) product_key, t0.app_ver_code
      ,sum(t0.add_device_num) add_device_num
      ,sum(t0.add_user_num) add_user_num
      ,sum(t0.active_device_num) active_device_num
      ,sum(t0.active_user_num) active_user_num
      ,sum(t0.start_cnt) start_cnt
  from (select t1.app_channel_id, t1.product_key, t1.app_ver_code
              ,0 add_device_num
              ,0 add_user_num
              ,t1.active_device_num
              ,t1.active_user_num
              ,0 start_cnt
          from stg.fact_kesheng_sdk_new_active_monthly_01 t1
         union all
        select t2.app_channel_id, t2.product_key, t2.app_ver_code
              ,t2.add_device_num
              ,t2.add_user_num
              ,0 active_device_num
              ,0 active_user_num
              ,0 start_cnt
          from rptdata.fact_kesheng_sdk_new_active_daily t2
         where t2.src_file_day >= '${MONTH_START_DAY}'
           and t2.src_file_day <= '${MONTH_END_DAY}'
         union all
        select t3.app_channel_id, t3.product_key, t3.app_ver_code
              ,0 add_device_num
              ,0 add_user_num
              ,0 active_device_num
              ,0 active_user_num
              ,t3.start_cnt
          from rptdata.fact_kesheng_sdk_session_error_daily t3
         where t3.src_file_day >= '${MONTH_START_DAY}'
           and t3.src_file_day <= '${MONTH_END_DAY}'
       ) t0
 group by t0.app_channel_id, nvl(t0.product_key,-998), t0.app_ver_code;
 
----------------------------------------------------------------------------------
set mapreduce.job.name=stg.fact_kesheng_sdk_new_active_monthly_01_${SRC_FILE_MONTH}_user;

insert into table stg.fact_kesheng_sdk_new_active_monthly_01
select if(substr(t2.grain_ind,1,1) = '0', '-1', t2.app_channel_id) app_channel_id
      ,if(substr(t2.grain_ind,2,1) = '0', -1, t2.product_key) product_key
      ,if(substr(t2.grain_ind,3,1) = '0', '-1', t2.app_ver_code) app_ver_code   
      ,0 active_device_num
      ,count(distinct t2.user_id)  active_user_num
  from rptdata.fact_kesheng_sdk_new_user_hourly t2
 where t2.src_file_day >= '${MONTH_START_DAY}'
   and t2.src_file_day <= '${MONTH_END_DAY}'
 group by if(substr(t2.grain_ind,1,1) = '0', '-1', t2.app_channel_id)
         ,if(substr(t2.grain_ind,2,1) = '0', -1, t2.product_key)
         ,if(substr(t2.grain_ind,3,1) = '0', '-1', t2.app_ver_code)  ;
		 
----------------------------------------------------------------------------------
set mapreduce.job.name=stg.fact_kesheng_sdk_new_active_monthly_01_${SRC_FILE_MONTH}_device;

insert overwrite table stg.fact_kesheng_sdk_new_active_monthly_01
select if(substr(t1.grain_ind,1,1) = '0', '-1', t1.app_channel_id) app_channel_id
      ,if(substr(t1.grain_ind,2,1) = '0', -1, t1.product_key) product_key
      ,if(substr(t1.grain_ind,3,1) = '0', '-1', t1.app_ver_code) app_ver_code   
      ,count(distinct t1.device_key) active_device_num
      ,0 active_user_num
  from rptdata.fact_kesheng_sdk_new_device_hourly t1
 where t1.src_file_day >= '${MONTH_START_DAY}'
   and t1.src_file_day <= '${MONTH_END_DAY}'
 group by if(substr(t1.grain_ind,1,1) = '0', '-1', t1.app_channel_id)
         ,if(substr(t1.grain_ind,2,1) = '0', -1, t1.product_key)
         ,if(substr(t1.grain_ind,3,1) = '0', '-1', t1.app_ver_code);


		 