

insert overwrite table stg.fact_kesheng_sdk_new_active_hourly_01
select if(t1.app_channel_id='','-998',t1.app_channel_id), t1.product_key, t1.app_ver_code
      ,sum(case when t1.src_file_hour = '${EXTRACT_HOUR}' 
                  then t1.new_cnt else 0 end) add_device_num	-- 新增设备数	new_cnt:新用户计数
      ,0 add_user_num											-- 新增账号数	为什么全为0
      ,sum(case when t1.src_file_hour = '${EXTRACT_HOUR}' 
                  then 1 else 0 end) active_device_num		-- 活跃设备数
      ,0 active_user_num									-- 活跃账号数
      ,0 start_cnt											-- 启动次数
      ,count(distinct t1.device_key) day_accu_active_user_num	-- 日累计活跃设备数
      ,t1.grain_ind
  from (select nvl(trim(regexp_extract(a.app_channel_id,'([^-]+$)',1)),'-998') app_channel_id  -- 抽取字符串a.app_channel_id中符合正则表达式的第1个子字符串
              ,a.device_key ,a.product_key ,a.app_ver_code ,a.new_cnt, a.grain_ind 
              ,a.src_file_hour
          from rptdata.fact_kesheng_sdk_new_device_hourly a
         where a.src_file_day  = '${EXTRACT_DATE}'
           and a.src_file_hour <= '${EXTRACT_HOUR}'		-- 小于等于这个小时的历史数据 
        ) t1
 group by if(t1.app_channel_id='','-998',t1.app_channel_id), t1.product_key, t1.app_ver_code, t1.grain_ind;
 
-- ############################################################################################################################################################################## --
 
 insert into table stg.fact_kesheng_sdk_new_active_hourly_01
select if(t2.app_channel_id='','-998',t2.app_channel_id) app_channel_id
      ,t2.product_key
	  ,t2.app_ver_code
      ,0 add_device_num
      ,sum(t2.new_cnt) add_user_num
      ,0 active_device_num
      ,count(1) active_user_num	
      ,0 start_cnt
      ,0 day_accu_active_device_num
      ,t2.grain_ind
  from (select nvl(trim(regexp_extract(a.app_channel_id,'([^-]+$)',1)),'-998') app_channel_id
              ,a.product_key ,a.app_ver_code ,a.new_cnt, a.grain_ind   --  new_cnt：新用户计数
          from rptdata.fact_kesheng_sdk_new_user_hourly a
         where a.src_file_day  = '${EXTRACT_DATE}'
           and a.src_file_hour = '${EXTRACT_HOUR}'   -- 只取当前周期的数据
        ) t2
 group by if(t2.app_channel_id='','-998',t2.app_channel_id), t2.product_key, t2.app_ver_code, t2.grain_ind;
 
 -- ############################################################################################################################################################################## --
 
 insert into table stg.fact_kesheng_sdk_new_active_hourly_01
select t3.app_channel_id, t3.product_key, t3.app_ver_code
      ,0 add_device_num
      ,0 add_user_num
      ,0 active_device_num
      ,0 active_user_num
      ,sum(t3.start_cnt) start_cnt
      ,0 day_accu_active_user_num
      ,rpad(reverse(bin(cast(grouping__id as int))),3,'0') grain_ind
from rptdata.fact_kesheng_sdk_session_start_hourly t3
where t3.src_file_day  = '${EXTRACT_DATE}' 
  and t3.src_file_hour = '${EXTRACT_HOUR}'
  and t3.product_key <> '-998'
group by app_channel_id, product_key, app_ver_code
grouping sets((), product_key, app_channel_id
            ,(product_key, app_ver_code), (product_key, app_channel_id)
            ,(product_key, app_ver_code, app_channel_id));
 
 -- ############################################################################################################################################################################## --
 
 insert overwrite table rptdata.fact_kesheng_sdk_new_active_hourly partition(src_file_day='${EXTRACT_DATE}', src_file_hour='${EXTRACT_HOUR}')
select if(substr(t0.grain_ind,1,1) = '0', '-1', t0.app_channel_id) app_channel_id
      ,if(substr(t0.grain_ind,2,1) = '0', -1, t0.product_key) product_key
      ,if(substr(t0.grain_ind,3,1) = '0', '-1', t0.app_ver_code) app_ver_code         
      ,sum(t0.add_device_num) add_device_num
      ,sum(t0.add_user_num) add_user_num
      ,sum(t0.active_device_num) active_device_num
      ,sum(t0.active_user_num) active_user_num
      ,sum(t0.start_cnt) start_cnt
      ,sum(t0.day_accu_active_user_num) day_accu_active_user_num
      ,t0.grain_ind 
from stg.fact_kesheng_sdk_new_active_hourly_01 t0
group by if(substr(t0.grain_ind,1,1) = '0', '-1', t0.app_channel_id)
      ,if(substr(t0.grain_ind,2,1) = '0', -1, t0.product_key)
      ,if(substr(t0.grain_ind,3,1) = '0', '-1', t0.app_ver_code);

