
整体框架：
rptdata.fact_kesheng_sdk_session_start_hourly	|->	rptdata.fact_kesheng_sdk_new_active_hourly	|->	app.cpa_new_active_hourly
rptdata.fact_kesheng_sdk_new_user_hourly		|
rptdata.fact_kesheng_sdk_new_device_hourly		|


-- =================================== stg.fact_kesheng_sdk_new_active_hourly_01_device ====================================================================== --

insert overwrite table stg.fact_kesheng_sdk_new_active_hourly_01
select if(substr(t1.grain_ind,1,1) = '0', '-1', t1.app_channel_id) app_channel_id --如果渠道为'-1'表示不考虑该维度
      ,if(substr(t1.grain_ind,2,1) = '0', -1, t1.product_key) product_key
      ,if(substr(t1.grain_ind,3,1) = '0', '-1', t1.app_ver_code) app_ver_code     
      ,sum(case when t1.src_file_hour = '${EXTRACT_HOUR}' then t1.new_cnt else 0 end) add_device_num		--当前周期(小时)新增设备数
      ,0 add_user_num
      ,count(distinct case when t1.src_file_hour='${EXTRACT_HOUR}' then t1.device_key else null end) active_device_num		-- 当前周期活跃设备数	新增用户数和活跃用户数在00点时应该一样？？ 对device_key去重
      ,0 active_user_num
      ,0 start_cnt
      ,count(distinct t1.device_key) day_accu_active_user_num		-- 日统计活跃用户数：统计时间应该是一天的活跃用户数
      ,t1.grain_ind
  from (select nvl(trim(regexp_extract(a.app_channel_id,'([^-]+$)',1)),'-998') app_channel_id	 
              ,a.device_key ,a.product_key ,a.app_ver_code ,a.new_cnt, a.grain_ind
              ,a.src_file_hour
          from rptdata.fact_kesheng_sdk_new_device_hourly a	-- 新增设备小时表
         where a.src_file_day  = '${EXTRACT_DATE}'
           and a.src_file_hour <= '${EXTRACT_HOUR}'	 -- 该天内小于等于抽取时间的历史数据 
        ) t1
 group by if(substr(t1.grain_ind,1,1) = '0', '-1', t1.app_channel_id)
         ,if(substr(t1.grain_ind,2,1) = '0', -1, t1.product_key)
         ,if(substr(t1.grain_ind,3,1) = '0', '-1', t1.app_ver_code)
         ,t1.grain_ind;
-- 测试
		 select app_channel_id,product_key,app_ver_code,grain_ind from stg.fact_kesheng_sdk_new_active_hourly_01 limit 1000;
-- ====================== stg.fact_kesheng_sdk_new_active_hourly_01_user ============================================================================================ --
insert into table stg.fact_kesheng_sdk_new_active_hourly_01
select if(substr(t2.grain_ind,1,1) = '0', '-1', t2.app_channel_id) app_channel_id	--如果渠道id为'-1'表示不考虑该维度 
      ,if(substr(t2.grain_ind,2,1) = '0', -1, t2.product_key) product_key		--如果product_key为'-1'表示不考虑该维度
      ,if(substr(t2.grain_ind,3,1) = '0', '-1', t2.app_ver_code) app_ver_code	--如果 app_ver_code 为'-1'表示不考虑该维度
      ,0 add_device_num
      ,sum(t2.new_cnt) add_user_num
      ,0 active_device_num
      ,count(distinct t2.user_id) active_user_num
      ,0 start_cnt
      ,0 day_accu_active_user_num
      ,t2.grain_ind
  from (select nvl(trim(regexp_extract(a.app_channel_id,'([^-]+$)',1)),'-998') app_channel_id
              ,a.product_key ,a.app_ver_code ,a.new_cnt, a.grain_ind, a.user_id
          from rptdata.fact_kesheng_sdk_new_user_hourly a	-- 新增用户小时表
         where a.src_file_day  = '${EXTRACT_DATE}'
           and a.src_file_hour = '${EXTRACT_HOUR}' --等于抽取时间
        ) t2
 group by if(substr(t2.grain_ind,1,1) = '0', '-1', t2.app_channel_id)
         ,if(substr(t2.grain_ind,2,1) = '0', -1, t2.product_key)
         ,if(substr(t2.grain_ind,3,1) = '0', '-1', t2.app_ver_code)
         ,t2.grain_ind;
	
-- ================ stg.fact_kesheng_sdk_new_active_hourly_01_start ================================================================================================== --
insert into table stg.fact_kesheng_sdk_new_active_hourly_01
select if(substr(t5.grain_ind,1,1) = '0', '-1', t5.app_channel_id) app_channel_id
      ,if(substr(t5.grain_ind,2,1) = '0', -1, t5.product_key) product_key
      ,if(substr(t5.grain_ind,3,1) = '0', '-1', t5.app_ver_code) app_ver_code
      ,0 add_device_num
      ,0 add_user_num
      ,0 active_device_num
      ,0 active_user_num
      ,t5.start_cnt
      ,0 day_accu_active_user_num
      ,t5.grain_ind
  from (select t3.app_channel_id, t3.product_key, t3.app_ver_code
              ,sum(t3.start_cnt) start_cnt
              ,rpad(reverse(bin(cast(grouping__id as int))),3,'0') grain_ind	-- 粒度标识生成算法
        from rptdata.fact_kesheng_sdk_session_start_hourly t3
        where t3.src_file_day  = '${EXTRACT_DATE}' 
          and t3.src_file_hour = '${EXTRACT_HOUR}'		--等于抽取时间
        group by app_channel_id, product_key, app_ver_code
        grouping sets((), product_key, app_channel_id
                    ,(product_key, app_ver_code), (product_key, app_channel_id)
                    ,(product_key, app_ver_code, app_channel_id))
       ) t5;
	
-- ======================================================================================================================================== --	
insert overwrite table rptdata.fact_kesheng_sdk_new_active_hourly partition(src_file_day='${EXTRACT_DATE}', src_file_hour='${EXTRACT_HOUR}')
select if(substr(t0.grain_ind,1,1) = '0', '-1', t0.app_channel_id) app_channel_id	-- 如果grain_ind的第一位为0，app_channel_id，赋值为'-1',否则取app_channel_id的值
      ,if(substr(t0.grain_ind,2,1) = '0', -1, t0.product_key) product_key		-- 如果grain_ind的第二位为0，product_key，赋值为'-1',否则取product_key的值
      ,if(substr(t0.grain_ind,3,1) = '0', '-1', t0.app_ver_code) app_ver_code		-- 如果grain_ind的第三位为0，app_ver_code，赋值为'-1',否则取app_ver_code的值        
      ,sum(t0.add_device_num) add_device_num
      ,sum(t0.add_user_num) add_user_num
      ,sum(t0.active_device_num) active_device_num
      ,sum(t0.active_user_num) active_user_num
      ,sum(t0.start_cnt) start_cnt
      ,sum(t0.day_accu_active_user_num) day_accu_active_user_num
from stg.fact_kesheng_sdk_new_active_hourly_01 t0
group by if(substr(t0.grain_ind,1,1) = '0', '-1', t0.app_channel_id)
      ,if(substr(t0.grain_ind,2,1) = '0', -1, t0.product_key)
      ,if(substr(t0.grain_ind,3,1) = '0', '-1', t0.app_ver_code);

	  
-- ############################################################################################################################################################################## --		 
-- ############################################################################################################################################################################## --
		 
insert overwrite table app.cpa_new_active_hourly partition(src_file_day='${SRC_FILE_DAY}', src_file_hour='${SRC_FILE_HOUR}')
select concat(t1.src_file_day,t1.src_file_hour) stat_time
      ,if(t1.product_key=-1,'-1',nvl(d1.product_name,'')) product_name
      ,t1.product_key
      ,t1.app_ver_code
      ,if(t1.app_channel_id='-1','-1',nvl(d2.chn_name,''))  chn_name
      ,t1.app_channel_id      
      ,t1.add_device_num
      ,t1.add_user_num
      ,t1.active_device_num			-- 活跃设备数
      ,t1.active_user_num
      ,t1.start_cnt
      ,t1.day_accu_active_user_num		-- 天累计活跃用户数
      ,if(t1.active_device_num = 0,0,round(t1.add_device_num*100/t1.active_device_num,2)) add_device_rate	-- 每小时新增用户数占比 = 每小时新增用户 / 小时的活跃用户数
      ,if(t1.active_user_num = 0,0,round(t1.add_user_num*100/t1.active_user_num,2)) add_user_rate	-- 每小时新增账号数占比 = 每小时新增账号 / 小时的活跃账号数
  from rptdata.fact_kesheng_sdk_new_active_hourly t1
  left join mscdata.dim_kesheng_sdk_product d1
    on t1.product_key = d1.product_key
  left join rptdata.dim_chn d2
    on t1.app_channel_id = d2.chn_id
 where t1.src_file_day='${SRC_FILE_DAY}' and t1.src_file_hour='${SRC_FILE_HOUR}'
   and (d1.product_key is not null or t1.product_key=-1)		-- 排除t1.product_key不为空，而d1.product_key为null
   and (d2.chn_id is not null or t1.app_channel_id='-1')		
   and (t1.app_ver_code rlike '^[\\w\\.]+$' or t1.app_ver_code = '-1');	
   -- ^行的开头，\w 单词字符：[a-zA-Z_0-9]，\. 表示点本身，[\\w\\.]：点或者单词字符，$行的结尾，

select * from rptdata.fact_kesheng_sdk_new_device_hourly t1 where grain_ind='011' and device_key=383918;

-- rptdata.fact_kesheng_sdk_new_device_hourly 数据出现问题
-- rptdata.fact_kesheng_sdk_new_active_hourly
-- app.cpa_new_active_hourly	需要重跑数据





