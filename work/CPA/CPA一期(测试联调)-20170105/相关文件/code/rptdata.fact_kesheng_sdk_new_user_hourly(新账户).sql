整体框架：
`ods`.`kesheng_sdk_raw_ex` |-> `ods.kesheng_sdk_json_v` (sessionStart模块) |-> ods.kesheng_sdk_sessionstart_v

|-> intdata.kesheng_sdk_session_start |-> rptdata.fact_kesheng_sdk_new_user_hourly(use_id)

--------------------------------------------------------------------------------------------------------------------------------

INSERT OVERWRITE TABLE intdata.kesheng_sdk_session_start PARTITION(src_file_day, src_file_hour)
  SELECT 
        concat_ws('_', nvl(installationID, ''),  case when v.os = 'iOS' then  nvl(v.idfa, '') when v.os = 'AD' then nvl(v.imei, '') end) as device_id,
        v.os,
        v.imei,
        v.imsi,
        v.appPackageName,
        v.idfa,
        v.idfv,
        v.currentAppVersionCode,
        v.currentOSVersion,
        v.currentAppVersionName,
        v.phoneNumber,
        v.installationID,
        split(v.firstLaunchChannelId, '-')[length(v.firstLaunchChannelId) - length(translate(v.firstLaunchChannelId, '-', ''))] as first_launch_channel_id,
		-- 等价于 split(v.firstLaunchChannelId, '-')['-'的个数]
		--有以“-”分成的三段或只有一段；如果有三段则取最后一段；
        v.userId,
        v.account,
        cast (v.startTs as bigint) as start_unix_time,		-- 把startTs转换为 bigint 类型
        v.clientId,
        v.src_file_day,
        v.src_file_hour
FROM ods.kesheng_sdk_sessionstart_v v
WHERE  ((v.os = 'AD' and (v.installationID is not null or v.imei is not null)) 
          or (v.os = 'iOS' and (v.installationID is not null or v.idfa is not null)))
      and v.src_file_day = '${EXTRACT_DATE}'
      and v.src_file_hour = '${EXTRACT_HOUR}';
-- ###################################################################################################### --	  

INSERT OVERWRITE TABLE stg.fact_kesheng_sdk_new_user_hourly_01		-- 新账户表
SELECT t1.user_id, 
       t1.first_launch_channel_id AS app_channel_id,		-- 第一次发行的渠道ID 作为 app渠道ID
       nvl(t2.product_key,-998) product_key,		
       t1.app_ver_code,
       min(t1.start_unix_time) AS upload_unix_time, 	-- 把最小的 开始显示时间 作为 上报是时间
       t1.src_file_day,
       t1.src_file_hour
 FROM intdata.kesheng_sdk_session_start t1 
 LEFT JOIN mscdata.dim_kesheng_sdk_app_pkg t2 
   ON (t1.os = t2.app_os_type AND t1.app_pkg_name = t2.app_pkg_name)
WHERE t1.src_file_day = '${SRC_FILE_DAY}'
      and t1.src_file_hour = '${SRC_FILE_HOUR}'
      and t1.user_id is not null	-- 选取不为空的 user_id
      and t1.user_id <> ''
      and t1.user_id <> 'null'
group by t1.user_id,		
         t1.first_launch_channel_id,
         t2.product_key,
         t1.app_ver_code,
         t1.src_file_day,
         t1.src_file_hour;
	 
-- ================================================================================粒度000==== --

INSERT OVERWRITE TABLE stg.fact_kesheng_sdk_new_user_hourly_02 PARTITION(grain_ind)	-- 动态分区
select
  user_id,
  app_channel_id,
  product_key,
  app_ver_code,
  upload_unix_time,
  row_number() over(partition by user_id order by upload_unix_time) row_num,	-- 按 user_id 分组
  src_file_day,
  src_file_hour,
  '000' grain_ind
from stg.fact_kesheng_sdk_new_user_hourly_01;	-- 如果用group by user_id来分组的话，其他字段需要套在聚合函数里，所以这里使用row_number()函数

	-- ==================================================================================== --

INSERT OVERWRITE TABLE rptdata.fact_kesheng_sdk_new_user_hourly PARTITION(src_file_day, src_file_hour, grain_ind)	--动态分区
SELECT										-- 这个表中 新老用户都存在
  t1.user_id,
  t1.app_channel_id,
  t1.product_key,
  t1.app_ver_code,
  t1.upload_unix_time,  
  case when t2.user_id is null and row_num = 1 then 1 else 0 end new_cnt,		-- 新账号计数
  t2.become_new_unix_time,
  t1.src_file_day,
  t1.src_file_hour,
  t1.grain_ind
FROM stg.fact_kesheng_sdk_new_user_hourly_02 t1 
     left join (select user_id, 
                       max(case when new_cnt = 1 then upload_unix_time else null end) become_new_unix_time --如果是最开始的时候，表未有任何数据，则new_cnt为null，则become_new_unix_time为null
                from rptdata.fact_kesheng_sdk_new_user_hourly 
                where grain_ind = '000'
                      and (src_file_day >= translate(add_months(concat_ws('-', substr('${SRC_FILE_DAY}', 1, 4), substr('${SRC_FILE_DAY}', 5, 2), '01'), -6), '-', '') --6个月之前怎么表示
                             and src_file_day < '${SRC_FILE_DAY}'
                            or src_file_day = '${SRC_FILE_DAY}' and src_file_hour < '${SRC_FILE_HOUR}'
                          )       
                group by user_id) t2
     ON (t1.user_id = t2.user_id)
where t1.grain_ind = '000';

-- ******************************************************************************************粒度010***************** --

INSERT OVERWRITE TABLE stg.fact_kesheng_sdk_new_user_hourly_02 PARTITION(grain_ind)
select
  user_id,
  app_channel_id,
  product_key,
  app_ver_code,
  upload_unix_time,
  row_number() over(partition by user_id, product_key order by upload_unix_time) row_num,
  src_file_day,
  src_file_hour,
  '010' grain_ind
from stg.fact_kesheng_sdk_new_user_hourly_01;

	-- ============================================================================================ --
INSERT OVERWRITE TABLE rptdata.fact_kesheng_sdk_new_user_hourly PARTITION(src_file_day, src_file_hour, grain_ind)
SELECT
  t1.user_id,
  t1.app_channel_id,
  t1.product_key,
  t1.app_ver_code,
  t1.upload_unix_time,  
  case when t2.user_id is null and row_num = 1 then 1 else 0 end new_cnt,
  t2.become_new_unix_time,
  t1.src_file_day,
  t1.src_file_hour,
  t1.grain_ind
FROM stg.fact_kesheng_sdk_new_user_hourly_02 t1 
     left join (select product_key,
                       user_id, 
                       max(case when new_cnt = 1 then upload_unix_time else null end) become_new_unix_time 
                from rptdata.fact_kesheng_sdk_new_user_hourly 
                where grain_ind = '010'
                      and (src_file_day >= translate(add_months(concat_ws('-', substr('${SRC_FILE_DAY}', 1, 4), substr('${SRC_FILE_DAY}', 5, 2), '01'), -6), '-', '')
                             and src_file_day < '${SRC_FILE_DAY}'
                            or src_file_day = '${SRC_FILE_DAY}' and src_file_hour < '${SRC_FILE_HOUR}'
                          )       
                group by product_key,
                         user_id) t2
     ON (t1.user_id = t2.user_id AND t1.product_key = t2.product_key)
where t1.grain_ind = '010';

-- ***************************************************************************************************粒度011******** --
INSERT OVERWRITE TABLE stg.fact_kesheng_sdk_new_user_hourly_02 PARTITION(grain_ind)
select
  user_id,
  app_channel_id,
  product_key,
  app_ver_code,
  upload_unix_time,
  row_number() over(partition by user_id, product_key, app_ver_code order by upload_unix_time) row_num,
  src_file_day,
  src_file_hour,
  '011' grain_ind
from stg.fact_kesheng_sdk_new_user_hourly_01;

	-- ==================================================================================== --
INSERT OVERWRITE TABLE rptdata.fact_kesheng_sdk_new_user_hourly PARTITION(src_file_day, src_file_hour, grain_ind)
select
  t1.user_id,
  t1.app_channel_id,
  t1.product_key,
  t1.app_ver_code,
  t1.upload_unix_time,  
  case when t2.user_id is null and row_num = 1 then 1 else 0 end new_cnt,
  t2.become_new_unix_time,
  t1.src_file_day,
  t1.src_file_hour,
  t1.grain_ind
from stg.fact_kesheng_sdk_new_user_hourly_02 t1 
     left join (select product_key,
                       app_ver_code,
                       user_id, 
                       max(case when new_cnt = 1 then upload_unix_time else null end) become_new_unix_time 
                from rptdata.fact_kesheng_sdk_new_user_hourly 
                where grain_ind = '011'
                      and (src_file_day >= translate(add_months(concat_ws('-', substr('${SRC_FILE_DAY}', 1, 4), substr('${SRC_FILE_DAY}', 5, 2), '01'), -6), '-', '')
                             and src_file_day < '${SRC_FILE_DAY}'
                            or src_file_day = '${SRC_FILE_DAY}' and src_file_hour < '${SRC_FILE_HOUR}'
                          )           
                group by product_key,
                         app_ver_code,
                         user_id) t2
     on (t1.user_id = t2.user_id and t1.product_key = t2.product_key and t1.app_ver_code = t2.app_ver_code)
where t1.grain_ind = '011';
-- *********************************************************************************************************粒度100** --
INSERT OVERWRITE TABLE stg.fact_kesheng_sdk_new_user_hourly_02 PARTITION(grain_ind)
select
  user_id,
  app_channel_id,
  product_key,
  app_ver_code,
  upload_unix_time,
  row_number() over(partition by user_id order by upload_unix_time) row_num,
  src_file_day,
  src_file_hour,
  '100' grain_ind
from stg.fact_kesheng_sdk_new_user_hourly_01;
	-- ==================================================================================== --
INSERT OVERWRITE TABLE rptdata.fact_kesheng_sdk_new_user_hourly PARTITION(src_file_day, src_file_hour, grain_ind)
select
  t1.user_id,
  t1.app_channel_id,
  t1.product_key,
  t1.app_ver_code,
  t1.upload_unix_time,  
  case when t2.user_id is null and row_num = 1 then 1 else 0 end new_cnt,
  t2.become_new_unix_time,
  t1.src_file_day,
  t1.src_file_hour,
  t1.grain_ind
from stg.fact_kesheng_sdk_new_user_hourly_02 t1 
     left join (select user_id, 
                       max(case when new_cnt = 1 then upload_unix_time else null end) become_new_unix_time 
                from rptdata.fact_kesheng_sdk_new_user_hourly 
                where grain_ind = '100'
                      and (src_file_day >= translate(add_months(concat_ws('-', substr('${SRC_FILE_DAY}', 1, 4), substr('${SRC_FILE_DAY}', 5, 2), '01'), -6), '-', '')
                             and src_file_day < '${SRC_FILE_DAY}'
                            or src_file_day = '${SRC_FILE_DAY}' and src_file_hour < '${SRC_FILE_HOUR}'
                          )        
                group by user_id) t2
     on (t1.user_id = t2.user_id)
where t1.grain_ind = '100'



-- *********************************************************************************************************粒度110** --
INSERT OVERWRITE TABLE stg.fact_kesheng_sdk_new_user_hourly_02 PARTITION(grain_ind)
select
  user_id,
  app_channel_id,
  product_key,
  app_ver_code,
  upload_unix_time,
  row_number() over(partition by user_id, app_channel_id, product_key order by upload_unix_time) row_num,
  src_file_day,
  src_file_hour,
  '110' grain_ind
from stg.fact_kesheng_sdk_new_user_hourly_01;
	-- ==================================================================================== --
INSERT OVERWRITE TABLE rptdata.fact_kesheng_sdk_new_user_hourly PARTITION(src_file_day, src_file_hour, grain_ind)
select
  t1.user_id,
  t1.app_channel_id,
  t1.product_key,
  t1.app_ver_code,
  t1.upload_unix_time,  
  case when t2.user_id is null and row_num = 1 then 1 else 0 end new_cnt,
  t2.become_new_unix_time,
  t1.src_file_day,
  t1.src_file_hour,
  t1.grain_ind
from stg.fact_kesheng_sdk_new_user_hourly_02 t1 
     left join (select app_channel_id,
                       product_key,
                       user_id, 
                       max(case when new_cnt = 1 then upload_unix_time else null end) become_new_unix_time 
                from rptdata.fact_kesheng_sdk_new_user_hourly 
                where grain_ind = '110'
                      and (src_file_day >= translate(add_months(concat_ws('-', substr('${SRC_FILE_DAY}', 1, 4), substr('${SRC_FILE_DAY}', 5, 2), '01'), -6), '-', '')
                             and src_file_day < '${SRC_FILE_DAY}'
                            or src_file_day = '${SRC_FILE_DAY}' and src_file_hour < '${SRC_FILE_HOUR}'
                          )        
                group by app_channel_id,
                         product_key,
                         user_id) t2
     on (t1.user_id = t2.user_id and t1.app_channel_id = t2.app_channel_id and t1.product_key = t2.product_key)
where t1.grain_ind = '110';

-- *********************************************************************************************************粒度111** --

INSERT OVERWRITE TABLE stg.fact_kesheng_sdk_new_user_hourly_02 PARTITION(grain_ind)
select
  user_id,
  app_channel_id,
  product_key,
  app_ver_code,
  upload_unix_time,
  row_number() over(partition by user_id, app_channel_id, product_key, app_ver_code order by upload_unix_time) row_num,
  src_file_day,
  src_file_hour,
  '111' grain_ind
from stg.fact_kesheng_sdk_new_user_hourly_01;
	-- ==================================================================================== --
INSERT OVERWRITE TABLE rptdata.fact_kesheng_sdk_new_user_hourly PARTITION(src_file_day, src_file_hour, grain_ind)
select
  t1.user_id,
  t1.app_channel_id,
  t1.product_key,
  t1.app_ver_code,
  t1.upload_unix_time,  
  case when t2.user_id is null and row_num = 1 then 1 else 0 end new_cnt,-- 新账号计数
  t2.become_new_unix_time,
  t1.src_file_day,
  t1.src_file_hour,
  t1.grain_ind
from stg.fact_kesheng_sdk_new_user_hourly_02 t1 
     left join (select app_channel_id,
                       product_key,
                       app_ver_code,
                       user_id, 
                       max(case when new_cnt = 1 then upload_unix_time else null end) become_new_unix_time 
                from rptdata.fact_kesheng_sdk_new_user_hourly 
                where grain_ind = '111'
                      and (src_file_day >= translate(add_months(concat_ws('-', substr('${SRC_FILE_DAY}', 1, 4), substr('${SRC_FILE_DAY}', 5, 2), '01'), -6), '-', '')
                             and src_file_day < '${SRC_FILE_DAY}'
                            or src_file_day = '${SRC_FILE_DAY}' and src_file_hour < '${SRC_FILE_HOUR}'
                          )        
                group by app_channel_id,
                         product_key,
                         app_ver_code,
                         user_id) t2
     on (t1.user_id = t2.user_id and t1.app_channel_id = t2.app_channel_id and t1.product_key = t2.product_key and t1.app_ver_code = t2.app_ver_code)
where t1.grain_ind = '111';

	-- ================================================================================================================================== --
	
insert into table stg.fact_kesheng_sdk_new_active_hourly_01
select if(substr(t2.grain_ind,1,1) = '0', '-1', t2.app_channel_id) app_channel_id 
      ,if(substr(t2.grain_ind,2,1) = '0', -1, t2.product_key) product_key
      ,if(substr(t2.grain_ind,3,1) = '0', '-1', t2.app_ver_code) app_ver_code
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
		 
	-- ========================================================================================= -- 
		 
insert overwrite table rptdata.fact_kesheng_sdk_new_active_hourly partition(src_file_day='${EXTRACT_DATE}', src_file_hour='${EXTRACT_HOUR}')
select if(substr(t0.grain_ind,1,1) = '0', '-1', t0.app_channel_id) app_channel_id
      ,if(substr(t0.grain_ind,2,1) = '0', -1, t0.product_key) product_key
      ,if(substr(t0.grain_ind,3,1) = '0', '-1', t0.app_ver_code) app_ver_code        
      ,sum(t0.add_device_num) add_device_num		-- 这里的sum()仅仅是为了配合 group by 使用
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
		 -- 不同维度情况下所有的数据
insert overwrite table app.cpa_new_active_hourly partition(src_file_day='${SRC_FILE_DAY}', src_file_hour='${SRC_FILE_HOUR}')
select concat(t1.src_file_day,t1.src_file_hour) stat_time
      ,if(t1.product_key=-1,'-1',nvl(d1.product_name,'')) product_name
      ,t1.product_key
      ,t1.app_ver_code
      ,if(t1.app_channel_id='-1','-1',nvl(d2.chn_name,''))  chn_name
      ,t1.app_channel_id      
      ,t1.add_device_num					-- 新增用户数
      ,t1.add_user_num						-- 新增账号数
      ,t1.active_device_num					-- 活跃设备数
      ,t1.active_user_num           		-- 活跃账号数
      ,t1.start_cnt							-- 启动次数
      ,t1.day_accu_active_device_num		-- 天累计活跃用户数
      ,if(t1.active_device_num = 0,0,round(t1.add_device_num*100/t1.active_device_num,2)) add_device_rate	-- 新增用户占比（小时）
      ,if(t1.active_user_num = 0,0,round(t1.add_user_num*100/t1.active_user_num,2)) add_user_rate	-- 新增账号占比（小时）
  from rptdata.fact_kesheng_sdk_new_active_hourly t1
  left join mscdata.dim_kesheng_sdk_product d1
    on t1.product_key = d1.product_key
  left join rptdata.dim_chn d2
    on t1.app_channel_id = d2.chn_id
 where t1.src_file_day='${SRC_FILE_DAY}' and t1.src_file_hour='${SRC_FILE_HOUR}'
   and (d1.product_key is not null or t1.product_key=-1)
   and (d2.chn_id is not null or t1.app_channel_id='-1')
   and (t1.app_ver_code rlike '^[\\w\\.]+$' or t1.app_ver_code = '-1');
