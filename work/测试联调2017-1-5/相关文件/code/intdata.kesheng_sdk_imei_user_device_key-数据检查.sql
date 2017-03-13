
CREATE VIEW `ods.kesheng_sdk_json_v` AS select translate(`k2`.`line`[0], '\u0000', '') `json`, `k2`.`line`[1] `client_ip`
      ,`k2`.`src_file_day`                        -- 20170111
      ,`k2`.`src_file_hour`                       -- 09
      ,`k2`.`input_file_name` ,`k2`.`block_offset_inside_file`
      ,concat(`k2`.`input_file_name`,':',`k2`.`block_offset_inside_file`) `rowkey`
  from (select split(`k1`.`line`,'\\|\\|') `line`
              ,`k1`.`src_file_day` ,`k1`.`src_file_hour`
              ,`k1`.`input__file__name` `input_file_name`  ,`k1`.`block__offset__inside__file` `block_offset_inside_file`
          from `ods`.`kesheng_sdk_raw_ex` `k1`
       ) `k2`


CREATE VIEW `ods.kesheng_sdk_sessionstart_v` AS SELECT concat(`s`.`input_file_name`, ':', `s`.`block_offset_inside_file`) as `rowkey`,
       `d`.`os`,
       `d`.`imei`,
       `d`.`imsi`,
       `d`.`apppackagename`,
       `d`.`idfa`,
       `d`.`idfv`,
       `d`.`currentappversioncode`,
       `d`.`currentosversion`,
       `d`.`currentappversionname`,
       `d`.`phonenumber`,
       `d`.`installationid`,
       `d`.`firstlaunchchannelid`,
       `d`.`userid`,
       `d`.`account`,
       `d`.`startts`,
       `d`.`clientid`,
       `s`.`src_file_day`,                      -- 20170111
       `s`.`src_file_hour`                      -- 09
FROM `ods`.`kesheng_sdk_json_v` `s`
LATERAL VIEW json_tuple(`s`.`json`, 'sessionStart') `p` as `sessionStart`
LATERAL VIEW json_tuple(`p`.`sessionstart`, 'os', 
                                      'imei', 
                                      'imsi', 
                                      'appPackageName', 
                                      'idfa', 
                                      'idfv', 
                                      'currentAppVersionCode', 
                                      'currentOSVersion', 
                                      'currentAppVersionName', 
                                      'phoneNumber', 
                                      'installationID', 
                                      'firstLaunchChannelId', 
                                      'userId', 
                                      'account', 
                                      'startTs', 
                                      'clientId') `d` 
                                      AS `os`, 
                                         `imei`, 
                                         `imsi`, 
                                         `appPackageName`, 
                                         `idfa`, 
                                         `idfv`, 
                                         `currentAppVersionCode`, 
                                         `currentOSVersion`, 
                                         `currentAppVersionName`, 
                                         `phoneNumber`, 
                                         `installationID`, 
                                         `firstLaunchChannelId`, 
                                         `userId`, 
                                         `account`, 
                                         `startTs`, 
                                         `clientId`
WHERE `p`.`sessionstart` is not null and `p`.`sessionstart` <> ''


-- 记录清洗规则：((v.os = 'AD' and (v.installationID is not null or v.imei is not null))
--          	or (v.os = 'iOS' and (v.installationID is not null or v.idfa is not null)))

add jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;
set mapreduce.job.name=intdata.kesheng_sdk_session_start_${EXTRACT_DATE}_${EXTRACT_HOUR};
set hive.enforce.bucketing=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

INSERT OVERWRITE TABLE intdata.kesheng_sdk_session_start PARTITION(src_file_day='${EXTRACT_DATE}', src_file_hour='${EXTRACT_HOUR}')
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
        v.userId,
        v.account,
        cast (v.startTs as bigint) as start_unix_time,
        v.clientId
FROM ods.kesheng_sdk_sessionstart_v v
WHERE  ((v.os = 'AD' and (v.installationID is not null or v.imei is not null)) 
          or (v.os = 'iOS' and (v.installationID is not null or v.idfa is not null)))
      and v.src_file_day = '${EXTRACT_DATE}'                   -- 20170111
      and v.src_file_hour = '${EXTRACT_HOUR}';                 -- 09

--firstLaunchChannelId: 23000104-99000-200300280000000	  
	  
-- ############################################################################################################################################################################### --

drop view if exists int.kesheng_sdk_session_start_cpa_v;
create view int.kesheng_sdk_session_start_cpa_v as
select a1.imei,a1.user_id.a1.idfa,a1.imsi,a1.app_ver_code,a1.phone_number 
      ,a1.app_pkg_name
      ,if(a1.app_channel_id = '', '-998', a1.app_channel_id) app_channel_id
      ,a1.app_os_type, a1.start_unix_time, a1.install_id, a1.first_launch_channel_id
      ,a1.src_file_day, a1.src_file_hour
  from (select  if(nvl(trim(a.imei),'') = '', '-998', trim(a.imei)) imei, -- 这里做了一个处理，imei、user_id、idfa、imsi如果为NULL或者为空字符串，赋值为 '-998'
                if(nvl(trim(a.user_id),'') = '', '-998', trim(a.user_id)) user_id,
                if(nvl(trim(a.idfa),'') = '', '-998', trim(a.idfa)) idfa,
                if(nvl(trim(a.imsi),'') = '', '-998', trim(a.imsi)) imsi,
                a.app_ver_code,
                if(nvl(trim(a.phone_number),'') = '', '-998', trim(a.phone_number)) phone_number,
                a.app_pkg_name,
                a.first_launch_channel_id,
                nvl(trim(regexp_extract(a.first_launch_channel_id,'([^-]+$)',1)),'-998') app_channel_id, -- [^-]：除了以-开头的任意一个，+：匹配前面的子表达式一次或者多次，$: 行的结尾 
                a.os as app_os_type,
                a.start_unix_time,
                a.install_id,
                a.src_file_day,                            -- 20170111
                a.src_file_hour                            -- 09
          from intdata.kesheng_sdk_session_start a
       ) a1;
	   	
	-- if(nvl(trim(regexp_extract(a.first_lanch_channel_id,[^-]+$,1)),'') = '', '-998',  nvl(trim(regexp_extract(a.first_lanch_channel_id,[^-]+$,1)),''))
-- ############################################################################################################################################################################## --
insert overwrite table stg.kesheng_sdk_active_device_hourly_01	-- 活跃设备小时表01
select a.app_os_type
      ,a.imei
      ,a.user_id
      ,a.imsi
      ,a.idfa
      ,a.phone_number
      ,a.src_file_day
      ,a.src_file_hour
  from int.kesheng_sdk_session_start_cpa_v a
  left join mscdata.cpa_phone_number_blacklist b		-- 电话号码黑名单
    on (a.phone_number = b.phone_number)				-- 能匹配上的都是不符合条件的
 where a.src_file_day = '${EXTRACT_DATE}'				-- 20170111
   and a.src_file_hour = '${EXTRACT_HOUR}'				-- 09
   and (a.app_os_type = 'AD' and length(a.imei) in (14,15) 
         and (a.user_id = '-998' and length(a.imsi) = 15 and a.imsi like '460%'		-- imei + imsi
              or a.user_id <> '-998' and b.phone_number is null  -- 非法号码剔除	-- imei + user_id
             )
        or a.app_os_type = 'iOS' and a.idfa <> '-998'		
           and (a.user_id = '-998' 								-- idfa
                or a.user_id <> '-998' and b.phone_number is null		-- idfa + user_id
               )
        )  
 group by a.app_os_type, a.imei, a.user_id, a.imsi, a.idfa		-- 只有当这些粒度都相同时才视为重复记录，会进行去重
         ,a.phone_number, a.src_file_day ,a.src_file_hour;	
		 
-- ===================================================================================================================== --		 
insert overwrite table stg.kesheng_sdk_active_device_hourly_02
select t2.device_key,		-- 存在为NULL的情况，即对应新用户
       t1.imei,
       t1.user_id,
       t1.imsi,
       t1.idfa,
       t1.phone_number,
       coalesce (t2.dw_crt_day, t1.src_file_day) dw_crt_day,		-- 设备key生成日期。当为新增用户,t2.dw_crt_day为NULL, 所以设备key生成日期用的是t1.src_file_day，即exract_day
       coalesce (t2.dw_crt_hour, t1.src_file_hour) dw_crt_hour,		-- 反之，不是新增用户用的是t2.dw_drt_hour
       t1.src_file_day,					-- 20170111
       t1.src_file_hour					-- 09
  from (select *
          from stg.kesheng_sdk_active_device_hourly_01 a
         where a.app_os_type = 'AD' and a.user_id <> '-998'  -- 用户ID不为空，对应情况就是 imei + user_id
       ) t1
  left join intdata.kesheng_sdk_imei_user_device_key t2       -- 20170111 09 之前的所有数据
    on (t1.imei = t2.imei and t1.user_id = t2.user_id);		 -- 当t1.imei和t1.user_id有任意一个在t2表中的对应的字段不符合on条件时，返回的t2.device_key为 NULL，说明该用户为新用户。
	
-- =====================================================================活跃设备小时表=================================== --
insert overwrite table intdata.kesheng_sdk_active_device_hourly partition(src_file_day,src_file_hour)
select t1.device_key,
       'AD' as app_os_type,
       t1.imei,
       t1.user_id,       
       t1.imsi,
       t1.idfa,
       t1.phone_number,
       t1.dw_crt_day,
       t1.dw_crt_hour,
       t1.src_file_day as dw_upd_day,
       t1.src_file_hour as dw_upd_hour,
       t1.src_file_day,	-- 20170111
       t1.src_file_hour	-- 09
  from stg.kesheng_sdk_active_device_hourly_02 t1
 where t1.device_key is not null		-- 老用户
 union all
select dense_rank() over(order by t2.imei,t2.user_id) + t3.device_key_max as device_key,	--	dense_rank()不会空出并列所占的名次：12234     
		-- 新的设备key生成的方式： 对t2的imei号和user_id进行排序，取序号，再加上最大的device_key，作为新增用户的device_key
       'AD' as app_os_type,
       t2.imei,
       t2.user_id,
       t2.imsi,
       t2.idfa, 
       t2.phone_number,
       t2.dw_crt_day,
       t2.dw_crt_hour,
       t2.dw_crt_day as dw_upd_day,
       t2.dw_crt_hour as dw_upd_hour,
       t2.src_file_day,	-- 20170111
       t2.src_file_hour	-- 09
  from (select * from stg.kesheng_sdk_active_device_hourly_02 a
         where device_key is null		-- 新增用户，并为其创建新的device_key
       ) t2
 cross join (select coalesce(max(device_key),0) device_key_max -- t2中所有的行都要连接t3中的行
               from intdata.kesheng_sdk_active_device_hourly b
              where b.src_file_day >= from_unixtime(unix_timestamp('${EXTRACT_DATE}','yyyyMMdd')-60*60*24*10,'yyyyMMdd')  -- 从过去的10天中选取最大的device_key，如果还是为NUll，则取0
            ) t3;
			
-- =============================================================================================== --

insert into intdata.kesheng_sdk_imei_user_device_key
select t1.device_key, t1.imei, t1.user_id, t1.dw_crt_day, t1.dw_crt_hour
  from intdata.kesheng_sdk_active_device_hourly t1		
  left join intdata.kesheng_sdk_imei_user_device_key t2	-- 健壮性：解决代码重复执行所带来的问题   
    on t2.device_key = t1.device_key
 where t1.src_file_day = '${EXTRACT_DATE}'               -- 20170111
   and t1.src_file_hour = '${EXTRACT_HOUR}'              -- 09
   and t1.app_os_type = 'AD'
   and t1.user_id <> '-998'
   and t1.dw_crt_day = t1.src_file_day		-- 设备key生成的时间也为EXTRACT_DATE,所以这里插进来的是当（小）时的新增用户
   and t1.dw_crt_hour = t1.src_file_hour	
   and t2.device_key is null				-- 考虑到所依赖的表会存在重复执行的问题，如果重复执行那么t2.device_key就不可能为null，所以就插入不了数据
 group by t1.device_key, t1.imei, t1.user_id, t1.dw_crt_day, t1.dw_crt_hour;
 
-- 选取得是当前周期的新增设备，以 imei + user_id为标识
-- 而且以后插进来的数据都是新增设备，表中的每一个device_key都是唯一的

