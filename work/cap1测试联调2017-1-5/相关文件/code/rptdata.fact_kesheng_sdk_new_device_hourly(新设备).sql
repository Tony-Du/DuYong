
整体框架：
`ods`.`kesheng_sdk_raw_ex` |-> `ods.kesheng_sdk_json_v` (sessionStart模块) |-> ods.kesheng_sdk_sessionstart_v

|-> intdata.kesheng_sdk_session_start                      |-> rptdata.fact_kesheng_sdk_new_device_hourly(device_key)
    intdata.kesheng_sdk_active_device_hourly(取device_key) |
	
--==================================================================================================================================

`ods`.`kesheng_sdk_raw_ex`
hdfs://ns1/user/hadoop/ods/kesheng


CREATE VIEW `ods.kesheng_sdk_json_v` AS select regexp_replace(`k2`.`line`[0], '\\\\n|\\\\r|\\\\u001f|\\\\u0000', '') `json`, `k2`.`line`[1] `client_ip`
      ,`k2`.`src_file_day`
      ,`k2`.`src_file_hour`
      ,`k2`.`input_file_name` ,`k2`.`block_offset_inside_file`
      ,concat(`k2`.`input_file_name`,':',`k2`.`block_offset_inside_file`) `rowkey`
  from (select split(`k1`.`line`,'\\|\\|') `line`
              ,`k1`.`src_file_day` ,`k1`.`src_file_hour`
              ,`k1`.`input__file__name` `input_file_name`  ,`k1`.`block__offset__inside__file` `block_offset_inside_file`
          from `ods`.`kesheng_sdk_raw_ex` `k1`
       ) `k2`
	   
--==================================================================================================================================

CREATE VIEW `ods.kesheng_sdk_sessionstart_v` AS SELECT `s`.`rowkey`,
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
       `s`.`src_file_day`,
       `s`.`src_file_hour`
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
WHERE nvl(`p`.`sessionstart`, '{}') <> '{}' and `p`.`sessionstart` <> '[]'

==================================================================================================================================

INSERT OVERWRITE TABLE intdata.kesheng_sdk_session_start PARTITION(src_file_day, src_file_hour)	-- 字段存在null或者空字符串的情况
  SELECT 
        concat_ws('_', nvl(installationID, ''),  case when v.os = 'iOS' then  nvl(v.idfa, '') when v.os = 'AD' then nvl(v.imei, '') end) as device_id,
		-- device_key 的形式：IOS对应installationID_idfa 或者 AD对应installationID_imei
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
        cast (v.startTs as bigint) as start_unix_time,	--  这个字段注意下！！  首次显示时间点时间,1970到目前为止的时间戳
        v.clientId,
        v.src_file_day,
        v.src_file_hour
FROM ods.kesheng_sdk_sessionstart_v v
WHERE  ((v.os = 'AD' and (v.installationID is not null or v.imei is not null)) 
          or (v.os = 'iOS' and (v.installationID is not null or v.idfa is not null)))
      and v.src_file_day = '${EXTRACT_DATE}'
      and v.src_file_hour = '${EXTRACT_HOUR}';
	  
44234234234_0BCA315A-D3D9-404D-82B8-5D3863164125        iOS     40efb734191443f79a6be67635e9bb12                sdk2.1.com      0BCA315A-D3D9-404D-82B8-5D3863164125   360D880D-0648-45C4-8890-F760D47EA719     7.0     10.0.2  (null)  (null)  44234234234     43243243                        1484564131399   3454235435432   20170116       18
-- ############################################################################################################################################################################## --
-- ############################################################################################################################################################################## --
set mapreduce.job.name=stg.fact_kesheng_sdk_new_device_hourly_01_${SRC_FILE_DAY}_${SRC_FILE_HOUR};
set hive.merge.mapredfiles=true;

with deviceinfo as
(
SELECT
    case when t1.imei is null or t1.imei = '' or t1.imei = 'null' then '-998' else t1.imei end as imei,
    case when t1.user_id is null or t1.user_id = '' or t1.user_id = 'null' then '-998' else t1.user_id end as user_id,
    case when t1.idfa is null or t1.idfa = '' or t1.idfa = 'null' then '-998' else t1.idfa end as idfa,
    case when t1.imsi is null or t1.imsi = '' or t1.imsi = 'null' then '-998' else t1.imsi end as imsi,
	case when t1.phone_number is null or t1.phone_number = '' or t1.phone_number = 'null' then '-998' else t1.phone_number end as phone_number,    
    t1.app_ver_code,
    t1.os,
    nvl(t2.product_key, -998) AS product_key,
    t1.first_launch_channel_id AS app_channel_id,
    t1.start_unix_time,
    t1.src_file_day,
    t1.src_file_hour
 FROM intdata.kesheng_sdk_session_start t1 
 LEFT JOIN mscdata.dim_kesheng_sdk_app_pkg t2 
   ON (t1.os = t2.app_os_type AND t1.app_pkg_name = t2.app_pkg_name)
WHERE t1.src_file_day = '${SRC_FILE_DAY}'
  and t1.src_file_hour = '${SRC_FILE_HOUR}'
)

INSERT OVERWRITE TABLE stg.fact_kesheng_sdk_new_device_hourly_01
SELECT device_key, 
       app_channel_id, 
       product_key,
       app_ver_code, 
       min(start_unix_time) AS start_unix_time, 
       src_file_day, 
       src_file_hour
FROM (
	select t2.device_key, t1.* 
     from deviceinfo t1 
     inner join (
				select * from intdata.kesheng_sdk_active_device_hourly
				where src_file_day = '${SRC_FILE_DAY}' and src_file_hour = '${SRC_FILE_HOUR}'
	            ) t2 
     on (t1.imei = t2.imei and t1.os = t2.app_os_type and t1.user_id = t2.user_id and t1.imsi = t2.imsi and t1.idfa = t2.idfa and t1.phone_number = t2.phone_number)
) t
GROUP BY device_key, app_channel_id, product_key, app_ver_code, src_file_day, src_file_hour;


with deviceinfo as
(
SELECT
case when t1.imei is null or t1.imei = '' or t1.imei = 'null' then '-998' else t1.imei end as imei,
case when t1.user_id is null or t1.user_id = '' or t1.user_id = 'null' then '-998' else t1.user_id end as user_id,
case when t1.idfa is null or t1.idfa = '' or t1.idfa = 'null' then '-998' else t1.idfa end as idfa,
case when t1.imsi is null or t1.imsi = '' or t1.imsi = 'null' then '-998' else t1.imsi end as imsi,    
t1.app_ver_code,
t2.app_os_type,	-- 会出现NULL值(只有当on的两个条件都符合的时候才不会出现null) 注：当t1.os为"com.miguvideo.datauploadsdk_1",t2.app_os_type没有匹配的;(IOS)t1.device_id='44234234234_0BCA315A-D3D9-404D-82B8-5D3863164125't2表中也是没有匹配的
nvl(t2.product_key, -998) AS product_key, 
t1.first_launch_channel_id AS app_channel_id,		
t1.start_unix_time,		-- 首次显示时间点时间,1970到目前为止的时间戳 bigint
t1.src_file_day,
t1.src_file_hour
FROM intdata.kesheng_sdk_session_start t1 
LEFT JOIN mscdata.dim_kesheng_sdk_app_pkg t2 
  ON (t1.os = t2.app_os_type AND t1.app_pkg_name = t2.app_pkg_name)  -- 关联关系：操作系统&app包名
WHERE t1.src_file_day = '${SRC_FILE_DAY}'
and t1.src_file_hour = '${SRC_FILE_HOUR}'
)
INSERT OVERWRITE TABLE stg.fact_kesheng_sdk_new_device_hourly_01
SELECT device_key, 
app_channel_id, 
product_key,
app_ver_code, 
min(start_unix_time) AS start_unix_time, 	-- start_unix_time 首次显示时间点时间,1970到目前为止的时间戳 bigint
-- upload_unix_time 上报时的时间戳（如果同一个粒度内有多条数据，以最早的时间戳为准）== min(start_unix_time)最小的显示时间
src_file_day, 
src_file_hour
FROM 
(select t2.device_key, t1.* from deviceinfo t1 join intdata.kesheng_sdk_imei_user_device_key t2 on (t1.imei = t2.imei and t1.user_id = t2.user_id) where t1.app_os_type = 'AD' and t1.imei <> '-998' and t1.user_id <> '-998'
 union all
 select t2.device_key, t1.* from deviceinfo t1 join intdata.kesheng_sdk_imei_imsi_device_key t2 on (t1.imei  = t2.imei and t1.imsi = t2.imsi) where t1.app_os_type = 'AD' and t1.imei <> '-998' and t1.user_id = '-998' and t1.imsi <> '-998'
 union all
 select t2.device_key, t1.* from deviceinfo t1 join intdata.kesheng_sdk_idfa_user_device_key t2 on (t1.idfa = t2.idfa and t1.user_id = t2.user_id) where t1.app_os_type = 'iOS'
) t
GROUP BY device_key, app_channel_id, product_key, app_ver_code, src_file_day, src_file_hour;
-- 该表的作用仅仅是为了拿到新增设备的device_key吗？ 是的！！！
-- 该处的join 代表的是inner join，所以这里全部都是当前周期的新增设备

-- 理论上这个是有可能存在device_key重复的记录
-- 测试：
select count(device_key), count(distinct device_key) from stg.fact_kesheng_sdk_new_device_hourly_01 where src_file_day = '20170116' and src_file_hour = '12';
		 
-- ========================================================================================================================================================== --	 

INSERT OVERWRITE TABLE stg.fact_kesheng_sdk_new_device_hourly_02 PARTITION(grain_ind)	-- 所有产品的新增用户
select
  device_key,
  app_channel_id,
  product_key,
  app_ver_code,
  upload_unix_time,
  row_number() over(partition by device_key order by upload_unix_time) row_num,--根据device_key分组，然后组内排序，取组内序号;
  -- 该函数不考虑是否并列，那怕根据条件查询出来的数值相同也会进行连续排名
  src_file_day,
  src_file_hour,
  '000' grain_ind		
  -- 粒度标识,000 - 将所渠道、产品、版本汇总成一条记录,010 - 产品,011 - 产品+版本,100 - 渠道,110 - 渠道+产品,111 - 渠道+产品+版本
from stg.fact_kesheng_sdk_new_device_hourly_01; 
--------------------------------------上下对比--------------------------------------------------------
INSERT OVERWRITE TABLE stg.fact_kesheng_sdk_new_device_hourly_02 PARTITION(grain_ind)
select
  device_key,
  app_channel_id,
  product_key,
  app_ver_code,
  upload_unix_time,
  row_number() over(partition by device_key, product_key order by upload_unix_time) row_num,
  src_file_day,
  src_file_hour,
  '010' grain_ind	-- 按product_key分组
from stg.fact_kesheng_sdk_new_device_hourly_01;
--------------------------------------上下对比--------------------------------------------------------
INSERT OVERWRITE TABLE stg.fact_kesheng_sdk_new_device_hourly_02 PARTITION(grain_ind)
select
  device_key,
  app_channel_id,
  product_key,
  app_ver_code,
  upload_unix_time,
  row_number() over(partition by device_key, product_key, app_ver_code order by upload_unix_time) row_num,
  src_file_day,
  src_file_hour,
  '011' grain_ind	-- 011 - 产品+版本
from stg.fact_kesheng_sdk_new_device_hourly_01;
--------------------------------------上下对比--------------------------------------------------------
INSERT OVERWRITE TABLE stg.fact_kesheng_sdk_new_device_hourly_02 PARTITION(grain_ind)
select
  device_key,
  app_channel_id,
  product_key,
  app_ver_code,
  upload_unix_time,
  row_number() over(partition by device_key order by upload_unix_time) row_num, --一个device_key只能属于最先推广的渠道
  src_file_day,
  src_file_hour,
  '100' grain_ind		-- 100 - 渠道
from stg.fact_kesheng_sdk_new_device_hourly_01;
--------------------------------------上下对比--------------------------------------------------------
INSERT OVERWRITE TABLE stg.fact_kesheng_sdk_new_device_hourly_02 PARTITION(grain_ind)
select
  device_key,
  app_channel_id,
  product_key,
  app_ver_code,
  upload_unix_time,
  row_number() over(partition by device_key, app_channel_id, product_key order by upload_unix_time) row_num,
  src_file_day,
  src_file_hour,
  '110' grain_ind	--110 - 渠道+产品
from stg.fact_kesheng_sdk_new_device_hourly_01;
--------------------------------------上下对比--------------------------------------------------------
INSERT OVERWRITE TABLE stg.fact_kesheng_sdk_new_device_hourly_02 PARTITION(grain_ind)
select
  device_key,
  app_channel_id,
  product_key,
  app_ver_code,
  upload_unix_time,
  row_number() over(partition by device_key, app_channel_id, product_key, app_ver_code order by upload_unix_time) row_num,
  src_file_day,
  src_file_hour,
  '111' grain_ind
from stg.fact_kesheng_sdk_new_device_hourly_01;

-- ======================================================================================================================================== --
		 
INSERT OVERWRITE TABLE rptdata.fact_kesheng_sdk_new_device_hourly PARTITION(src_file_day, src_file_hour, grain_ind)
select
  t1.device_key,
  t1.app_channel_id,
  t1.product_key,
  t1.app_ver_code,
  t1.upload_unix_time,  -- upload_unix_time 上报时的时间戳（如果同一个粒度内有多条数据，以最早的时间戳为准）== min(start_unix_time)最小的显示时间
  case when t2.device_key is null and row_num = 1 then 1 else 0 end new_cnt,	-- 按客户定义的新用户计数的算法！ t1.row_num
  t2.become_new_unix_time,	-- 会出现空值，数据不准,所以增加下面的字段 become_new_dw_day
  -- 成为新设备的时间戳（如果同一个粒度内有多条新设备，以最近的时间戳为准）如果用户为当前时间的新增用户，那么become_new_unix_time是null
  case when t2.device_key is null 
	   then concat_ws('-', substr('${SRC_FILE_DAY}', 1, 4), substr('${SRC_FILE_DAY}', 5, 2), substr('${SRC_FILE_DAY}', 7, 2))
	   else t2.become_new_dw_day end,		-- become_new_dw_day的算法  20170117 09
  t1.src_file_day,
  t1.src_file_hour,
  t1.grain_ind
from stg.fact_kesheng_sdk_new_device_hourly_02 t1 
     left join (select device_key, 
                       max(case when new_cnt = 1 then upload_unix_time else null end) become_new_unix_time,	-- 成为新设备的时间戳算法：如果为非当前周期新用户，取upload_unix_time，否则赋予null
                       max(become_new_dw_day) become_new_dw_day 
                from rptdata.fact_kesheng_sdk_new_device_hourly  --当前周期之前的6个月数据
                where grain_ind = '000'
                      and (src_file_day >= translate(add_months(concat_ws('-', substr('${SRC_FILE_DAY}', 1, 4), substr('${SRC_FILE_DAY}', 5, 2), '01'), -6), '-', '')  
							-- translate:去掉'-'
                             and src_file_day < '${SRC_FILE_DAY}'		-- 近6个月的去重用户 + 统计日前一天至月初的去重用户
                            or src_file_day = '${SRC_FILE_DAY}' and src_file_hour < '${SRC_FILE_HOUR}'
                          )                                
                group by device_key) t2
     on (t1.device_key = t2.device_key)
where t1.grain_ind = '000';
---------------------------------------------------------上下对比--------------------------------------------------------
INSERT OVERWRITE TABLE rptdata.fact_kesheng_sdk_new_device_hourly PARTITION(src_file_day, src_file_hour, grain_ind) --动态分区
select 							-- 这个表中存的是2017011109(小)时的数据，包括该时间周期内的新增设备(new_cnt=1) 和 相对于前6个月的新增设备而言的老设备(new_cnt=0)
  t1.device_key,
  t1.app_channel_id,
  t1.product_key,
  t1.app_ver_code,
  t1.upload_unix_time,  
  case when t2.device_key is null and row_num = 1 then 1 else 0 end new_cnt,
  t2.become_new_unix_time,
  case when t2.device_key is null 
	   then concat_ws('-', substr('${SRC_FILE_DAY}', 1, 4), substr('${SRC_FILE_DAY}', 5, 2), substr('${SRC_FILE_DAY}', 7, 2))
	   else t2.become_new_dw_day end,
  t1.src_file_day,
  t1.src_file_hour,
  t1.grain_ind
from stg.fact_kesheng_sdk_new_device_hourly_02 t1 
     left join (select product_key,
                       device_key, 
                       max(case when new_cnt = 1 then upload_unix_time else null end) become_new_unix_time,
                       max(become_new_dw_day) become_new_dw_day 
                from rptdata.fact_kesheng_sdk_new_device_hourly 
                where grain_ind = '010'
                      and (src_file_day >= translate(add_months(concat_ws('-', substr('${SRC_FILE_DAY}', 1, 4), substr('${SRC_FILE_DAY}', 5, 2), '01'), -6), '-', '')
                             and src_file_day < '${SRC_FILE_DAY}'
                            or src_file_day = '${SRC_FILE_DAY}' and src_file_hour < '${SRC_FILE_HOUR}'
                          )      
                group by product_key,device_key) t2
     on (t1.device_key = t2.device_key and t1.product_key = t2.product_key) -- 当不符合该条件时，t2表中的字段为null，即在该维度下 为新用户。
where t1.grain_ind = '010';		


...(其他粒度的)



-- 测试
select app_channel_id,product_key,app_ver_code,grain_ind from rptdata.fact_kesheng_sdk_new_device_hourly  limit 100;
select * from rptdata.fact_kesheng_sdk_new_device_hourly where new_cnt <> 1 limit 100;
select * from rptdata.fact_kesheng_sdk_new_device_hourly where device_key=3;
select * from rptdata.fact_kesheng_sdk_new_device_hourly where src_file_day='20170109' and src_file_hour='01'and grain_ind='000';
OK  
3598    200300280000002 10      3.2.0   1483896238147   0       1483892491719   2017-01-09      20170109        01      000
3600    200300280000002 10      3.2.0   1483895871216   0       1483893431248   2017-01-09      20170109        01      000
3626    201800000000006 10      3.2.0   1483896208812   0       1483748932973   2017-01-09      20170109        01      000
3635    200300120100028 10      3.2.0   1483896585976   0       1483892059799   2017-01-09      20170109        01      000
3638    201800000000006 10      3.2.0   1483894858937   0       1483893367043   2017-01-09      20170109        01      000		-- 已出现过的设备
3661    201800000000002 10      3.2.0   1483896103173   1       NULL    2017-01-09      20170109        01      000		-- 当前周期新设备
3662    200300380000001 10      3.2.0   1483896734006   1       NULL    2017-01-09      20170109        01      000
3663    201800000000002 10      3.2.0   1483896165294   1       NULL    2017-01-09      20170109        01      000
3664    201800000010104 10      3.2.0   1483895087769   1       NULL    2017-01-09      20170109        01      000
3665    201800000000002 10      3.2.0   1483880999604   1       NULL    2017-01-09      20170109        01      000

select * from stg.fact_kesheng_sdk_new_device_hourly_02 where src_file_day='20170124' and src_file_hour='11'and grain_ind='011';
OK		-- product_key,app_ver_code
58      200300290000002 10      3.2.1   1485227153827   1       20170124        11      011
154     200300240100003 10      3.2.1   1485227135399   1       20170124        11      011
484     200300280000002 10      3.2.1   1485227218697   1       20170124        11      011
660     200300280000002 10      3.2.1   1485226827987   1       20170124        11      011
3844    201800000000002 10      3.2.0   1485227375149   1       20170124        11      011
5081    201800000020057 10      3.2.0   1485226969019   1       20170124        11      011
5244    201800000000006 10      3.2.1   1485227303133   1       20170124        11      011
7705    201800000000006 10      3.2.1   1485226816455   1       20170124        11      011
9490    200300220100004 10      3.2.1   1485226889158   1       20170124        11      011
11696   201800000000002 10      3.2.1   1485226856775   1       20170124        11      011
11718   201800000000002 10      3.2.1   1485226966832   1       20170124        11      011

select * from stg.fact_kesheng_sdk_new_device_hourly_02 where src_file_day='20170124' and src_file_hour='11'and grain_ind='111';
OK		-- app_channel_id,product_key,app_ver_code
58      200300290000002 10      3.2.1   1485227153827   1       20170124        11      111
154     200300240100003 10      3.2.1   1485227135399   1       20170124        11      111
484     200300280000002 10      3.2.1   1485227218697   1       20170124        11      111
660     200300280000002 10      3.2.1   1485226827987   1       20170124        11      111
3844    201800000000002 10      3.2.0   1485227375149   1       20170124        11      111
5081    201800000020057 10      3.2.0   1485226969019   1       20170124        11      111
5244    201800000000006 10      3.2.1   1485227303133   1       20170124        11      111
7705    201800000000006 10      3.2.1   1485226816455   1       20170124        11      111
9490    200300220100004 10      3.2.1   1485226889158   1       20170124        11      111