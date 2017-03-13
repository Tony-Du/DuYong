
-- == 用户手机型号分布统计 =======

-- == app.cpa_phone_mode_daily =================================================

set mapreduce.job.name=app.cpa_phone_mode_daily_${SRC_FILE_DAY};
set hive.merge.mapredfiles=true;

insert overwrite table app.cpa_phone_mode_daily partition(src_file_day='${SRC_FILE_DAY}')
select '${SRC_FILE_DAY}' stat_day    
      ,(case when t1.product_key = -1 then '-1' else nvl(d1.product_name,'')  end) product_name 
      ,t1.product_key
      ,(case when t1.app_channel_id = '-1' then '-1' else nvl(d2.chn_name,'') end) chn_name
      ,t1.app_channel_id
      ,t1.phone_mode_name
      ,t1.accu7day_add_device_num     -- 近7日累计新增设备数
      ,t1.accu30day_add_device_num    -- 近30天累计新增设备数        
      ,if(t1.accu7day_all_add_device_num = 0, 0, 
            round(t1.accu7day_add_device_num*100/t1.accu7day_all_add_device_num,2)) accu7day_phone_mode2all_rate        --近7日累计新增占比,这里关键需要求t1.accu7day_all_add_device_num
      ,if(t1.accu30day_all_add_device_num = 0, 0, 
            round(t1.accu30day_add_device_num*100/t1.accu30day_all_add_device_num,2)) accu30day_phone_mode2all_rate		--近30日累计新增占比,这里关键需要求t1.accu30day_all_add_device_num
  from rptdata.fact_kesheng_sdk_phone_mode_daily t1
  left join mscdata.dim_kesheng_sdk_product d1
    on t1.product_key = d1.product_key
  left join rptdata.dim_chn d2
    on t1.app_channel_id = d2.chn_id
 where t1.src_file_day = '${SRC_FILE_DAY}'
   and (d1.product_key is not null or t1.product_key = -1)
   and (d2.chn_id is not null or t1.app_channel_id = '-1');

-- == rptdata.fact_kesheng_sdk_phone_mode_daily =================================================

set mapreduce.job.name=stg.fact_kesheng_sdk_phone_mode_daily_01_${SRC_FILE_DAY};
set hive.merge.mapredfiles=true;

insert overwrite table stg.fact_kesheng_sdk_phone_mode_daily_01
select t6.app_channel_id
      ,t6.product_key
      ,if(substr(t6.grain_ind,3,1)='0', '-1', t6.phone_mode_name) phone_mode_name
      ,t6.add_device_num
  from (select t5.app_channel_id, t5.product_key, t5.phone_mode_name
              ,sum(t5.new_cnt) add_device_num	-- 当前周期（今天）新增用户数
              ,rpad(reverse(bin(cast(grouping__id as int))),3,'0') grain_ind
          from (select if(substr(t3.grain_ind,1,1)='0', '-1', t3.app_channel_id) app_channel_id
                      ,if(substr(t3.grain_ind,2,1)='0', -1, t3.product_key) product_key
                      ,nvl(t2.phone_mode_name,'-998') phone_mode_name
                      ,t3.new_cnt
                  from (select a.device_key, a.app_channel_id, a.product_key
                              ,a.new_cnt ,a.grain_ind
                          from rptdata.fact_kesheng_sdk_new_device_hourly a		-- 该表按这3个维度的组合统计：app_channel_id, product_key, app_ver_code
                         where a.src_file_day = '${SRC_FILE_DAY}'
                           and substr(a.grain_ind,3,1) = '0'	-- 不考虑 app_ver_code
                           and a.new_cnt > 0	-- 新设备
                       ) t3
                  left join (select b.device_key, 
                                    if(nvl(b.phone_brand_name,'') = '',b.phone_mode_name,	--如果手机品牌为空字符串，取值:手机型号，否则再判断手机型号是否包含手机品牌，包含，取值:手机型号，不包含，取值:手机品牌和型号相连接
                                          if(instr(upper(b.phone_mode_name), upper(b.phone_brand_name)) > 0, 
                                                   b.phone_mode_name, concat_ws(' ', b.phone_brand_name, b.phone_mode_name))) phone_mode_name
                               from intdata.kesheng_sdk_device_key2phone_mode b		-- 3个关键字段：device_key，phone_brand_name，phone_mode_name
                              where b.src_file_day = '${SRC_FILE_DAY}'
                            ) t2
                    on t3.device_key = t2.device_key
               ) t5
         group by app_channel_id, product_key, phone_mode_name	--按照这三个维度来进行统计（把原来的 app_ver_code 换为 phone_mode_name ）
         grouping sets((app_channel_id, product_key)
                     ,(app_channel_id, product_key, phone_mode_name))
       ) t6;

-----------------------------------------------------------------------------
set mapreduce.job.name=rptdata.fact_kesheng_sdk_phone_mode_daily_${SRC_FILE_DAY};

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.merge.mapredfiles=true;


with stg_phone_mode_daily
  as (select t3.app_channel_id, t3.product_key, t3.phone_mode_name
            ,sum(t3.add_device_num) add_device_num
            ,sum(t3.accu7day_add_device_num) accu7day_add_device_num
            ,sum(t3.accu30day_add_device_num) accu30day_add_device_num
        from (select t1.app_channel_id, t1.product_key, t1.phone_mode_name
                    ,t1.add_device_num							
                    ,t1.add_device_num accu7day_add_device_num	 
                    ,t1.add_device_num accu30day_add_device_num  
                from stg.fact_kesheng_sdk_phone_mode_daily_01 t1	-- union all前面表示的是当前周期（今天）的新增设备数
               union all										
              select t2.app_channel_id, t2.product_key, t2.phone_mode_name	-- union all后面表示的是当前周期（今天）之前的的累计新增设备数
                    ,0 add_device_num
                    ,if(t2.src_file_day > from_unixtime(unix_timestamp('${SRC_FILE_DAY}','yyyyMMdd')-60*60*24*7,'yyyyMMdd')
                         ,t2.add_device_num, 0) accu7day_add_device_num				--如果是近一周的数据，取t2.add_device_num,否则取0
                    ,t2.add_device_num accu30day_add_device_num  
                from rptdata.fact_kesheng_sdk_phone_mode_daily t2
               where t2.src_file_day < '${SRC_FILE_DAY}'	-- 近30天内的数据（不包括当前周期，今天）
                 and t2.src_file_day > from_unixtime(unix_timestamp('${SRC_FILE_DAY}','yyyyMMdd')-60*60*24*30,'yyyyMMdd')
             ) t3
       group by t3.app_channel_id, t3.product_key, t3.phone_mode_name
     )
insert overwrite table rptdata.fact_kesheng_sdk_phone_mode_daily partition(src_file_day='${SRC_FILE_DAY}')
select t5.app_channel_id, t5.product_key, t5.phone_mode_name
      ,t5.add_device_num
      ,t5.accu7day_add_device_num
      ,t5.accu30day_add_device_num
      ,t6.accu7day_add_device_num accu7day_all_add_device_num
      ,t6.accu30day_add_device_num accu30day_all_add_device_num
  from stg_phone_mode_daily t5						-- 这里的cross join 做什么作用
      ,(select a.app_channel_id, a.product_key
              ,a.add_device_num
              ,a.accu7day_add_device_num
              ,a.accu30day_add_device_num
          from stg_phone_mode_daily a
         where a.phone_mode_name = '-1'		-- ？？？  不考虑phone_mode_name这个维度
       ) t6
 where t5.app_channel_id = t6.app_channel_id
   and t5.product_key = t6.product_key;

-- == intdata.kesheng_sdk_device_key2phone_mode =================================================

set mapreduce.job.name=stg.kesheng_sdk_device2phone_mode_01_${SRC_FILE_DAY};
set hive.merge.mapredfiles=true;

insert overwrite table stg.kesheng_sdk_device_key2phone_mode_01
select t1.app_os_type, t1.imei, t1.idfa, t1.user_id, t1.imsi, t1.phone_number
      ,t1.phone_mode_name, t1.phone_brand_name, t1.upload_unix_time
  from (select a1.app_os_type, a1.imei, a1.idfa, a1.user_id, a1.imsi, a1.phone_number
              ,a1.phone_mode_name, a1.phone_brand_name, a1.upload_unix_time
              ,row_number()over(partition by a1.app_os_type, a1.imei, a1.idfa, a1.user_id, a1.imsi, a1.phone_number
                                    order by a1.upload_unix_time desc) rn
          from (select a.os_type_code app_os_type 
                      ,if(nvl(a.imei,'')='','-998',a.imei) imei
                      ,if(nvl(a.idfa,'')='','-998',a.idfa) idfa
                      ,if(nvl(a.user_id,'')='','-998',a.user_id) user_id
                      ,if(nvl(a.imsi,'')='','-998',a.imsi) imsi
                      ,if(nvl(a.phone_number,'')='','-998',a.phone_number) phone_number
                      ,a.phone_mode_name
					  ,a.phone_brand_name
					  ,a.upload_unix_time
                  from intdata.kesheng_sdk_device a				-- 这里为什么要取最近三天的设备信息？？？
                 where a.src_file_day <= '${SRC_FILE_DAY}'		-- 包括今天的前三天，比如：今天20170217, 20170215 <= src_file_day <= 20170217
                   and a.src_file_day >= from_unixtime(unix_timestamp('${SRC_FILE_DAY}','yyyyMMdd')-60*60*24*2,'yyyyMMdd')	
                   and nvl(a.phone_mode_name,'null') <> 'null' and a.phone_mode_name <> ''
               ) a1
       ) t1
 where rn = 1;		-- 为什么又要分组取第一个值？？？

-------------------------------------------------------------------------------------
set mapreduce.job.name=intdata.kesheng_sdk_device2phone_mode_${SRC_FILE_DAY};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.merge.mapredfiles=true;

insert overwrite table intdata.kesheng_sdk_device_key2phone_mode partition(src_file_day='${SRC_FILE_DAY}')
select t3.device_key
      ,t3.phone_mode_name
      ,t3.phone_brand_name
      ,t3.upload_unix_time
  from (select t2.device_key
			  ,t1.phone_mode_name
              ,t1.phone_brand_name
			  ,t1.upload_unix_time
              ,row_number()over(partition by t2.device_key
                                    order by t1.upload_unix_time desc) rn
          from stg.kesheng_sdk_device_key2phone_mode_01 t1					-- from a,b 表示a,b两表进行cross join(笛卡尔积)
              ,(select b.device_key, b.app_os_type, b.imei, b.user_id
                      ,b.imsi, b.idfa, b.phone_number
                  from intdata.kesheng_sdk_active_device_hourly b		-- 该表是device_key的产生表
                 where b.src_file_day = '${SRC_FILE_DAY}'
               ) t2
         where t1.app_os_type = t2.app_os_type and t1.imei = t2.imei
           and t1.idfa = t2.idfa and t1.user_id = t2.user_id 
           and t1.imsi = t2.imsi and t1.phone_number = t2.phone_number
       ) t3
 where t3.rn = 1;	-- 为什么又要分组取第一个值？？？取相同device_key的记录的其中一个


-- ==  intdata.kesheng_sdk_device ================================================
add jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;
set mapreduce.job.name=kesheng_sdk_device_${EXTRACT_DATE};
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

INSERT OVERWRITE TABLE intdata.kesheng_sdk_device PARTITION(src_file_day)
SELECT
    concat_ws('_', nvl(install_id, ''),  nvl(COALESCE(imei, idfa), '')) device_id,
    imei,
    idfa,
    idfv,
    imsi,
    sim,
    udid,
    os_type,
    macId,
    app_ver_code,
    phone_mode_name,
    phone_brand_name,
    phone_number,
    sreen_width,
    sreen_height,
    sreen_density,
    app_key,
    app_pkg_name,
    app_channel_id,
    user_id,
    language_code,
    upload_unix_time,
    os_ver_code,
    sdk_ver_code,
    is_BTB_flag,
    back_cnt,
    install_id,
    src_file_day
FROM (SELECT 
         v.imei,
         v.idfa,
         v.idfv,
         v.imsi,
         v.sim,
         v.udid,
         v.os as os_type,
         v.macId,
         v.appVersion as app_ver_code,
         v.phoneMode as phone_mode_name,
         v.phoneBrand as phone_brand_name,
         v.phoneNumber as phone_number,
         v.sreenWidth as sreen_width,
         v.sreenHeight as sreen_height,
         v.sreenDensity as sreen_density,
         v.appkey as app_key,
         v.apppkg as app_pkg_name,
         v.appchannel as app_channel_id,
         v.userid as user_id,
         v.language as language_code,
         cast(v.uploadTs as bigint) as upload_unix_time,
         v.osversion as os_ver_code,
         v.sdkversion as sdk_ver_code,
         v.isBTB as is_BTB_flag,
         cast(v.backCount as bigint) as back_cnt,
         v.installationID as install_id,
         v.extract_date_label as src_file_day,
         row_number() over(partition by v.installationID, v.imei, v.idfa order by v.uploadTs desc) row_num
    FROM ods.kesheng_sdk_deviceinfo_v v
    WHERE v.extract_date_label = '${EXTRACT_DATE}' and
         (
          (v.os = 'AD' and (v.installationID is not null or v.imei is not null)) 
          or (v.os = 'iOS' and (v.installationID is not null or v.idfa is not null))
         )
    ) x
WHERE row_num = 1;	-- 取uploadts最大（最近访问）的那一条记录 为什么这么做？？

--测试
select v.installationID, 
v.os,
v.imei, 
v.idfa,
v.uploadTs,
row_number() over(partition by v.installationID, v.imei, v.idfa order by v.uploadTs desc) row_num 
FROM ods.kesheng_sdk_deviceinfo_v v
WHERE v.extract_date_label = 20170304
and
(
 (v.os = 'AD' and (v.installationID is not null or v.imei is not null)) 
 or (v.os = 'iOS' and (v.installationID is not null or v.idfa is not null))
);
NULL    iOS     3fc94b21e6964ceab1a798dca2731c4d        E6DA1A2F-3124-4C68-8B3F-A9F5F60344A8    1488607269211   1
NULL    iOS     3fc94b21e6964ceab1a798dca2731c4d        E6DA1A2F-3124-4C68-8B3F-A9F5F60344A8    1488606889305   2
NULL    iOS     3fc94b21e6964ceab1a798dca2731c4d        E6DA1A2F-3124-4C68-8B3F-A9F5F60344A8    1488592401956   3
NULL    iOS     417e80a1f3844898b6742ceaa5350041        00000000-0000-0000-0000-000000000000    1488575616260   1
NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966   1
NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966   2
NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966   3
NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966   4
NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966   5
NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966   6
NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966   7
NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966   8
NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966   9
NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966   10
NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966   11
NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966   12
NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966   13
NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966   14
NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966   15
NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966   16
NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966   17
NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966   18
NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966   19
NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966   20
NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966   21
NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966   22
NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966   23

select v.install_id,
v.os_type_code,
v.imei, 
v.idfa,
v.upload_unix_time 
from intdata.kesheng_sdk_device v
where v.src_file_day = '20170304'
 and v.imei = '41a70de57e494d12b5c7f6cdfa35b219' 
and v.idfa = '1CD00D70-48EF-46B2-AC4D-6B5C5D477B47';

NULL    iOS     41a70de57e494d12b5c7f6cdfa35b219        1CD00D70-48EF-46B2-AC4D-6B5C5D477B47    1488633474966

-- == ods.kesheng_sdk_deviceinfo_v ================================================

CREATE VIEW `ods.kesheng_sdk_deviceinfo_v` 
AS SELECT `s`.`rowkey`,
       `d`.`os`,
       `d`.`imei`,
       `d`.`imsi`,
       `d`.`idfa`,
       `d`.`idfv`,
       `d`.`sim`,
       `d`.`macid`,
       `d`.`appversion`,
       `d`.`phonemode`,
       `d`.`phonebrand`,
       `d`.`phonenumber`,
       `d`.`sreenwidth`,
       `d`.`sreenheight`,
       `d`.`sreendensity`,
       `d`.`appkey`,
       `d`.`apppkg`,
       `d`.`appchannel`,
       `d`.`userid`,
       `d`.`language`,
       `d`.`isbtb`,
       `d`.`backcount`,
       `d`.`uploadts`,
       `d`.`clientid`,
       `d`.`sessionid`,
       `d`.`osversion`,
       `d`.`sdkversion`,
       `d`.`ints`,
       `d`.`networktype`,
       `d`.`udid`,
       `d`.`installationid`,
       `s`.`client_ip`,
       `s`.`src_file_day` `extract_date_label`,
       `s`.`src_file_hour` `extract_hour_label`
FROM `ods`.`kesheng_sdk_json_v` `s`
LATERAL VIEW json_tuple(`s`.`json`, 'deviceInfo') `p` as `deviceInfo`	-- deviceInfo
LATERAL VIEW json_tuple(`p`.`deviceinfo`, 'os', 'imei', 'imsi', 'idfa', 'idfv', 'sim', 'macId', 'appVersion', 'phoneMode', 'phoneBrand', 'phoneNumber', 'sreenWidth', 'sreenHeight', 'sreenDensity', 'appkey', 'apppkg', 'appchannel', 'userid', 'language', 'isBTB', 'backCount', 'uploadTs', 'clientId', 'sessionId', 'osversion', 'sdkversion', 'ints', 'networktype', 'udid', 'installationID') `d` AS `os`, `imei`, `imsi`, `idfa`, `idfv`, `sim`, `macId`, `appVersion`, `phoneMode`, `phoneBrand`, `phoneNumber`, `sreenWidth`, `sreenHeight`, `sreenDensity`, `appkey`, `apppkg`, `appchannel`, `userid`, `language`, `isBTB`, `backCount`, `uploadTs`, `clientId`, `sessionId`, `osversion`, `sdkversion`, `ints`, `networktype`, `udid`, `installationID

-- select s.src_file_day, s.src_file_hour from ods.kesheng_sdk_json_v s ;

-- == ods.kesheng_sdk_json_v ================================================

CREATE VIEW `ods.kesheng_sdk_json_v` AS select translate(`k2`.`line`[0], '\u0000', '') `json`
	  ,`k2`.`line`[1] `client_ip`
      ,`k2`.`src_file_day`
      ,`k2`.`src_file_hour`
      ,`k2`.`input_file_name` 
	  ,`k2`.`block_offset_inside_file`
      ,concat(`k2`.`input_file_name`,':',`k2`.`block_offset_inside_file`) `rowkey`
  from (select split(`k1`.`line`,'\\|\\|') `line`
              ,`k1`.`src_file_day` 
			  ,`k1`.`src_file_hour`
              ,`k1`.`input__file__name` `input_file_name`  
			  ,`k1`.`block__offset__inside__file` `block_offset_inside_file`
          from `ods`.`kesheng_sdk_raw_ex` `k1`
       ) `k2`

-- == ods.kesheng_sdk_raw_ex ================================================

CREATE EXTERNAL TABLE `ods.kesheng_sdk_raw_ex`(
  `line` string)
PARTITIONED BY ( 
  `src_file_day` string, 
  `src_file_hour` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
LOCATION
  'hdfs://ns1/user/hadoop/ods/kesheng'
TBLPROPERTIES (
  'transient_lastDdlTime'='1485154783')


{"deviceInfo":{}
,"timeInfo":[]
,"eventInfo":[]
,"customInfo":[{"ContentID":"220106297720170220002","networkType":"WIFI","loadTime":"11","timestamp":"2017-02-20 01:59:23:345","account":"743233634","channelID":"null","type":"11","result":"0","clientID":"37D8D171EF6337E0EC0F3B9B7B10C744"},{"ContentID":"220106297720170220002","networkType":"WIFI","programeUrl":"http:\/\/live.gslb.cmvideo.cn\/wd_r2\/cctv\/cctv5hd\/1200\/index.m3u8?msisdn=15053731087&sid=2201062977&timestamp=20170220015923&Channel_ID=0117_42000202-00002-200300270000011&pid=2028597139&spid=699019&imei=f9b9fa15795510fe13befbbb641f9584&netType=4&ProgramID=220106297720170220002&client_ip=&encrypt=bdcf57cb37e31b33fe7c1b32a836bea8","timestamp":"2017-02-20 01:59:23:712","state":"Open","stateNumID":"1","programeType":"0","channelID":"null","rateType":"2","playSessionID":"621424572", "programeType" :"1", "type" :"17", "timestamp" :"2017-02-20 01:59:39:918", "state" :"Open", "playSessionID" :"258a4d3dacafbf9333ae45f2b726ce18"},{ "networkType2" :"4G", "result" :"0", "dns" :"", "account" :"15267700263", "mask" :"", "type" :"2", "timestamp" :"2017-02-20 01:59:39", "ip" :"10.115.97.220", "networkType" :"APN"},{ "account" :"15267700263", "result" :"0", "stateNumID" :"2", "programeUrl" :"http:\/\/gslb.miguvod.lovev.com\/depository\/asset\/zhengshi\/5100\/066\/877\/5100066877\/media\/5100066877_5000480782_58.mp4.m3u8?msisdn=15267700263&mdspid=&spid=600058&netType=5&sid=5500086193&pid=2028593060&timestamp=20170220015939&Channel_ID=0111_64040001-99000-700200000000008&ProgramID=621424572&ParentNodeID=-99&client_ip=112.17.244.241&assertID=5500086193&vormsid=2017022001593901755967&imei=f020b126065fda5e97332152d3929c60924c66efeab9e9912f36c303af670751&SecurityKey=20170220015939&encrypt=34da8ad1aaeea6700de964232792940a&jid=258a4d3dacafbf9333ae45f2b726ce18", "rateType" :"75", "ContentID" :"621424572", "programeType" :"1", "type" :"17", "timestamp" :"2017-02-20 01:59:40:034", "state" :"Connecting", "playSessionID" :"258a4d3dacafbf9333ae45f2b726ce18"}]
,"clientInfo":{}}||112.17.244.241 
20170220        
2

{"sdkSessionInfo":{"networkType":"WIFI","udid":"ffffffff-ab6a-cb28-0000-0000000000001484927255664","apppkg":"com.cmvideo.migumovie","appVersion":"4.0.0.2","clientId":"3316b6b0c7e5301250a36f005595aa26","imei":""}
,"eventInfo":[]
,"customInfo":[{"timestamp":"2017-02-20 01:59:31:636","account":"15859550875","result":"0","type":"26","playSessionID":"cfdb77681de61f59b77603e6385099fb","ContentID":"619571101","size":"219661312"}]
,"deviceInfo":{}
,"clientInfo":{}
,"timeInfo":[]
,"exception":[]}||111.147.231.108      
20170220        
2

{"sdkSessionInfo":{"imei":"5383c39275914fe689d5c76b896489eb","udid":"8CDA158E-8FF7-40DA-B2AA-62CE19F8C784","idfa":"C5BCD167-BFEF-4733-962D-2D793B79C0F8","idfv":"33690A00-69E1-4034-9348-AF0E2240C89F","appVersion":"3.0.1","networkType":"WiFi","apppkg":"com.cmvideo.miguvideohd","clientId":"111111"}
,"deviceInfo":{}
,"timeInfo":[{"page":"LauncherViewController","sts":0,"ets":1487527189987}]
,"eventInfo":[]
,"exception":[]
,"customInfo":[]
,"clientInfo":{}}||85.145.218.228    
20170220        
2

{"deviceInfo":{}
,"timeInfo":[]
,"eventInfo":[]
,"customInfo":[{"number":"40","timestamp":"2017-02-20 01:58:54","result":"0","type":"19","loadTime":"5681.0"}]
,"exception":[]
,"sdkSessionInfo":{"udid":"ffffffff-d726-a13b-0000-0000000000001487526294593","appVersion":"3.0.1.2","apppkg":"com.cmcc.cmvideo","clientId":"3460a3d20200abc5d417f5a8ac2e5337","networkType":"4G","imei":""}
,"clientInfo":{}}||112.17.238.228       
20170220        
2

















