

drop table ods.kesheng_event_raw_ex;
create external table ods.kesheng_event_raw_ex (
    line string
)
partitioned by (src_file_day string, src_file_hour string)
location '/user/hadoop/ods/migu/kesheng/kesheng_event';

/*-----
alter table ods.kesheng_event_raw_ex add if not exists partition(src_file_day='20170322', src_file_hour='16') 
  location '20170322/16'
  
sudo su - hadoop -c "hive -e \"alter table ods.kesheng_event_json_ex add if not exists partition(src_file_day='#substr(#flow.startDataTime#,0,8)#', src_file_hour='#substr(#flow.startDataTime#,8,10)#') location '#substr(#flow.startDataTime#,0,8)#/#substr(#flow.startDataTime#,8,10)#';\""
 
--- */
-------------------------------------------------------------------------------------

CREATE VIEW `ods.kesheng_event_json_v` AS select concat(`k2`.`input_file_name`,':',`k2`.`block_offset_inside_file`) `rowkey`
      ,`k2`.`line`[0] `json`, `k2`.`line`[1] `client_ip`
      ,`k2`.`src_file_day`
      ,`k2`.`src_file_hour`      
  from (select split(`k1`.`line`,'\\|\\|') `line`
              ,`k1`.`src_file_day` ,`k1`.`src_file_hour`
              ,`k1`.`input__file__name` `input_file_name`  ,`k1`.`block__offset__inside__file` `block_offset_inside_file`
          from `ods`.`kesheng_event_raw_ex` `k1`
       ) `k2`
	   
	  	   
-------------------------------------------------------------------------------------	   

	   
set mapreduce.job.name=odsdata.kesheng_event_json_${SRC_FILE_DAY}_${SRC_FILE_HOUR};
set hive.merge.mapredfiles=true;

insert overwrite table odsdata.kesheng_event_json partition(src_file_day='${SRC_FILE_DAY}', src_file_hour='${SRC_FILE_HOUR}')
select e1.rowkey
      ,p2.imei
      ,p2.udid
      ,p2.idfa
      ,p2.idfv
      ,p2.installationID
      ,p2.clientId 
      ,p2.appVersion
      ,p2.apppkg
      ,p2.appchannel
      ,p2.os
      ,p2.networkType
      ,p2.account
      ,e1.client_ip
      ,regexp_replace(j1.customEvent_json,'\\[|\\]','') customEvent_json
  from ods.kesheng_event_json_v e1
       lateral view json_tuple(e1.json,'customEvent') j1 as customEvent_json
       lateral view json_tuple(e1.json,'sdkSessionInfo') j2 as sdkSessionInfo_json
       lateral view json_tuple(j2.sdkSessionInfo_json, 'imei', 'udid', 'idfa', 'idfv', 'installationID', 'clientId'
                                            , 'appVersion', 'apppkg', 'appchannel', 'os', 'networkType', 'account') p2
                 as imei, udid, idfa, idfv, installationID, clientId 
                   ,appVersion, apppkg, appchannel, os, networkType, account
 where e1.src_file_day='${SRC_FILE_DAY}' and e1.src_file_hour='${SRC_FILE_HOUR}'
   and nvl(j2.sdkSessionInfo_json,'{}') <> '{}';

-------------------------------------------------------------------------------------------------------------------   
   
set mapreduce.job.name=intdata.kesheng_event_occur_${SRC_FILE_DAY}_${SRC_FILE_HOUR};
set hive.merge.mapredfiles=true;

insert overwrite table intdata.kesheng_event_occur partition(src_file_day='${SRC_FILE_DAY}', src_file_hour='${SRC_FILE_HOUR}')
select t1.rowkey
      ,t1.app_ver_code
      ,t1.app_pkg_name
      ,t1.app_channel_id
      ,t1.app_os_type
      ,t1.event_name
      ,t1.du
      ,t1.timestamp
      ,nvl(d1.product_key, -998) product_key
  from ods.kesheng_event_param_json_v t1
  left join mscdata.dim_kesheng_sdk_app_pkg d1
    on (t1.app_pkg_name = d1.app_pkg_name and t1.app_os_type = d1.app_os_type)
 where t1.src_file_day = '${SRC_FILE_DAY}' 
   and t1.src_file_hour = '${SRC_FILE_HOUR}'
   and nvl(t1.event_name, '') <> '';

   
--------------------------------------------------------------------------------------------------------------

set mapreduce.job.name=intdata.kesheng_event_params_${SRC_FILE_DAY}_${SRC_FILE_HOUR};
set hive.merge.mapredfiles=true;

with stg_kesheng_event_param_json as
(select a.rowkey
       ,a.app_ver_code
       ,a.app_pkg_name
       ,a.app_channel_id
       ,a.app_os_type
       ,a.event_name
       ,nvl(d1.product_key, -998) product_key
       ,a.eventParams_json
  from ods.kesheng_event_param_json_v a
  left join mscdata.dim_kesheng_sdk_app_pkg d1
    on (a.app_pkg_name = d1.app_pkg_name and a.app_os_type = d1.app_os_type)
 where a.src_file_day = '${SRC_FILE_DAY}' 
   and a.src_file_hour = '${SRC_FILE_HOUR}'
   and nvl(a.event_name, '') <> ''
)
insert overwrite table intdata.kesheng_event_params partition(src_file_day='${SRC_FILE_DAY}', src_file_hour='${SRC_FILE_HOUR}')
select t3.rowkey
,t3.app_ver_code
,t3.app_pkg_name
,t3.app_channel_id
,t3.app_os_type
,t3.event_name
,t3.param_pos
,t3.param_key_val[0] param_name
,t3.param_key_val[1] param_val
,t3.product_key
from (select t1.rowkey ,t1.app_ver_code ,t1.app_pkg_name ,t1.app_channel_id
,t1.app_os_type ,t1.event_name ,t1.product_key, p1.param_pos
,split(p1.param_key_val, '\\s*:\\s*') param_key_val
from stg_kesheng_event_param_json t1
lateral view posexplode(split(regexp_replace(regexp_replace(t1.eventParams_json, '\\{|\\}', ''), '"', ''), '\\s*,\\s*')) p1 
as param_pos, param_key_val
) t3;

-- 分割 param_name 和 param_val 出现问题
--------------------------------------------------------------------------------------------------------------


set mapreduce.job.name=rptdata.fact_kesheng_event_occur_hourly_${SRC_FILE_DAY}_${SRC_FILE_HOUR};

set hive.groupby.skewindata=true;
set hive.optimize.skewjoin=true;
set hive.merge.mapredfiles=true;


insert overwrite table rptdata.fact_kesheng_event_occur_hourly 
       partition(src_file_day='${SRC_FILE_DAY}', src_file_hour='${SRC_FILE_HOUR}')
select t1.app_channel_id
      ,t1.product_key
      ,t1.app_ver_code
      ,t1.event_name
      ,count(1) event_cnt
      ,sum(t1.du) sum_du
      ,rpad(reverse(bin(cast(grouping__id as int))),4,'0') grain_ind
  from intdata.kesheng_event_occur t1
 where t1.src_file_day = '${SRC_FILE_DAY}' 
   and t1.src_file_hour = '${SRC_FILE_HOUR}'
 group by app_channel_id, product_key, app_ver_code, event_name
grouping sets (event_name, 					--如果不加上event_name,那么event_name一直为空
              (app_channel_id, event_name),
 			  (product_key, event_name),
			  (app_channel_id, product_key, event_name),
			  (product_key, app_ver_code, event_name),
			  (app_channel_id, product_key, app_ver_code, event_name));
			  
			  
--------------------------------------------------------------------------------------------------------------			  
			  
set mapreduce.job.name=rptdata.fact_kesheng_event_params_hourly_${SRC_FILE_DAY}_${SRC_FILE_HOUR};

set hive.groupby.skewindata=true;
set hive.optimize.skewjoin=true;
set hive.merge.mapredfiles=true;


insert overwrite table rptdata.fact_kesheng_event_params_hourly 
       partition(src_file_day='${SRC_FILE_DAY}', src_file_hour='${SRC_FILE_HOUR}')
select t1.app_channel_id
      ,t1.product_key
      ,t1.app_ver_code
      ,t1.event_name
      ,min(t1.param_pos) param_pos
      ,t1.param_name
      ,t1.param_val
      ,count(1) val_cnt
      ,rpad(reverse(bin(cast(grouping__id as int))),6,'0') grain_ind
 from intdata.kesheng_event_params t1
where t1.src_file_day = '${SRC_FILE_DAY}' 
  and t1.src_file_hour = '${SRC_FILE_HOUR}'
group by app_channel_id, product_key, app_ver_code
        ,event_name, param_name, param_val
grouping sets ((event_name, param_name, param_val), 
			  (app_channel_id, event_name, param_name, param_val),
			  (product_key, event_name, param_name, param_val),
              (app_channel_id, product_key,event_name, param_name, param_val),
              (product_key, app_ver_code, event_name, param_name, param_val),
              (app_channel_id, product_key, app_ver_code, event_name, param_name, param_val));

			  
			  
			  