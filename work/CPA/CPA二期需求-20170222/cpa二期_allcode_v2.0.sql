

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

CREATE VIEW `ods.kesheng_event_json_v` AS select 
	   concat(`k2`.`input_file_name`,':',`k2`.`block_offset_inside_file`) `rowkey`
      ,`k2`.`line`[0] `json`				--对一些特殊字符需要做处理, \u000 \u01f
	  ,`k2`.`line`[1] `client_ip`
      ,`k2`.`src_file_day`
      ,`k2`.`src_file_hour`      
  from (select split(`k1`.`line`,'\\|\\|') `line`
              ,`k1`.`src_file_day` ,`k1`.`src_file_hour`
              ,`k1`.`input__file__name` `input_file_name`  ,`k1`.`block__offset__inside__file` `block_offset_inside_file`
          from `ods`.`kesheng_event_raw_ex` `k1`
       ) `k2`
       
           
-------------------------------------------------------------------------------------      


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
create or replace view ods.kesheng_event_param_json_v
as 
select e1.rowkey
      ,e1.imei
      ,e1.udid
      ,e1.idfa
      ,e1.idfv
      ,e1.installationID install_id
      ,e1.clientId client_id
      ,e1.appVersion app_ver_code
      ,e1.apppkg app_pkg_name
      ,regexp_extract(e1.appchannel,'([^-]+$)',1) as app_channel_id		
      ,e1.appchannel app_channel_src	--保留原始的渠道ID
      ,e1.os app_os_type
      ,e1.networkType network_type
      ,e1.account
      ,j2.eventName event_name
      ,j2.du
      ,j2.timestamp
      ,j2.eventParams_json
      ,e1.src_file_day
      ,e1.src_file_hour
  from odsdata.kesheng_event_json e1
       lateral view explode(split(regexp_replace(e1.customEvent_json,'\\}\\,\\{', '\\}\\|\\|\\{'), '\\|\\|')) j1 
                 as event_json
       lateral view json_tuple(j1.event_json, 'eventName', 'du', 'timestamp', 'eventParams') j2
                 as eventName, du, timestamp, eventParams_json

------------------------------------------------------------------------------------------------------------------- 

insert overwrite table intdata.kesheng_event_occur partition(src_file_day='${SRC_FILE_DAY}', src_file_hour='${SRC_FILE_HOUR}')
select t1.rowkey
      ,t1.app_ver_code
      ,t1.app_pkg_name		----
      ,t1.app_channel_id
	  ,t1.app_channel_src 
      ,t1.app_os_type		----
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

with stg_kesheng_event_param_json as
(select a.rowkey
       ,a.app_ver_code
       ,a.app_pkg_name
       ,a.app_channel_id
       ,a.app_channel_src
       ,a.app_os_type
       ,a.event_name
       ,nvl(d1.product_key, -998) product_key
       ,a.eventParams_json
  from ods.kesheng_event_param_json_v a
  left join mscdata.dim_kesheng_sdk_app_pkg d1
    on (a.app_pkg_name = d1.app_pkg_name and a.app_os_type = d1.app_os_type)
 where a.src_file_day = '${SRC_FILE_DAY}' 
   and a.src_file_hour = '${SRC_FILE_HOUR}'
   and nvl(a.event_name, '')<>''                 --剔除空数据
   and nvl(a.app_ver_code,'')<>''                --剔除空数据
   and nvl(a.app_pkg_name,'')<>''                --剔除空数据
   and nvl(a.app_channel_id,'')<>''              --剔除空数据
   and nvl(a.app_os_type,'')<>''                 --剔除空数据
)
insert overwrite table intdata.kesheng_event_params partition(src_file_day='${SRC_FILE_DAY}', src_file_hour='${SRC_FILE_HOUR}')
select t3.rowkey
      ,t3.app_ver_code
      ,t3.app_pkg_name
      ,t3.app_channel_id
      ,t3.app_channel_src
      ,t3.app_os_type
      ,t3.event_name
      ,t3.param_pos
      ,t3.param_key_val[0] param_name
      ,t3.param_key_val[1] param_val
      ,t3.product_key
from (select t1.rowkey ,t1.app_ver_code ,t1.app_pkg_name ,t1.app_channel_id ,t1.app_channel_src
,t1.app_os_type ,t1.event_name ,t1.product_key, p1.param_pos
,split(p1.param_key_val, '\\s*:\\s*') param_key_val
from stg_kesheng_event_param_json t1
lateral view posexplode(split(regexp_replace(regexp_replace(t1.eventParams_json, '\\{|\\}', ''), '"', ''), '\\s*,\\s*')) p1 
as param_pos, param_key_val
) t3
where nvl(t3.param_key_val[0],'')<>''      --剔除空数据
  and nvl(t3.param_key_val[1],'')<>'';     --剔除空数据


-- 分割 param_name 和 param_val 出现问题
--------------------------------------------------------------------------------------------------------------

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
grouping sets (event_name,                  --如果不加上event_name,那么event_name一直为空
              (app_channel_id, event_name),
              (product_key, event_name),
              (app_channel_id, product_key, event_name),
              (product_key, app_ver_code, event_name),
              (app_channel_id, product_key, app_ver_code, event_name));
              
              
--------------------------------------------------------------------------------------------------------------            


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

--------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------


insert overwrite table app.cpa_event_occur_daily partition (src_file_day = '${SRC_FILE_DAY}')
select t1.product_key
	  ,if(t1.product_key=-1, '-1', nvl(b1.product_name,'')) product_name
	  ,t1.app_ver_code
	  ,t1.app_channel_id
	  ,t1.event_name
	  ,t1.event_cnt
	  ,t1.sum_du 
	  ,if(t1.event_cnt = 0, 0, round(t1.sum_du/t1.event_cnt,2)) as avg_du
  from( 
		select if(substr(a.grain_ind,1,1)= '0', '-1', a.app_channel_id) as app_channel_id
			  ,if(substr(a.grain_ind,2,1)= '0', -1, a.product_key) as product_key
			  ,if(substr(a.grain_ind,3,1)= '0', '-1', a.app_ver_code) as app_ver_code
			  ,a.event_name
			  ,sum(a.event_cnt) as event_cnt
			  ,sum(a.sum_du) as sum_du
		 from rptdata.fact_kesheng_event_occur_hourly a 
		where a.src_file_day = '${SRC_FILE_DAY}' 
		group by if(substr(a.grain_ind,1,1)= '0', '-1', a.app_channel_id)
				,if(substr(a.grain_ind,2,1)= '0', -1, a.product_key)
				,if(substr(a.grain_ind,3,1)= '0', '-1', a.app_ver_code)
				,a.event_name
      )t1
 left join mscdata.dim_kesheng_sdk_product b1 on t1.product_key = b1.product_key
where b1.product_key is not null or t1.product_key = -1
;

        
              