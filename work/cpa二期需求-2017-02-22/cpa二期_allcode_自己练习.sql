
drop table ods.kesheng_sec_json_ex_dy;
create external table ods.kesheng_sec_json_ex_dy (
    json string
)
partitioned by (src_file_day string, src_file_hour string)
location '/tmp/tony';

-- 1.解析ods.kesheng_sec_json_ex (第一层：sdkSessionInfo)
drop table if exists temp.odsdata_kesheng_sec_event_json_dy;
create table odsdata.kesheng_sec_event_json_dy(
rowkey string,
imei string,
udid string,
idfa string,
idfv string,
installationid string,
clientid string,
appversion string,
apppkg string,
appchannel string,
os string,
networktype string,
account string,
customevent_json string
)partitioned by (src_file_day string, src_file_hour string);


insert overwrite table temp.odsdata_kesheng_sec_event_json_dy partition(src_file_day='${SRC_FILE_DAY}', src_file_hour='${SRC_FILE_HOUR}')
 select concat(INPUT__FILE__NAME,':',BLOCK__OFFSET__INSIDE__FILE) as rowkey
	   ,c.imei
       ,c.udid
	   ,c.idfa
	   ,c.idfv
	   ,c.installationID
	   ,c.clientId
	   ,c.appVersion
	   ,c.apppkg
	   ,c.appchannel
	   ,c.os
	   ,c.networkType
	   ,c.account
	   ,b.customEvent_json
   from ods.kesheng_sec_json_ex_dy a
		lateral view json_tuple(a.json, 'customEvent', 'sdkSessionInfo') b as customEvent_json, sdkSessionInfo_json
		lateral view json_tuple(b.sdkSessionInfo_json, 'imei', 'udid', 'idfa', 
						'idfv', 'installationID', 'clientId', 'appVersion', 
						'apppkg', 'appchannel', 'os', 'networkType', 'account') c as imei, udid, idfa, idfv, installationID, clientId, 
																					 appVersion, apppkg, appchannel, os, networkType, account
  where a.src_file_day = '${SRC_FILE_DAY}' and a.src_file_hour = '${SRC_FILE_HOUR}' 
    and nvl(b.sdkSessionInfo_json,'{}') <> '{}' ;

		
-- 2.解析ods.kesheng_sec_json_ex (第二层：customEvent) 

create or replace view temp.ods_kesheng_sec_event_param_json_v_dy
as 
select a.rowkey
	  ,a.imei
      ,a.udid
	  ,a.idfa
	  ,a.idfv
	  ,a.installationID as install_id
	  ,a.clientId as client_id
	  ,a.appVersion as app_ver_code
	  ,a.apppkg as app_pkg_name
	  ,a.appchannel as app_channel_id
	  ,a.os as app_os_type
	  ,a.networkType as network_type
	  ,a.account
	  ,c.eventName as event_name
	  ,c.du
	  ,c.timestamp
	  ,c.eventParams_json
	  ,a.src_file_day
	  ,a.src_file_hour
  from temp.odsdata_kesheng_sec_event_json_dy a 
       lateral view explode(split(regexp_replace(regexp_replace(a.customEvent_json, '\\[|\\]', ''), '\\}\\,\\{', '\\}\\|\\|\\{'), '\\|\\|')) b 
			   as event_json
	   lateral view json_tuple(b.event_json, 'eventName', 'du', 'timestamp', 'eventParams') c 
			   as eventName, du, timestamp, eventParams_json;

-- 3.解析ods.kesheng_sec_json_ex (第三层：eventParams) 

drop table if exists temp.intdata_kesheng_sec_event_params_dy;
create table temp.intdata_kesheng_sec_event_params_dy(
rowkey string,
app_ver_code string,
app_pkg_name string,
app_channel_id string,
app_os_type string,
event_name string,
param_name string,
param_val string,
product_key smallint
)
partitioned by (src_file_day string, src_file_hour string);


with stg_ods_kesheng_sec_event_param_json_v_dy as 
(
select v.rowkey
	  ,v.app_ver_code
	  ,v.app_pkg_name
	  ,v.app_channel_id
	  ,v.app_os_type
	  ,v.event_name
	  ,v.eventParams_json
	  ,nvl(d1.product_key, -998) as product_key
from temp.ods_kesheng_sec_event_param_json_v_dy v 
left join mscdata.dim_kesheng_sdk_app_pkg d1 
  on (v.app_pkg_name = d1.app_pkg_name and v.app_os_type = d1.app_os_type)
where v.src_file_day = '${SRC_FILE_DAY}' 
  and v.src_file_hour = '${SRC_FILE_HOUR}'
  and nvl(v.event_name, '') <> '' 
)
insert overwrite table temp.intdata_kesheng_sec_event_params_dy partition(src_file_day='${SRC_FILE_DAY}', src_file_hour='${SRC_FILE_HOUR}')
select t.rowkey
	  ,t.app_ver_code
	  ,t.app_pkg_name 
	  ,t.app_channel_id 
	  ,t.app_os_type 	  
	  ,t.event_name 
	  ,t.param_key_val[0] as param_name
	  ,t.param_key_val[1] as param_val
	  ,t.product_key 
  from (
		select  a.rowkey
			   ,a.app_ver_code
			   ,a.app_pkg_name
			   ,a.app_channel_id
			   ,a.app_os_type
			   ,a.product_key 
			   ,a.event_name
			   ,b.param_pos
			   ,split(b.param_key_val,'\\s*:\\s*') as param_key_val
		  from stg_ods_kesheng_sec_event_param_json_v_dy a 
			   lateral view posexplode(split(regexp_replace(regexp_replace(a.eventParams_json, '\\{|\\}', ''), '"', ''), '\\s*,\\s*')) b
			   as param_pos, param_key_val
		) t;



-- intdata层:事件发生表 ---------------------------------

drop table if exists temp.intdata_kesheng_sec_event_occur_dy;
create table temp.intdata_kesheng_sec_event_occur_dy(
rowkey          string,
app_ver_code    string,
app_pkg_name    string,
app_channel_id  string,
app_os_type     string,
event_name      string,
du              string,
timestamp       string,
product_key     smallint
)
partitioned by (src_file_day string, src_file_hour string);

insert overwrite table temp.intdata_kesheng_sec_event_occur_dy partition (src_file_day = '${SRC_FILE_DAY}' and src_file_hour = '${SRC_FILE_HOUR}')
select v.rowkey
	  ,v.app_ver_code
	  ,v.app_pkg_name
	  ,v.app_channel_id
	  ,v.app_os_type
	  ,v.event_name
	  ,v.du
	  ,v.timestamp
	  ,nvl(d.product_key,-998) product_key
  from temp.ods_kesheng_sec_event_param_json_v_dy v 
  left join mscdata.dim_kesheng_sdk_app_pkg d
    on (v.app_pkg_name = d.app_pkg_name and v.app_os_type = d.app_os_type)
 where v.src_file_day = '${SRC_FILE_DAY}' 
   and v.src_file_hour = '${SRC_FILE_HOUR}'
   and nvl(v.event_name, '') <> '';
   
      
-- rptdata层:事件发生汇总 ---------------------------------   

drop table if exists temp.rptdata_fact_kesheng_sec_event_occur_hourly_dy ;

create table temp.rptdata_fact_kesheng_sec_event_occur_hourly_dy 
(
app_channel_id string,
product_key    smallint,
app_ver_code   string,
event_name     string,
event_cnt      bigint,
sum_du         decimal(20,2),
grain_ind      string   
) partitioned by (src_file_day string, src_file_hour string);


insert overwrite table temp.rptdata_fact_kesheng_sec_event_occur_hourly_dy partition (src_file_day = '${SRC_FILE_DAY}', src_file_hour = '${SRC_FILE_HOUR}')
select a.app_channel_id
	  ,a.product_key
	  ,a.app_ver_code
	  ,a.event_name
	  ,count(1) as event_cnt
	  ,sum(a.du) as sum_du
	  ,rpad(reverse(bin(cast(grouping__id as int))),3,'0') as grain_ind	
  from temp.intdata_kesheng_sec_event_occur_dy a 
 where a.src_file_day = '${SRC_FILE_DAY}' 
   and a.src_file_hour = '${SRC_FILE_HOUR}'
 group by app_channel_id, product_key, app_ver_code, event_name 
grouping sets((), app_channel_id, product_key, 
			  (app_channel_id, product_key),
			  (product_key, app_ver_code),
			  (app_channel_id, product_key, app_ver_code));

 
-- rptdata层:事件参数汇总 ---------------------------------   

drop table if exists temp.rptdata_fact_kesheng_sec_event_params_hourly_dy;

create table temp.rptdata_fact_kesheng_sec_event_params_hourly_dy
(
app_channel_id string,
product_key    smallint,
app_ver_code   string,
event_name     string,
param_name     string,
param_val      string,
val_cnt        bigint,
grain_ind      string
)partitioned  by (src_file_day string, src_file_hour string);

insert overwrite table temp.rptdata_fact_kesheng_sec_event_params_hourly_dy partition (src_file_day = '${SRC_FILE_DAY}', src_file_hour = '${SRC_FILE_HOUR}')
select a.app_channel_id
	  ,a.product_key
	  ,a.app_ver_code
	  ,a.event_name
	  ,a.param_name
	  ,a.param_val
	  ,count(1) as val_cnt
	  ,rpad(reverse(bin(cast(grouping__id as int))),3,'0') as grain_ind	
  from temp.intdata_kesheng_sec_event_params_dy a 
 where a.src_file_day = '${SRC_FILE_DAY}' 
   and a.src_file_hour = '${SRC_FILE_HOUR}'
 group by app_channel_id, product_key, app_ver_code, event_name, param_name, param_val 
grouping sets((), app_channel_id, product_key, 
			  (app_channel_id, product_key),
			  (product_key, app_ver_code),
			  (app_channel_id, product_key, app_ver_code));


-- app层:事件发生（小时）  ---------------------------------   
drop table if exists temp.app_cpa_sec_event_occur_hourly_dy;

create table temp.app_cpa_sec_event_occur_hourly_dy(
product_key string,
product_name string,
app_ver_code string,
app_channel_id string,
event_name string,
event_cnt bigint,			
sum_du decimal(20,2),	
avg_du decimal(20,2)	
)
partitioned by (src_file_day string, src_file_hour string);


insert overwrite table temp.app_cpa_sec_event_occur_hourly_dy partition (src_file_day = '${SRC_FILE_DAY}', src_file_hour = '${SRC_FILE_HOUR}')

select t1.product_key 
	  ,if(t1.product_key = -1, '-1', nvl(b1.product_name,'')) product_name 
	  ,t1.app_ver_code 
	  ,t1.app_channel_id 
	  ,t1.event_name 
	  ,t1.event_cnt 
	  ,t1.sum_du 
	  ,if(t1.event_cnt = 0, 0, round(t1.sum_du/t1.event_cnt,2)) as avg_du 
  from (		
		select if(substr(a.grain_ind,1,1) = '0', '-1', a.app_channel_id) as app_channel_id
			  ,if(substr(a.grain_ind,2,1) = '0', -1, a.product_key) as product_key
			  ,if(substr(a.grain_ind,3,1) = '0', '-1', a.app_ver_code) as app_ver_code 
			  ,a.event_name
			  ,sum(a.event_cnt) as event_cnt   
			  ,sum(a.sum_du) as sum_du 	
		  from temp.rptdata_fact_kesheng_sec_event_occur_hourly_dy a  
		 where a.src_file_day = '${SRC_FILE_DAY}' and a.src_file_hour = '${SRC_FILE_HOUR}' 
		 group by if(substr(a.grain_ind,1,1) = '0', '-1', a.app_channel_id) 
				 ,if(substr(a.grain_ind,2,1) = '0', -1, a.product_key) 
				 ,if(substr(a.grain_ind,3,1) = '0', '-1', a.app_ver_code)
				 ,a.event_name
       ) t1
 left join mscdata.dim_kesheng_sdk_app_pkg b1 on t1.product_key = b1.product_key 
where d1.product_key is not null or t1.product_key = -1;


-- app层:事件参数（小时）  ---------------------------------  


