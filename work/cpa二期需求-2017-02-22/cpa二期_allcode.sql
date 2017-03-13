
-- === ods.kesheng_sec_json_ex ===================================================================================

drop table ods.kesheng_sec_json_ex;
create external table ods.kesheng_sec_json_ex (
    json string
)
partitioned by (src_file_day string, src_file_hour string)
location '/user/hadoop/ods/migu/kesheng/kesheng_sec';

-- === odsdata.kesheng_sec_event_json ===================================================================================

set mapreduce.job.name=odsdata.kesheng_sec_event_json_${SRC_FILE_DAY}_${SRC_FILE_HOUR};
set hive.merge.mapredfiles=true;

insert overwrite table odsdata.kesheng_sec_event_json partition(src_file_day='${SRC_FILE_DAY}', src_file_hour='${SRC_FILE_HOUR}')
select concat(INPUT__FILE__NAME,':',BLOCK__OFFSET__INSIDE__FILE) rowkey
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
      ,regexp_replace(j1.customEvent_json,'\\[|\\]','') customEvent_json
  from ods.kesheng_sec_json_ex e1
       lateral view json_tuple(e1.json,'customEvent') j1 as customEvent_json
       lateral view json_tuple(e1.json,'sdkSessionInfo') j2 as sdkSessionInfo_json
       lateral view json_tuple(j2.sdkSessionInfo_json, 'imei', 'udid', 'idfa', 'idfv', 'installationID', 'clientId'
                                            , 'appVersion', 'apppkg', 'appchannel', 'os', 'networkType', 'account') p2
                 as imei, udid, idfa, idfv, installationID, clientId 
                   ,appVersion, apppkg, appchannel, os, networkType, account
 where e1.src_file_day='${SRC_FILE_DAY}' and e1.src_file_hour='${SRC_FILE_HOUR}'
   and nvl(j2.sdkSessionInfo_json,'{}') <> '{}';
   
-- === ods.kesheng_sec_event_param_json_v =================================================================================== 
  
create or replace view ods.kesheng_sec_event_param_json_v
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
      ,e1.appchannel app_channel_id
      ,e1.os app_os_type
      ,e1.networkType network_type
      ,e1.account
      ,j2.eventName event_name
      ,j2.du
      ,j2.timestamp
      ,j2.eventParams_json
      ,e1.src_file_day
      ,e1.src_file_hour
  from odsdata.kesheng_sec_event_json e1
       lateral view explode(split(regexp_replace(e1.customEvent_json,'\\}\\,\\{', '\\}\\|\\|\\{'), '\\|\\|')) j1 
                 as event_json
       lateral view json_tuple(j1.event_json, 'eventName', 'du', 'timestamp', 'eventParams') j2
                 as eventName, du, timestamp, eventParams_json
        
-- === intdata.kesheng_sec_event_occur ===================================================================================
   
set mapreduce.job.name=intdata.kesheng_sec_event_occur_${SRC_FILE_DAY}_${SRC_FILE_HOUR};
set hive.merge.mapredfiles=true;

insert overwrite table intdata.kesheng_sec_event_occur partition(src_file_day='${SRC_FILE_DAY}', src_file_hour='${SRC_FILE_HOUR}')
select t1.rowkey
      ,t1.app_ver_code
      ,t1.app_pkg_name
      ,t1.app_channel_id
      ,t1.app_os_type
      ,t1.event_name
      ,t1.du
      ,t1.timestamp
      ,nvl(d1.product_key, -998) product_key
  from ods.kesheng_sec_event_param_json_v t1
  left join mscdata.dim_kesheng_sdk_app_pkg d1
    on (t1.app_pkg_name = d1.app_pkg_name and t1.app_os_type = d1.app_os_type)
 where t1.src_file_day = '${SRC_FILE_DAY}' 
   and t1.src_file_hour = '${SRC_FILE_HOUR}'
   and nvl(t1.event_name, '') <> '';

-- === intdata.kesheng_sec_event_params ===================================================================================
     
set mapreduce.job.name=intdata.kesheng_sec_event_params_${SRC_FILE_DAY}_${SRC_FILE_HOUR};
set hive.merge.mapredfiles=true;

with stg_kesheng_sec_event_param_json as
(select a.rowkey
       ,a.app_ver_code
       ,a.app_pkg_name
       ,a.app_channel_id
       ,a.app_os_type
       ,a.event_name
       ,nvl(d1.product_key, -998) product_key
       ,a.eventParams_json
  from ods.kesheng_sec_event_param_json_v a
  left join mscdata.dim_kesheng_sdk_app_pkg d1
    on (a.app_pkg_name = d1.app_pkg_name and a.app_os_type = d1.app_os_type)
 where a.src_file_day = '${SRC_FILE_DAY}' 
   and a.src_file_hour = '${SRC_FILE_HOUR}'
   and nvl(a.event_name, '') <> ''
)
insert overwrite table intdata.kesheng_sec_event_params partition(src_file_day='${SRC_FILE_DAY}', src_file_hour='${SRC_FILE_HOUR}')
select t3.rowkey
      ,t3.app_ver_code
      ,t3.app_pkg_name
      ,t3.app_channel_id
      ,t3.app_os_type
      ,t3.event_name
      ,translate(t3.param_key_val[0], '"', '') param_name
      ,translate(t3.param_key_val[1], '"', '') param_val
      ,t3.product_key
  from (select t1.rowkey ,t1.app_ver_code ,t1.app_pkg_name ,t1.app_channel_id
              ,t1.app_os_type ,t1.event_name ,t1.product_key
              ,split(p1.param_key_val, '"\\s*:\\s*"') param_key_val
          from stg_kesheng_sec_event_param_json t1
               lateral view posexplode(split(regexp_replace(j2.eventParams_json, '\\{|\\}', ''), '"\\s*,\\s*"')) p1 
                         as param_pos, param_key_val
        ) t3
 where nvl(translate(t3.param_key_val[0], '"', ''), '') <> ''
   and nvl(translate(t3.param_key_val[1], '"', ''), '') <> '';

-- \s是指空白，包括空格、换行、tab缩进等所有的空白
   
   
-- === rptdata.fact_kesheng_sec_event_occur_hourly ===================================================================================
   
set mapreduce.job.name=rptdata.fact_kesheng_sec_event_occur_hourly_${SRC_FILE_DAY}_${SRC_FILE_HOUR};
set hive.groupby.skewindata=true;
set hive.optimize.skewjoin=true;
set hive.merge.mapredfiles=true;

insert overwrite table rptdata.fact_kesheng_sec_event_occur_hourly 
       partition(src_file_day='${SRC_FILE_DAY}', src_file_hour='${SRC_FILE_HOUR}')
select t1.app_channel_id
      ,t1.product_key
      ,t1.app_ver_code
      ,t1.event_name
      ,count(1) event_cnt
      ,sum(t1.du) sum_du
      ,rpad(reverse(bin(cast(grouping__id as int))),3,'0') grain_ind
  from intdata.kesheng_sec_event_occur t1
 where t1.src_file_day = '${SRC_FILE_DAY}' 
   and t1.src_file_hour = '${SRC_FILE_HOUR}'
 group by t1.app_channel_id, t1.product_key, t1.app_ver_code, t1.event_name
grouping set ((), app_channel_id, product_key,
              (app_channel_id, product_key),
              (product_key, app_ver_code),
              (app_channel_id, product_key, app_ver_code));   
   
-- === rptdata.fact_kesheng_sec_event_params_hourly ===============================================================================
   
set mapreduce.job.name=rptdata.fact_kesheng_sec_event_params_hourly_${SRC_FILE_DAY}_${SRC_FILE_HOUR};
set hive.groupby.skewindata=true;
set hive.optimize.skewjoin=true;
set hive.merge.mapredfiles=true;

insert overwrite table rptdata.fact_kesheng_sec_event_params_hourly 
       partition(src_file_day='${SRC_FILE_DAY}', src_file_hour='${SRC_FILE_HOUR}')
select t1.app_channel_id
      ,t1.product_key
      ,t1.app_ver_code
      ,t1.event_name
      ,min(t1.param_pos) param_pos
      ,t1.param_name
      ,t1.param_val
      ,count(1) val_cnt		-- 注意
      ,rpad(reverse(bin(cast(grouping__id as int))),3,'0') grain_ind
 from intdata.kesheng_sec_event_params t1
where t1.src_file_day = '${SRC_FILE_DAY}' 
  and t1.src_file_hour = '${SRC_FILE_HOUR}'
group by t1.app_channel_id, t1.product_key, t1.app_ver_code		--注意这里group by后面的字段
        ,t1.event_name, t1.param_name, t1.param_val		-- 针对每一个事件，每一个参数，每一个参数值出现的次数(val_cnt)是多少？
grouping set ((), app_channel_id, product_key,
              (app_channel_id, product_key),
              (product_key, app_ver_code),
              (app_channel_id, product_key, app_ver_code));
			  
-- =================================================================================================== --	
		  
-- 1、限制param_name的个数小于等于20个，依据param_pos、时间排序
-- 2、限制param_val的值个数小于等于1000个，依据val_cnt排序
select a.param_name, a.param_val
  from (select a.param_name, a.param_val, sum(a.val_cnt) val_cnt
              ,dense_rank()over(partition by a.param_name 
                                    order by min(a.src_file_hour) asc, min(a.param_pos) asc) param_name_rank
              ,row_number()over(partition by a.param_name, a.param_val 
                                    order by sum(a.val_cnt) desc) param_val_rank
          from rptdata.fact_kesheng_sec_event_params_hourly a
         where a.src_file_day = '${SRC_FILE_DAY}' 	-- 一天以内 
           and a.grain_ind = '000'						
         group by a.param_name, a.param_val
        ) t1
 where t1.param_name_rank <= 20 and t1.param_val_rank <= 1000;

取prama_name与param_val的大概逻辑是这样的

-- rptdata.fact_kesheng_sec_event_params_hourly表增加了参数位置字段param_pos

select a1.event_name
	  ,a1.param_name
	  ,a1.param_val
	  ,a1.val_cnt
  from (select a.event_name
			  ,a.param_name
			  ,a.param_val
			  ,sum(a.val_cnt) val_cnt
              ,row_number()over(partition by a.param_name, a.param_val order by sum(a.val_cnt) desc) param_val_rank	-- 按照param_val出现的次数(权重)来排序,最多的排第一位
          from rptdata.fact_kesheng_sec_event_params_hourly a
         where a.src_file_day = '${SRC_FILE_DAY}' and a.src_file_hour = '{SRC_FILE_HOUR}' 
           and a.grain_ind = '000'	
         group by a.param_name, a.param_val
        ) a1
 where a1.param_val_rank <= 1000;
 
 