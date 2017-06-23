{"customEvent":[{"eventName":"sdkinvoke","du":"0","timestamp":"1492565494787","eventParams":{"clientId":"3be580a56a71bd5551ca9fbca283b711","timestamp":"2017-04-19 09:31:35:706"}},
                {"eventName":"tokenvalidate","du":"0","timestamp":"1492565524361","eventParams":{"timestamp":"2017-04-19 09:32:05:276","token":"8484010001320200524D7A41794F5551304D6A5579515549334D444D784E55597940687474703A2F2F70617373706F72742E6D6967752E636E2F403165643532376136343663613437633239663537383962656131613866396437030004015B4837040006323033303035FF0020B20401B8E74BA5D32B42456D2AA8A045CB62257C23D931F684DBD030DEDB8BF7","clientId":"3be580a56a71bd5551ca9fbca283b711"}},
                {"eventName":"sdkcallback","du":"0","timestamp":"1492565524363","eventParams":{"timestamp":"2017-04-19 09:32:05:278","sdkcallback":"8484010001320200524D7A41794F5551304D6A5579515549334D444D784E55597940687474703A2F2F70617373706F72742E6D6967752E636E2F403165643532376136343663613437633239663537383962656131613866396437030004015B4837040006323033303035FF0020B20401B8E74BA5D32B42456D2AA8A045CB62257C23D931F684DBD030DEDB8BF7","clientId":"3be580a56a71bd5551ca9fbca283b711"}},
                {"eventName":"tokenvalidateres","du":"0","timestamp":"1492565525103","eventParams":{"resultDesc":"migutoken login success","timestamp":"2017-04-19 09:32:06:23","clientId":"3be580a56a71bd5551ca9fbca283b711","resultCode":"LOGIN_SUCCESS"}},
                {"eventName":"portalres",
                 "du":"0",
                 "timestamp":"1492565525774",
                 "eventParams":{"resultDesc":"success",
                                "timestamp":"2017-04-19 09:32:06:693",
                                "clientId":"3be580a56a71bd5551ca9fbca283b711",
                                "resultCode":"1"}
                }],
 "sdkSessionInfo":{"udid":"ffffffff-ddfb-6b00-0000-0000000000001489894156845",
                   "clientId":"3be580a56a71bd5551ca9fbca283b711",
                   "apppkg":"com.cmcc.cmvideo",
                   "appVersion":"3.0.1.4",
                   "appchannel":"23000104-99000-200300290000000\n",
                   "os":"AD",
                   "installationID":"ffffffff-ddfb-6b00-0000-0000000000001489894156845",
                   "networkType":"UNKNOWN",
                   "account":"",
                   "imei":""}
}||223.104.6.30 
20170419    09


================================================================================================================================================

desc `ods`.`kesheng_event_raw_ex`;
OK
line                    string                                      
src_file_day            string      --分区                            
src_file_hour           string      --分区

hdfs://ns1/user/hadoop/ods/migu/kesheng/kesheng_event

================================================================================================================================================

CREATE VIEW `ods.kesheng_event_json_v` AS select 
       concat(`k2`.`input_file_name`,':',`k2`.`block_offset_inside_file`) `rowkey`
      ,regexp_replace(`k2`.`line`[0], '\\\\n|\\\\r|\\\\u001f|\\\\u0000', '') `json`
      ,`k2`.`line`[1] `client_ip`
      ,`k2`.`src_file_day`
      ,`k2`.`src_file_hour`      
  from (select split(`k1`.`line`,'\\|\\|') `line`
              ,`k1`.`src_file_day` 
              ,`k1`.`src_file_hour`
              ,`k1`.`input__file__name` `input_file_name`  
              ,`k1`.`block__offset__inside__file` `block_offset_inside_file`
          from `ods`.`kesheng_event_raw_ex` `k1`
       ) `k2`
       
-- \u开头的是一个Unicode码的字符，每一个'\u0000'代表的是NULL, 输出控制台是一个空格,可以查看Unicode表。
-- Unicode字符通常用十六进制编码方案表示，范围在'\u0000'到'\uFFFF'之间。
-- 通常中文在的显示都是被转化为Unicode显示的。
-- Unicode可同时包含65536个字符，ASCII/ANSI只包含255个字符，实际上是Unicode的一个子集，范围为\u0000到\u00FF。
-- \u001f表示一个 不可见分隔符。
================================================================================================================================================

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

================================================================================================================================================

CREATE VIEW `ods.kesheng_event_param_json_v` AS select `e1`.`rowkey`
      ,`e1`.`imei`
      ,`e1`.`udid`
      ,`e1`.`idfa`
      ,`e1`.`idfv`
      ,`e1`.`installationid` `install_id`
      ,`e1`.`clientid` `client_id`
      ,`e1`.`appversion` `app_ver_code`
      ,`e1`.`apppkg` `app_pkg_name`
      ,regexp_extract(`e1`.`appchannel`,'([^-]+$)',1) as `app_channel_id`
      ,`e1`.`appchannel` `app_channel_src`
      ,`e1`.`os` `app_os_type`
      ,`e1`.`networktype` `network_type`
      ,`e1`.`account`
      ,`j2`.`eventname` `event_name`
      ,`j2`.`du`
      ,`j2`.`timestamp`
      ,`j2`.`eventparams_json`
      ,`e1`.`src_file_day`
      ,`e1`.`src_file_hour`
  from `odsdata`.`kesheng_event_json` `e1`
       lateral view explode(split(regexp_replace(`e1`.`customevent_json`,'\\}\\,\\{', '\\}\\|\\|\\{'), '\\|\\|')) `j1` 
                 as `event_json`
       lateral view json_tuple(`j1`.`event_json`, 'eventName', 'du', 'timestamp', 'eventParams') `j2`
                 as `eventName`, `du`, `timestamp`, `eventParams_json`

================================================================================================================================================

insert overwrite table intdata.kesheng_event_occur partition(src_file_day='${SRC_FILE_DAY}', src_file_hour='${SRC_FILE_HOUR}')
select t1.rowkey
      ,t1.app_ver_code
      ,t1.app_pkg_name
      ,t1.app_channel_id
      ,t1.app_channel_src
      ,t1.app_os_type
      ,t1.event_name
      ,t1.du
      ,t1.timestamp
      ,nvl(d1.product_key, -998) product_key
  from ods.kesheng_event_param_json_v t1
  left join mscdata.dim_kesheng_sdk_app_pkg d1
    on (t1.app_pkg_name = d1.app_pkg_name and t1.app_os_type = d1.app_os_type)
 where t1.src_file_day = '${SRC_FILE_DAY}'
   and nvl(t1.app_ver_code,'')<>'' 
   and nvl(t1.app_pkg_name,'')<>'' 
   and nvl(t1.app_channel_id,'')<>'' 
   and nvl(t1.app_os_type,'')<>'' 
   and t1.src_file_hour = '${SRC_FILE_HOUR}'
   and nvl(t1.event_name, '') <> '';
   
================================================================================================================================================   

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
   and nvl(a.event_name, '')<>'' 
   and nvl(a.app_ver_code,'')<>'' 
   and nvl(a.app_pkg_name,'')<>'' 
   and nvl(a.app_channel_id,'')<>'' 
   and nvl(a.app_os_type,'')<>''
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
      ,translate(t3.param_key_val[0], '"', '') param_name
      ,translate(t3.param_key_val[1], '"', '') param_val
      ,t3.product_key
from (select t1.rowkey 
            ,t1.app_ver_code 
            ,t1.app_pkg_name 
            ,t1.app_channel_id 
            ,t1.app_channel_src
            ,t1.app_os_type 
            ,t1.event_name 
            ,t1.product_key
            ,p1.param_pos
            ,split(p1.param_key_val, '"\\s*:\\s*"') param_key_val
        from stg_kesheng_event_param_json t1
     lateral view posexplode(split(regexp_replace(t1.eventParams_json, '\\{|\\}', ''), '"\\s*,\\s*"')) p1 
          as param_pos, param_key_val
) t3
where nvl(translate(t3.param_key_val[0], '"', ''),'')<>'' 
  and nvl(translate(t3.param_key_val[1], '"', ''),'')<>'';
  
================================================================================================================================================

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
grouping sets (event_name, 
              (app_channel_id, event_name),
              (product_key, event_name),
              (app_channel_id, product_key, event_name),
              (product_key, app_ver_code, event_name),
              (app_channel_id, product_key, app_ver_code, event_name));

================================================================================================================================================

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
              
================================================================================================================================================


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
================================================================================================================================================

insert overwrite table stg.cpa_event_params_daily_01
select if(substr(a.grain_ind,1,1)='0', '-1', a.app_channel_id) as app_channel_id 
      ,if(substr(a.grain_ind,2,1)='0', -1, a.product_key) as product_key 
      ,if(substr(a.grain_ind,3,1)='0', '-1', a.app_ver_code) as app_ver_code 
      ,a.event_name
      ,a.param_name
      ,a.param_val
      ,sum(a.val_cnt) as val_cnt
  from rptdata.fact_kesheng_event_params_hourly a 
 where a.src_file_day = '${SRC_FILE_DAY}' 
 group by if(substr(a.grain_ind,1,1)='0', '-1', a.app_channel_id)
         ,if(substr(a.grain_ind,2,1)='0', -1, a.product_key)
         ,if(substr(a.grain_ind,3,1)='0', '-1', a.app_ver_code)
         ,a.event_name
         ,a.param_name
         ,a.param_val;
         
--------------------------------------
         
with stg_cpa_event_params_daily as  
(
select a1.event_name
      ,a1.param_name
      ,a1.param_val
  from (
        select a.event_name
              ,a.param_name
              ,a.param_val
              ,row_number()over(partition by a.event_name, a.param_name order by a.val_cnt desc) param_val_rank
          from stg.cpa_event_params_daily_01 a
         where a.app_channel_id = '-1' and a.product_key= -1 and a.app_ver_code = '-1'
        ) a1
 where a1.param_val_rank <= 1000
)
insert overwrite table app.cpa_event_params_daily partition (src_file_day = '${SRC_FILE_DAY}')
select t1.product_key
      ,if(t1.product_key=-1, '-1', nvl(b1.product_name,'')) as product_name
      ,t1.app_ver_code
      ,t1.app_channel_id
      ,t1.event_name
      ,t1.param_name
      ,t1.param_val
      ,t1.val_cnt
      ,if(t1.all_val_cnt = 0, 0, round(t1.val_cnt/t1.all_val_cnt, 4)) as val_pct
  from (        
        select a.app_channel_id
              ,a.product_key
              ,a.app_ver_code
              ,a.event_name
              ,a.param_name
              ,nvl(b.param_val,'-998') as param_val
              ,sum(a.val_cnt) as val_cnt 
              ,sum(sum(a.val_cnt)) over(partition by a.app_channel_id, a.product_key, a.app_ver_code, a.event_name, a.param_name) as all_val_cnt
          from stg.cpa_event_params_daily_01 a
          left join stg_cpa_event_params_daily b    
            on a.event_name = b.event_name and a.param_name = b.param_name and a.param_val = b.param_val
         group by a.app_channel_id
                 ,a.product_key
                 ,a.app_ver_code
                 ,a.event_name
                 ,a.param_name
                 ,nvl(b.param_val,'-998')
       ) t1
  left join mscdata.dim_kesheng_sdk_product b1 on t1.product_key = b1.product_key
 where b1.product_key is not null or t1.product_key = -1
;

================================================================================================================================================


insert overwrite table stg.cpa_event_params_last7_daily_01
select if(substr(a.grain_ind,1,1)='0', '-1', a.app_channel_id) as app_channel_id 
      ,if(substr(a.grain_ind,2,1)='0', -1, a.product_key) as product_key 
      ,if(substr(a.grain_ind,3,1)='0', '-1', a.app_ver_code) as app_ver_code 
      ,a.event_name
      ,a.param_name
      ,a.param_val
      ,sum(a.val_cnt) as val_cnt
  from rptdata.fact_kesheng_event_params_hourly a 
 where a.src_file_day <= '${SRC_FILE_DAY}' 
   and a.src_file_day > from_unixtime(unix_timestamp('${SRC_FILE_DAY}','yyyyMMdd')-60*60*24*7,'yyyyMMdd')
 group by if(substr(a.grain_ind,1,1)='0', '-1', a.app_channel_id)
         ,if(substr(a.grain_ind,2,1)='0', -1, a.product_key)
         ,if(substr(a.grain_ind,3,1)='0', '-1', a.app_ver_code)
         ,a.event_name
         ,a.param_name
         ,a.param_val;

--------------------------------------

with stg_cpa_event_params_last7_daily as
(
select a1.event_name
      ,a1.param_name
      ,a1.param_val
  from (
        select a.event_name
              ,a.param_name
              ,a.param_val
              ,row_number()over(partition by a.event_name, a.param_name order by a.val_cnt desc) param_val_rank
          from stg.cpa_event_params_last7_daily_01 a
         where a.app_channel_id = '-1' and a.product_key= -1 and a.app_ver_code = '-1'
        ) a1
 where a1.param_val_rank <= 1000
)
insert overwrite table app.cpa_event_params_last7_daily partition (src_file_day = '${SRC_FILE_DAY}')
select t1.product_key
      ,if(t1.product_key=-1, '-1', nvl(b1.product_name,'')) as product_name
      ,t1.app_ver_code
      ,t1.app_channel_id
      ,t1.event_name
      ,t1.param_name
      ,t1.param_val
      ,t1.val_cnt
      ,if (t1.all_val_cnt = 0, 0 ,round(t1.val_cnt/t1.all_val_cnt, 4)) as val_pct
  from (
        select a.app_channel_id
              ,a.product_key
              ,a.app_ver_code
              ,a.event_name
              ,a.param_name
              ,nvl(b.param_val, '-998') as param_val
              ,sum(a.val_cnt) as val_cnt 
              ,sum(sum(a.val_cnt)) over(partition by a.app_channel_id, a.product_key, a.app_ver_code, a.event_name, a.param_name) as all_val_cnt
          from stg.cpa_event_params_last7_daily_01 a
          left join stg_cpa_event_params_last7_daily b
            on a.event_name = b.event_name and a.param_name = b.param_name and a.param_val = b.param_val
         group by a.app_channel_id
                 ,a.product_key
                 ,a.app_ver_code
                 ,a.event_name
                 ,a.param_name
                 ,nvl(b.param_val, '-998')	--要把 param_val='-998' 的都放在一起
       ) t1
  left join mscdata.dim_kesheng_sdk_product b1 on t1.product_key = b1.product_key
 where b1.product_key is not null or t1.product_key = -1
;

================================================================================================================================================

insert overwrite table stg.cpa_event_params_last30_daily_01
select if(substr(a.grain_ind,1,1)='0', '-1', a.app_channel_id) as app_channel_id 
      ,if(substr(a.grain_ind,2,1)='0', -1, a.product_key) as product_key 
      ,if(substr(a.grain_ind,3,1)='0', '-1', a.app_ver_code) as app_ver_code 
      ,a.event_name
      ,a.param_name
      ,a.param_val
      ,sum(a.val_cnt) as val_cnt
  from rptdata.fact_kesheng_event_params_hourly a 
 where a.src_file_day <= '${SRC_FILE_DAY}' 
   and a.src_file_day > from_unixtime(unix_timestamp('${SRC_FILE_DAY}','yyyyMMdd')-60*60*24*30,'yyyyMMdd')
 group by if(substr(a.grain_ind,1,1)='0', '-1', a.app_channel_id)
         ,if(substr(a.grain_ind,2,1)='0', -1, a.product_key)
         ,if(substr(a.grain_ind,3,1)='0', '-1', a.app_ver_code)
         ,a.event_name
         ,a.param_name
         ,a.param_val;
         
--------------------------------------

with stg_cpa_event_params_last30_daily as 
(
select a1.event_name
      ,a1.param_name
      ,a1.param_val     
  from (
        select a.event_name
              ,a.param_name
              ,a.param_val
              ,row_number()over(partition by a.event_name, a.param_name order by a.val_cnt desc) param_val_rank
          from stg.cpa_event_params_last30_daily_01 a
         where a.app_channel_id = '-1' and a.product_key= -1 and a.app_ver_code = '-1'                                                                          
        ) a1
 where a1.param_val_rank <= 1000
)
insert overwrite table app.cpa_event_params_last30_daily partition (src_file_day = '${SRC_FILE_DAY}')
select t1.product_key
      ,if(t1.product_key=-1, '-1', nvl(b1.product_name,'')) as product_name 
      ,t1.app_ver_code
      ,t1.app_channel_id
      ,t1.event_name
      ,t1.param_name
      ,t1.param_val
      ,t1.val_cnt
      ,if (t1.all_val_cnt = 0, 0 ,round(t1.val_cnt/t1.all_val_cnt, 4)) as val_pct 
  from (
        select a.app_channel_id
              ,a.product_key
              ,a.app_ver_code
              ,a.event_name
              ,a.param_name
              ,nvl(b.param_val, '-998') as param_val
              ,sum(a.val_cnt) as val_cnt 
              ,sum(sum(a.val_cnt)) over(partition by a.app_channel_id, a.product_key, a.app_ver_code, a.event_name, a.param_name) as all_val_cnt
          from stg.cpa_event_params_last30_daily_01 a 
          left join stg_cpa_event_params_last30_daily b
            on a.event_name = b.event_name and a.param_name = b.param_name and a.param_val = b.param_val
         group by a.app_channel_id
                 ,a.product_key
                 ,a.app_ver_code
                 ,a.event_name
                 ,a.param_name
                 ,nvl(b.param_val, '-998')     
        ) t1
  left join mscdata.dim_kesheng_sdk_product b1 on t1.product_key = b1.product_key 
 where b1.product_key is not null or t1.product_key = -1
; 


================================================================================================================================================
