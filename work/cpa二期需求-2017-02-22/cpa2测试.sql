external table ods.kesheng_sec_json_ex

------------------------------------------------------------
insert overwrite table odsdata.kesheng_sec_event_json partition(src_file_day='20170315', src_file_hour='13')
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
as imei, udid, idfa, idfv, installationID, clientId, appVersion, apppkg, appchannel, os, networkType, account
where e1.src_file_day = 20170315 and e1.src_file_hour = 13
and nvl(j2.sdkSessionInfo_json,'{}') <> '{}';

{"customEvent": [{"eventName": "event01","du": "8567856785876","timestamp": "1480521763049","eventParams": {"ContentID": "yixiuge1","account": "13356976635","networkType": "4G","result": "0","type": "12","loadTime": 1}},{"eventName": "event02","du": "523452344","timestamp": "1480521763049","eventParams": {"ContentID": "yixiuge","account": "13856576635","networkType": "WIFI","result": "0","type": "5","loadTime": 2432}}],
"sdkSessionInfo": {"imei": "123455768978968","udid": "ffffffff-edcd-a967-0000-0000000000001484875370454","idfa": "","idfv": "06759882-FAE1-4234-AA24-5185265BF7F0","installationID": "ffffffff-edcd-a967-0000-0000000000001484875370454","clientId": "1236","appVersion": "2.1.3demo","apppkg": "com.cmcc.migutvtwo","appchannel": "baidu","os": "AD","networkType": "WIFI","account": ""}}

-------------------------------------------------------------------------------------
CREATE VIEW `ods.kesheng_sec_event_param_json_v` AS select `e1`.`rowkey`
,`e1`.`imei`
,`e1`.`udid`
,`e1`.`idfa`
,`e1`.`idfv`
,`e1`.`installationid` `install_id`
,`e1`.`clientid` `client_id`
,`e1`.`appversion` `app_ver_code`
,`e1`.`apppkg` `app_pkg_name`
,`e1`.`appchannel` `app_channel_id`
,`e1`.`os` `app_os_type`
,`e1`.`networktype` `network_type`
,`e1`.`account`
,`j2`.`eventname` `event_name`
,`j2`.`du`
,`j2`.`timestamp`
,`j2`.`eventparams_json`
,`e1`.`src_file_day`
,`e1`.`src_file_hour`
from `odsdata`.`kesheng_sec_event_json` `e1`
lateral view explode(split(regexp_replace(`e1`.`customevent_json`,'\\}\\,\\{', '\\}\\|\\|\\{'), '\\|\\|')) `j1`
as `event_json`
lateral view json_tuple(`j1`.`event_json`, 'eventName', 'du', 'timestamp', 'eventParams') `j2`
as `eventName`, `du`, `timestamp`, `eventParams_json`

-------------------------------------------------------------------------------------------

insert overwrite table intdata.kesheng_sec_event_occur partition(src_file_day=20170315, src_file_hour=13)
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
where t1.src_file_day =20170315
and t1.src_file_hour = 13
and nvl(t1.event_name, '') <> '';

+-----------------------------+----------------+---------------------+----------------+-----------------+---------------+--+
|       t.app_pkg_name        | t.app_os_type  | t.product_ver_name  | t.product_key  | t.product_name  | t.create_day  |
+-----------------------------+----------------+---------------------+----------------+-----------------+---------------+--+
| com.cmcc.migutvtwo          | AD             | 咪咕直播安卓版             | 10             | 咪咕直播            | 20161228      |
| com.cmcc.cmvideo            | AD             | 咪咕视频安卓版             | 20             | 咪咕视频            | 20161228      |
| com.cmvideo.migumovie       | AD             | 咪咕影院安卓版             | 30             | 咪咕影院            | 20161228      |
| com.cmcc.migupad            | AD             | 咪咕视频安卓pad版          | 20             | 咪咕视频            | 20161228      |
| cn.cmvideo.migutv           | iOS            | 咪咕直播IOS             | 10             | 咪咕直播            | 20161228      |
| com.wondertek.hecmccmobile  | iOS            | 咪咕视频IOS             | 20             | 咪咕视频            | 20161228      |
| com.cmvideo.migumovie       | iOS            | 咪咕影院IOS             | 30             | 咪咕影院            | 20161228      |
+-----------------------------+----------------+---------------------+----------------+-----------------+---------------+--+

select * from intdata.kesheng_sec_event_occur t;


-----------------------------------------------------------------

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
where a.src_file_day = 20170315
and a.src_file_hour = 13
and nvl(a.event_name, '') <> ''
)
insert overwrite table intdata.kesheng_sec_event_params partition(src_file_day=20170315, src_file_hour=13)
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
from stg_kesheng_sec_event_param_json t1
lateral view posexplode(split(regexp_replace(regexp_replace(t1.eventParams_json, '\\{|\\}', ''), '"', ''), '\\s*,\\s*')) p1
as param_pos, param_key_val
) t3;


-----------------------------------------------------------------------------------------

insert overwrite table rptdata.fact_kesheng_sec_event_occur_hourly partition(src_file_day=20170315, src_file_hour=13)
select t1.app_channel_id
,t1.product_key
,t1.app_ver_code
,t1.event_name
,count(1) event_cnt
,sum(t1.du) sum_du
,rpad(reverse(bin(cast(grouping__id as int))),4,'0') grain_ind
from intdata.kesheng_sec_event_occur t1
where t1.src_file_day =20170315
and t1.src_file_hour = 13
group by app_channel_id, product_key, app_ver_code, event_name
grouping sets (event_name, (app_channel_id, event_name), (product_key, event_name),
(app_channel_id, product_key, event_name),
(product_key, app_ver_code, event_name),
(app_channel_id, product_key, app_ver_code, event_name));

select * from rptdata.fact_kesheng_sec_event_occur_hourly t;
+----------------+-----------------+-----------------+-------------------+---------------+--------------+----------------+------------------+-----------------+--+
| t.product_key  | t.product_name  | t.app_ver_code  | t.app_channel_id  | t.event_name  | t.event_cnt  |    t.sum_du    |     t.avg_du     | t.src_file_day  |
+----------------+-----------------+-----------------+-------------------+---------------+--------------+----------------+------------------+-----------------+--+
| -1             | -1              | -1              | -1                | event01       | 1            | 8567856785876  | 8567856785876    | 20170315        |
| -1             | -1              | -1              | -1                | event02       | 18           | 3241322114182  | 180073450787.89  | 20170315        |
| -1             | -1              | -1              | -1                | event12       | 1            | 654654         | 654654           | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event01       | 1            | 8567856785876  | 8567856785876    | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event01       | 1            | 8567856785876  | 8567856785876    | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event02       | 17           | 8898689858     | 523452344.59     | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event02       | 17           | 8898689858     | 523452344.59     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event01       | 1            | 8567856785876  | 8567856785876    | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event01       | 1            | 8567856785876  | 8567856785876    | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event02       | 17           | 8898689858     | 523452344.59     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event02       | 17           | 8898689858     | 523452344.59     | 20170315        |
| -1             | -1              | -1              | baidu2            | event01       | 1            | 8567856785876  | 8567856785876    | 20170315        |
| -1             | -1              | -1              | baidu2            | event02       | 18           | 3241322114182  | 180073450787.89  | 20170315        |
| -1             | -1              | -1              | baidu2            | event12       | 1            | 654654         | 654654           | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event01       | 1            | 8567856785876  | 8567856785876    | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event01       | 1            | 8567856785876  | 8567856785876    | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event02       | 17           | 8898689858     | 523452344.59     | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event02       | 17           | 8898689858     | 523452344.59     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event01       | 1            | 8567856785876  | 8567856785876    | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event01       | 1            | 8567856785876  | 8567856785876    | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event02       | 17           | 8898689858     | 523452344.59     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event02       | 17           | 8898689858     | 523452344.59     | 20170315        |
+----------------+-----------------+-----------------+-------------------+---------------+--------------+----------------+------------------+-----------------+--+

------------------------------------------------------------------------------------------------------------------

insert overwrite table rptdata.fact_kesheng_sec_event_params_hourly partition(src_file_day=20170315, src_file_hour=13)
select t1.app_channel_id
,t1.product_key
,t1.app_ver_code
,t1.event_name
,min(t1.param_pos) param_pos
,t1.param_name
,t1.param_val
,count(1) val_cnt
,rpad(reverse(bin(cast(grouping__id as int))),6,'0') grain_ind
from intdata.kesheng_sec_event_params t1
where t1.src_file_day = 20170315
and t1.src_file_hour = 13
group by app_channel_id, product_key, app_ver_code
,event_name, param_name, param_val
grouping sets ((event_name, param_name, param_val),
(app_channel_id, event_name, param_name, param_val),
(product_key, event_name, param_name, param_val),
(app_channel_id, product_key,event_name, param_name, param_val),
(product_key, app_ver_code, event_name, param_name, param_val),
(app_channel_id, product_key, app_ver_code, event_name, param_name, param_val));

select * from rptdata.fact_kesheng_sec_event_params_hourly t;
+-------------------+----------------+-----------------+---------------+--------------+---------------+--------------+------------+--------------+-----------------+------------------+--+
| t.app_channel_id  | t.product_key  | t.app_ver_code  | t.event_name  | t.param_pos  | t.param_name  | t.param_val  | t.val_cnt  | t.grain_ind  | t.src_file_day  | t.src_file_hour  |
+-------------------+----------------+-----------------+---------------+--------------+---------------+--------------+------------+--------------+-----------------+------------------+--+
| NULL              | NULL           | NULL            | event01       | 0            | ContentID     | yixiuge1     | 1          | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event01       | 1            | account       | 13356976635  | 1          | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event01       | 5            | loadTime      | 1            | 1          | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event01       | 2            | networkType   | 4G           | 1          | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event01       | 3            | result        | 0            | 1          | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event01       | 4            | type          | 12           | 1          | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event02       | 0            | ContentID     | yixiuge      | 15         | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event02       | 0            | ContentID     | yixiuge2     | 1          | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event02       | 0            | ContentID     | 中华小当家        | 2          | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event02       | 1            | account       | 13256976635  | 1          | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event02       | 1            | account       | 13856576635  | 16         | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event02       | 3            | loadTime      | 2432         | 15         | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event02       | 5            | loadTime      | 4            | 1          | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event02       | 2            | networkType   | 3G           | 2          | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event02       | 2            | networkType   | 4G           | 1          | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event02       | 1            | networkType   | WIFI         | 15         | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event02       | 3            | result        | 0            | 16         | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event02       | 3            | result        | 2            | 1          | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event02       | 4            | type          | 10           | 1          | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event02       | 2            | type          | 5            | 16         | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event12       | 0            | ContentID     | yixiuge1     | 1          | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event12       | 1            | account       | 13856974635  | 1          | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event12       | 5            | loadTime      | 103435200    | 1          | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event12       | 2            | networkType   | 3G           | 1          | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event12       | 3            | result        | 0            | 1          | 000111       | 20170315        | 13               |
| NULL              | NULL           | NULL            | event12       | 4            | type          | 3            | 1          | 000111       | 20170315        | 13               |
| NULL              | -998           | NULL            | event02       | 0            | ContentID     | yixiuge2     | 1          | 010111       | 20170315        | 13               |
| NULL              | -998           | NULL            | event02       | 1            | account       | 13256976635  | 1          | 010111       | 20170315        | 13               |
| NULL              | -998           | NULL            | event02       | 5            | loadTime      | 4            | 1          | 010111       | 20170315        | 13               |
| NULL              | -998           | NULL            | event02       | 2            | networkType   | 3G           | 1          | 010111       | 20170315        | 13               |
| NULL              | -998           | NULL            | event02       | 3            | result        | 2            | 1          | 010111       | 20170315        | 13               |
| NULL              | -998           | NULL            | event02       | 4            | type          | 10           | 1          | 010111       | 20170315        | 13               |
| NULL              | -998           | NULL            | event12       | 0            | ContentID     | yixiuge1     | 1          | 010111       | 20170315        | 13               |
| NULL              | -998           | NULL            | event12       | 1            | account       | 13856974635  | 1          | 010111       | 20170315        | 13               |
| NULL              | -998           | NULL            | event12       | 5            | loadTime      | 103435200    | 1          | 010111       | 20170315        | 13               |
| NULL              | -998           | NULL            | event12       | 2            | networkType   | 3G           | 1          | 010111       | 20170315        | 13               |
| NULL              | -998           | NULL            | event12       | 3            | result        | 0            | 1          | 010111       | 20170315        | 13               |
| NULL              | -998           | NULL            | event12       | 4            | type          | 3            | 1          | 010111       | 20170315        | 13               |
| NULL              | -998           | 2.1.3demo2      | event02       | 0            | ContentID     | yixiuge2     | 1          | 011111       | 20170315        | 13               |
| NULL              | -998           | 2.1.3demo2      | event02       | 1            | account       | 13256976635  | 1          | 011111       | 20170315        | 13               |
| NULL              | -998           | 2.1.3demo2      | event02       | 5            | loadTime      | 4            | 1          | 011111       | 20170315        | 13               |
| NULL              | -998           | 2.1.3demo2      | event02       | 2            | networkType   | 3G           | 1          | 011111       | 20170315        | 13               |
| NULL              | -998           | 2.1.3demo2      | event02       | 3            | result        | 2            | 1          | 011111       | 20170315        | 13               |
| NULL              | -998           | 2.1.3demo2      | event02       | 4            | type          | 10           | 1          | 011111       | 20170315        | 13               |
| NULL              | -998           | 2.1.3demo2      | event12       | 0            | ContentID     | yixiuge1     | 1          | 011111       | 20170315        | 13               |
| NULL              | -998           | 2.1.3demo2      | event12       | 1            | account       | 13856974635  | 1          | 011111       | 20170315        | 13               |
| NULL              | -998           | 2.1.3demo2      | event12       | 5            | loadTime      | 103435200    | 1          | 011111       | 20170315        | 13               |
| NULL              | -998           | 2.1.3demo2      | event12       | 2            | networkType   | 3G           | 1          | 011111       | 20170315        | 13               |
| NULL              | -998           | 2.1.3demo2      | event12       | 3            | result        | 0            | 1          | 011111       | 20170315        | 13               |
| NULL              | -998           | 2.1.3demo2      | event12       | 4            | type          | 3            | 1          | 011111       | 20170315        | 13               |
| NULL              | 10             | NULL            | event01       | 0            | ContentID     | yixiuge1     | 1          | 010111       | 20170315        | 13               |
| NULL              | 10             | NULL            | event01       | 1            | account       | 13356976635  | 1          | 010111       | 20170315        | 13               |
| NULL              | 10             | NULL            | event01       | 5            | loadTime      | 1            | 1          | 010111       | 20170315        | 13               |
| NULL              | 10             | NULL            | event01       | 2            | networkType   | 4G           | 1          | 010111       | 20170315        | 13               |
| NULL              | 10             | NULL            | event01       | 3            | result        | 0            | 1          | 010111       | 20170315        | 13               |
| NULL              | 10             | NULL            | event01       | 4            | type          | 12           | 1          | 010111       | 20170315        | 13               |
| NULL              | 10             | NULL            | event02       | 0            | ContentID     | yixiuge      | 15         | 010111       | 20170315        | 13               |
| NULL              | 10             | NULL            | event02       | 0            | ContentID     | 中华小当家        | 2          | 010111       | 20170315        | 13               |
| NULL              | 10             | NULL            | event02       | 1            | account       | 13856576635  | 16         | 010111       | 20170315        | 13               |
| NULL              | 10             | NULL            | event02       | 3            | loadTime      | 2432         | 15         | 010111       | 20170315        | 13               |
| NULL              | 10             | NULL            | event02       | 2            | networkType   | 3G           | 1          | 010111       | 20170315        | 13               |
| NULL              | 10             | NULL            | event02       | 2            | networkType   | 4G           | 1          | 010111       | 20170315        | 13               |
| NULL              | 10             | NULL            | event02       | 1            | networkType   | WIFI         | 15         | 010111       | 20170315        | 13               |
| NULL              | 10             | NULL            | event02       | 3            | result        | 0            | 16         | 010111       | 20170315        | 13               |
| NULL              | 10             | NULL            | event02       | 2            | type          | 5            | 16         | 010111       | 20170315        | 13               |
| NULL              | 10             | 2.1.3demo1      | event01       | 0            | ContentID     | yixiuge1     | 1          | 011111       | 20170315        | 13               |
| NULL              | 10             | 2.1.3demo1      | event01       | 1            | account       | 13356976635  | 1          | 011111       | 20170315        | 13               |
| NULL              | 10             | 2.1.3demo1      | event01       | 5            | loadTime      | 1            | 1          | 011111       | 20170315        | 13               |
| NULL              | 10             | 2.1.3demo1      | event01       | 2            | networkType   | 4G           | 1          | 011111       | 20170315        | 13               |
| NULL              | 10             | 2.1.3demo1      | event01       | 3            | result        | 0            | 1          | 011111       | 20170315        | 13               |
| NULL              | 10             | 2.1.3demo1      | event01       | 4            | type          | 12           | 1          | 011111       | 20170315        | 13               |
| NULL              | 10             | 2.1.3demo1      | event02       | 0            | ContentID     | yixiuge      | 15         | 011111       | 20170315        | 13               |
| NULL              | 10             | 2.1.3demo1      | event02       | 0            | ContentID     | 中华小当家        | 2          | 011111       | 20170315        | 13               |
| NULL              | 10             | 2.1.3demo1      | event02       | 1            | account       | 13856576635  | 16         | 011111       | 20170315        | 13               |
| NULL              | 10             | 2.1.3demo1      | event02       | 3            | loadTime      | 2432         | 15         | 011111       | 20170315        | 13               |
| NULL              | 10             | 2.1.3demo1      | event02       | 2            | networkType   | 3G           | 1          | 011111       | 20170315        | 13               |
| NULL              | 10             | 2.1.3demo1      | event02       | 2            | networkType   | 4G           | 1          | 011111       | 20170315        | 13               |
| NULL              | 10             | 2.1.3demo1      | event02       | 1            | networkType   | WIFI         | 15         | 011111       | 20170315        | 13               |
| NULL              | 10             | 2.1.3demo1      | event02       | 3            | result        | 0            | 16         | 011111       | 20170315        | 13               |
| NULL              | 10             | 2.1.3demo1      | event02       | 2            | type          | 5            | 16         | 011111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event01       | 0            | ContentID     | yixiuge1     | 1          | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event01       | 1            | account       | 13356976635  | 1          | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event01       | 5            | loadTime      | 1            | 1          | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event01       | 2            | networkType   | 4G           | 1          | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event01       | 3            | result        | 0            | 1          | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event01       | 4            | type          | 12           | 1          | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event02       | 0            | ContentID     | yixiuge      | 15         | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event02       | 0            | ContentID     | yixiuge2     | 1          | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event02       | 0            | ContentID     | 中华小当家        | 2          | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event02       | 1            | account       | 13256976635  | 1          | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event02       | 1            | account       | 13856576635  | 16         | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event02       | 3            | loadTime      | 2432         | 15         | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event02       | 5            | loadTime      | 4            | 1          | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event02       | 2            | networkType   | 3G           | 2          | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event02       | 2            | networkType   | 4G           | 1          | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event02       | 1            | networkType   | WIFI         | 15         | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event02       | 3            | result        | 0            | 16         | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event02       | 3            | result        | 2            | 1          | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event02       | 4            | type          | 10           | 1          | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event02       | 2            | type          | 5            | 16         | 100111       | 20170315        | 13               |
+-------------------+----------------+-----------------+---------------+--------------+---------------+--------------+------------+--------------+-----------------+------------------+--+
| t.app_channel_id  | t.product_key  | t.app_ver_code  | t.event_name  | t.param_pos  | t.param_name  | t.param_val  | t.val_cnt  | t.grain_ind  | t.src_file_day  | t.src_file_hour  |
+-------------------+----------------+-----------------+---------------+--------------+---------------+--------------+------------+--------------+-----------------+------------------+--+
| baidu2            | NULL           | NULL            | event12       | 0            | ContentID     | yixiuge1     | 1          | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event12       | 1            | account       | 13856974635  | 1          | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event12       | 5            | loadTime      | 103435200    | 1          | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event12       | 2            | networkType   | 3G           | 1          | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event12       | 3            | result        | 0            | 1          | 100111       | 20170315        | 13               |
| baidu2            | NULL           | NULL            | event12       | 4            | type          | 3            | 1          | 100111       | 20170315        | 13               |
| baidu2            | -998           | NULL            | event02       | 0            | ContentID     | yixiuge2     | 1          | 110111       | 20170315        | 13               |
| baidu2            | -998           | NULL            | event02       | 1            | account       | 13256976635  | 1          | 110111       | 20170315        | 13               |
| baidu2            | -998           | NULL            | event02       | 5            | loadTime      | 4            | 1          | 110111       | 20170315        | 13               |
| baidu2            | -998           | NULL            | event02       | 2            | networkType   | 3G           | 1          | 110111       | 20170315        | 13               |
| baidu2            | -998           | NULL            | event02       | 3            | result        | 2            | 1          | 110111       | 20170315        | 13               |
| baidu2            | -998           | NULL            | event02       | 4            | type          | 10           | 1          | 110111       | 20170315        | 13               |
| baidu2            | -998           | NULL            | event12       | 0            | ContentID     | yixiuge1     | 1          | 110111       | 20170315        | 13               |
| baidu2            | -998           | NULL            | event12       | 1            | account       | 13856974635  | 1          | 110111       | 20170315        | 13               |
| baidu2            | -998           | NULL            | event12       | 5            | loadTime      | 103435200    | 1          | 110111       | 20170315        | 13               |
| baidu2            | -998           | NULL            | event12       | 2            | networkType   | 3G           | 1          | 110111       | 20170315        | 13               |
| baidu2            | -998           | NULL            | event12       | 3            | result        | 0            | 1          | 110111       | 20170315        | 13               |
| baidu2            | -998           | NULL            | event12       | 4            | type          | 3            | 1          | 110111       | 20170315        | 13               |
| baidu2            | -998           | 2.1.3demo2      | event02       | 0            | ContentID     | yixiuge2     | 1          | 111111       | 20170315        | 13               |
| baidu2            | -998           | 2.1.3demo2      | event02       | 1            | account       | 13256976635  | 1          | 111111       | 20170315        | 13               |
| baidu2            | -998           | 2.1.3demo2      | event02       | 5            | loadTime      | 4            | 1          | 111111       | 20170315        | 13               |
| baidu2            | -998           | 2.1.3demo2      | event02       | 2            | networkType   | 3G           | 1          | 111111       | 20170315        | 13               |
| baidu2            | -998           | 2.1.3demo2      | event02       | 3            | result        | 2            | 1          | 111111       | 20170315        | 13               |
| baidu2            | -998           | 2.1.3demo2      | event02       | 4            | type          | 10           | 1          | 111111       | 20170315        | 13               |
| baidu2            | -998           | 2.1.3demo2      | event12       | 0            | ContentID     | yixiuge1     | 1          | 111111       | 20170315        | 13               |
| baidu2            | -998           | 2.1.3demo2      | event12       | 1            | account       | 13856974635  | 1          | 111111       | 20170315        | 13               |
| baidu2            | -998           | 2.1.3demo2      | event12       | 5            | loadTime      | 103435200    | 1          | 111111       | 20170315        | 13               |
| baidu2            | -998           | 2.1.3demo2      | event12       | 2            | networkType   | 3G           | 1          | 111111       | 20170315        | 13               |
| baidu2            | -998           | 2.1.3demo2      | event12       | 3            | result        | 0            | 1          | 111111       | 20170315        | 13               |
| baidu2            | -998           | 2.1.3demo2      | event12       | 4            | type          | 3            | 1          | 111111       | 20170315        | 13               |
| baidu2            | 10             | NULL            | event01       | 0            | ContentID     | yixiuge1     | 1          | 110111       | 20170315        | 13               |
| baidu2            | 10             | NULL            | event01       | 1            | account       | 13356976635  | 1          | 110111       | 20170315        | 13               |
| baidu2            | 10             | NULL            | event01       | 5            | loadTime      | 1            | 1          | 110111       | 20170315        | 13               |
| baidu2            | 10             | NULL            | event01       | 2            | networkType   | 4G           | 1          | 110111       | 20170315        | 13               |
| baidu2            | 10             | NULL            | event01       | 3            | result        | 0            | 1          | 110111       | 20170315        | 13               |
| baidu2            | 10             | NULL            | event01       | 4            | type          | 12           | 1          | 110111       | 20170315        | 13               |
| baidu2            | 10             | NULL            | event02       | 0            | ContentID     | yixiuge      | 15         | 110111       | 20170315        | 13               |
| baidu2            | 10             | NULL            | event02       | 0            | ContentID     | 中华小当家        | 2          | 110111       | 20170315        | 13               |
| baidu2            | 10             | NULL            | event02       | 1            | account       | 13856576635  | 16         | 110111       | 20170315        | 13               |
| baidu2            | 10             | NULL            | event02       | 3            | loadTime      | 2432         | 15         | 110111       | 20170315        | 13               |
| baidu2            | 10             | NULL            | event02       | 2            | networkType   | 3G           | 1          | 110111       | 20170315        | 13               |
| baidu2            | 10             | NULL            | event02       | 2            | networkType   | 4G           | 1          | 110111       | 20170315        | 13               |
| baidu2            | 10             | NULL            | event02       | 1            | networkType   | WIFI         | 15         | 110111       | 20170315        | 13               |
| baidu2            | 10             | NULL            | event02       | 3            | result        | 0            | 16         | 110111       | 20170315        | 13               |
| baidu2            | 10             | NULL            | event02       | 2            | type          | 5            | 16         | 110111       | 20170315        | 13               |
| baidu2            | 10             | 2.1.3demo1      | event01       | 0            | ContentID     | yixiuge1     | 1          | 111111       | 20170315        | 13               |
| baidu2            | 10             | 2.1.3demo1      | event01       | 1            | account       | 13356976635  | 1          | 111111       | 20170315        | 13               |
| baidu2            | 10             | 2.1.3demo1      | event01       | 5            | loadTime      | 1            | 1          | 111111       | 20170315        | 13               |
| baidu2            | 10             | 2.1.3demo1      | event01       | 2            | networkType   | 4G           | 1          | 111111       | 20170315        | 13               |
| baidu2            | 10             | 2.1.3demo1      | event01       | 3            | result        | 0            | 1          | 111111       | 20170315        | 13               |
| baidu2            | 10             | 2.1.3demo1      | event01       | 4            | type          | 12           | 1          | 111111       | 20170315        | 13               |
| baidu2            | 10             | 2.1.3demo1      | event02       | 0            | ContentID     | yixiuge      | 15         | 111111       | 20170315        | 13               |
| baidu2            | 10             | 2.1.3demo1      | event02       | 0            | ContentID     | 中华小当家        | 2          | 111111       | 20170315        | 13               |
| baidu2            | 10             | 2.1.3demo1      | event02       | 1            | account       | 13856576635  | 16         | 111111       | 20170315        | 13               |
| baidu2            | 10             | 2.1.3demo1      | event02       | 3            | loadTime      | 2432         | 15         | 111111       | 20170315        | 13               |
| baidu2            | 10             | 2.1.3demo1      | event02       | 2            | networkType   | 3G           | 1          | 111111       | 20170315        | 13               |
| baidu2            | 10             | 2.1.3demo1      | event02       | 2            | networkType   | 4G           | 1          | 111111       | 20170315        | 13               |
| baidu2            | 10             | 2.1.3demo1      | event02       | 1            | networkType   | WIFI         | 15         | 111111       | 20170315        | 13               |
| baidu2            | 10             | 2.1.3demo1      | event02       | 3            | result        | 0            | 16         | 111111       | 20170315        | 13               |
| baidu2            | 10             | 2.1.3demo1      | event02       | 2            | type          | 5            | 16         | 111111       | 20170315        | 13               |
+-------------------+----------------+-----------------+---------------+--------------+---------------+--------------+------------+--------------+-----------------+------------------+--+
----------------------------------------------------------------------------------------

insert overwrite table app.cpa_sec_event_occur_daily partition (src_file_day =20170315)
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
from rptdata.fact_kesheng_sec_event_occur_hourly a
where a.src_file_day = 20170315
group by if(substr(a.grain_ind,1,1)= '0', '-1', a.app_channel_id)
,if(substr(a.grain_ind,2,1)= '0', -1, a.product_key)
,if(substr(a.grain_ind,3,1)= '0', '-1', a.app_ver_code)
,a.event_name
)t1
left join mscdata.dim_kesheng_sdk_app_pkg b1 on t1.product_key = b1.product_key
where b1.product_key is not null or t1.product_key = -1;

select * from app.cpa_sec_event_occur_daily t;
+----------------+-----------------+-----------------+-------------------+---------------+--------------+----------------+------------------+-----------------+--+
| t.product_key  | t.product_name  | t.app_ver_code  | t.app_channel_id  | t.event_name  | t.event_cnt  |    t.sum_du    |     t.avg_du     | t.src_file_day  |
+----------------+-----------------+-----------------+-------------------+---------------+--------------+----------------+------------------+-----------------+--+
| -1             | -1              | -1              | -1                | event01       | 1            | 8567856785876  | 8567856785876    | 20170315        |
| -1             | -1              | -1              | -1                | event02       | 18           | 3241322114182  | 180073450787.89  | 20170315        |
| -1             | -1              | -1              | -1                | event12       | 1            | 654654         | 654654           | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event01       | 1            | 8567856785876  | 8567856785876    | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event01       | 1            | 8567856785876  | 8567856785876    | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event02       | 17           | 8898689858     | 523452344.59     | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event02       | 17           | 8898689858     | 523452344.59     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event01       | 1            | 8567856785876  | 8567856785876    | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event01       | 1            | 8567856785876  | 8567856785876    | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event02       | 17           | 8898689858     | 523452344.59     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event02       | 17           | 8898689858     | 523452344.59     | 20170315        |
| -1             | -1              | -1              | baidu2            | event01       | 1            | 8567856785876  | 8567856785876    | 20170315        |
| -1             | -1              | -1              | baidu2            | event02       | 18           | 3241322114182  | 180073450787.89  | 20170315        |
| -1             | -1              | -1              | baidu2            | event12       | 1            | 654654         | 654654           | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event01       | 1            | 8567856785876  | 8567856785876    | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event01       | 1            | 8567856785876  | 8567856785876    | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event02       | 17           | 8898689858     | 523452344.59     | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event02       | 17           | 8898689858     | 523452344.59     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event01       | 1            | 8567856785876  | 8567856785876    | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event01       | 1            | 8567856785876  | 8567856785876    | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event02       | 17           | 8898689858     | 523452344.59     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event02       | 17           | 8898689858     | 523452344.59     | 20170315        |
+----------------+-----------------+-----------------+-------------------+---------------+--------------+----------------+------------------+-----------------+--+
-------------------------------------------------------------------

with stg_cpa_sec_event_params_daily as
(select a1.event_name
,a1.param_name
,a1.param_val
from (
select a.event_name
,a.param_name
,a.param_val
,sum(a.val_cnt)
,row_number()over(partition by a.event_name, a.param_name, a.param_val order by sum(a.val_cnt) desc) param_val_rank
from rptdata.fact_kesheng_sec_event_params_hourly a
where a.src_file_day = 20170315
and a.grain_ind = '000111'
group by a.event_name
,a.param_name
,a.param_val
) a1
where a1.param_val_rank <= 1000
)
insert overwrite table app.cpa_sec_event_params_daily partition (src_file_day = 20170315)
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
select t0.app_channel_id
,t0.product_key
,t0.app_ver_code
,t0.event_name
,t0.param_name
,nvl(t0.param_val, '-998') as param_val
,t0.val_cnt
,sum(t0.val_cnt) over(partition by t0.app_channel_id, t0.product_key, t0.app_ver_code, t0.event_name, t0.param_name) as all_val_cnt
from (
select if(substr(a.grain_ind,1,1)='0', '-1', a.app_channel_id) as app_channel_id
,if(substr(a.grain_ind,2,1)='0', -1, a.product_key) as product_key
,if(substr(a.grain_ind,3,1)='0', '-1', a.app_ver_code) as app_ver_code
,a.event_name
,a.param_name
,b.param_val
,sum(a.val_cnt) as val_cnt
from rptdata.fact_kesheng_sec_event_params_hourly a
left join stg_cpa_sec_event_params_daily b
on a.event_name = b.event_name and a.param_name = b.param_name and a.param_val = b.param_val
where a.src_file_day = 20170315
group by if(substr(a.grain_ind,1,1)='0', '-1', a.app_channel_id)
,if(substr(a.grain_ind,2,1)='0', -1, a.product_key)
,if(substr(a.grain_ind,3,1)='0', '-1', a.app_ver_code)
,a.event_name
,a.param_name
,b.param_val
)t0
) t1
left join mscdata.dim_kesheng_sdk_app_pkg b1 on t1.product_key = b1.product_key
where b1.product_key is not null or t1.product_key = -1;

select * from app.cpa_sec_event_params_daily t1;
+----------------+-----------------+-----------------+-------------------+---------------+---------------+--------------+------------+------------+-----------------+--+
| t.product_key  | t.product_name  | t.app_ver_code  | t.app_channel_id  | t.event_name  | t.param_name  | t.param_val  | t.val_cnt  | t.val_pct  | t.src_file_day  |
+----------------+-----------------+-----------------+-------------------+---------------+---------------+--------------+------------+------------+-----------------+--+
| -1             | -1              | -1              | -1                | event01       | ContentID     | yixiuge1     | 1          | 1          | 20170315        |
| -1             | -1              | -1              | -1                | event01       | account       | 13356976635  | 1          | 1          | 20170315        |
| -1             | -1              | -1              | -1                | event01       | loadTime      | 1            | 1          | 1          | 20170315        |
| -1             | -1              | -1              | -1                | event01       | networkType   | 4G           | 1          | 1          | 20170315        |
| -1             | -1              | -1              | -1                | event01       | result        | 0            | 1          | 1          | 20170315        |
| -1             | -1              | -1              | -1                | event01       | type          | 12           | 1          | 1          | 20170315        |
| -1             | -1              | -1              | -1                | event02       | ContentID     | yixiuge      | 15         | 0.8333     | 20170315        |
| -1             | -1              | -1              | -1                | event02       | ContentID     | yixiuge2     | 1          | 0.0556     | 20170315        |
| -1             | -1              | -1              | -1                | event02       | ContentID     | 中华小当家        | 2          | 0.1111     | 20170315        |
| -1             | -1              | -1              | -1                | event02       | account       | 13256976635  | 1          | 0.0588     | 20170315        |
| -1             | -1              | -1              | -1                | event02       | account       | 13856576635  | 16         | 0.9412     | 20170315        |
| -1             | -1              | -1              | -1                | event02       | loadTime      | 2432         | 15         | 0.9375     | 20170315        |
| -1             | -1              | -1              | -1                | event02       | loadTime      | 4            | 1          | 0.0625     | 20170315        |
| -1             | -1              | -1              | -1                | event02       | networkType   | WIFI         | 15         | 0.8333     | 20170315        |
| -1             | -1              | -1              | -1                | event02       | networkType   | 3G           | 2          | 0.1111     | 20170315        |
| -1             | -1              | -1              | -1                | event02       | networkType   | 4G           | 1          | 0.0556     | 20170315        |
| -1             | -1              | -1              | -1                | event02       | result        | 0            | 16         | 0.9412     | 20170315        |
| -1             | -1              | -1              | -1                | event02       | result        | 2            | 1          | 0.0588     | 20170315        |
| -1             | -1              | -1              | -1                | event02       | type          | 10           | 1          | 0.0588     | 20170315        |
| -1             | -1              | -1              | -1                | event02       | type          | 5            | 16         | 0.9412     | 20170315        |
| -1             | -1              | -1              | -1                | event12       | ContentID     | yixiuge1     | 1          | 1          | 20170315        |
| -1             | -1              | -1              | -1                | event12       | account       | 13856974635  | 1          | 1          | 20170315        |
| -1             | -1              | -1              | -1                | event12       | loadTime      | 103435200    | 1          | 1          | 20170315        |
| -1             | -1              | -1              | -1                | event12       | networkType   | 3G           | 1          | 1          | 20170315        |
| -1             | -1              | -1              | -1                | event12       | result        | 0            | 1          | 1          | 20170315        |
| -1             | -1              | -1              | -1                | event12       | type          | 3            | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event01       | ContentID     | yixiuge1     | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event01       | ContentID     | yixiuge1     | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event01       | account       | 13356976635  | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event01       | account       | 13356976635  | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event01       | loadTime      | 1            | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event01       | loadTime      | 1            | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event01       | networkType   | 4G           | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event01       | networkType   | 4G           | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event01       | result        | 0            | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event01       | result        | 0            | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event01       | type          | 12           | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event01       | type          | 12           | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event02       | ContentID     | yixiuge      | 15         | 0.8824     | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event02       | ContentID     | yixiuge      | 15         | 0.8824     | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event02       | ContentID     | 中华小当家        | 2          | 0.1176     | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event02       | ContentID     | 中华小当家        | 2          | 0.1176     | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event02       | account       | 13856576635  | 16         | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event02       | account       | 13856576635  | 16         | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event02       | loadTime      | 2432         | 15         | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event02       | loadTime      | 2432         | 15         | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event02       | networkType   | 3G           | 1          | 0.0588     | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event02       | networkType   | 3G           | 1          | 0.0588     | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event02       | networkType   | 4G           | 1          | 0.0588     | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event02       | networkType   | 4G           | 1          | 0.0588     | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event02       | networkType   | WIFI         | 15         | 0.8824     | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event02       | networkType   | WIFI         | 15         | 0.8824     | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event02       | result        | 0            | 16         | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event02       | result        | 0            | 16         | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event02       | type          | 5            | 16         | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | -1                | event02       | type          | 5            | 16         | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event01       | ContentID     | yixiuge1     | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event01       | ContentID     | yixiuge1     | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event01       | account       | 13356976635  | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event01       | account       | 13356976635  | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event01       | loadTime      | 1            | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event01       | loadTime      | 1            | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event01       | networkType   | 4G           | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event01       | networkType   | 4G           | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event01       | result        | 0            | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event01       | result        | 0            | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event01       | type          | 12           | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event01       | type          | 12           | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event02       | ContentID     | yixiuge      | 15         | 0.8824     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event02       | ContentID     | yixiuge      | 15         | 0.8824     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event02       | ContentID     | 中华小当家        | 2          | 0.1176     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event02       | ContentID     | 中华小当家        | 2          | 0.1176     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event02       | account       | 13856576635  | 16         | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event02       | account       | 13856576635  | 16         | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event02       | loadTime      | 2432         | 15         | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event02       | loadTime      | 2432         | 15         | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event02       | networkType   | 3G           | 1          | 0.0588     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event02       | networkType   | 3G           | 1          | 0.0588     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event02       | networkType   | 4G           | 1          | 0.0588     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event02       | networkType   | 4G           | 1          | 0.0588     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event02       | networkType   | WIFI         | 15         | 0.8824     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event02       | networkType   | WIFI         | 15         | 0.8824     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event02       | result        | 0            | 16         | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event02       | result        | 0            | 16         | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event02       | type          | 5            | 16         | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | -1                | event02       | type          | 5            | 16         | 1          | 20170315        |
| -1             | -1              | -1              | baidu2            | event01       | ContentID     | yixiuge1     | 1          | 1          | 20170315        |
| -1             | -1              | -1              | baidu2            | event01       | account       | 13356976635  | 1          | 1          | 20170315        |
| -1             | -1              | -1              | baidu2            | event01       | loadTime      | 1            | 1          | 1          | 20170315        |
| -1             | -1              | -1              | baidu2            | event01       | networkType   | 4G           | 1          | 1          | 20170315        |
| -1             | -1              | -1              | baidu2            | event01       | result        | 0            | 1          | 1          | 20170315        |
| -1             | -1              | -1              | baidu2            | event01       | type          | 12           | 1          | 1          | 20170315        |
| -1             | -1              | -1              | baidu2            | event02       | ContentID     | yixiuge      | 15         | 0.8333     | 20170315        |
| -1             | -1              | -1              | baidu2            | event02       | ContentID     | yixiuge2     | 1          | 0.0556     | 20170315        |
| -1             | -1              | -1              | baidu2            | event02       | ContentID     | 中华小当家        | 2          | 0.1111     | 20170315        |
| -1             | -1              | -1              | baidu2            | event02       | account       | 13256976635  | 1          | 0.0588     | 20170315        |
| -1             | -1              | -1              | baidu2            | event02       | account       | 13856576635  | 16         | 0.9412     | 20170315        |
| -1             | -1              | -1              | baidu2            | event02       | loadTime      | 2432         | 15         | 0.9375     | 20170315        |
| -1             | -1              | -1              | baidu2            | event02       | loadTime      | 4            | 1          | 0.0625     | 20170315        |
| -1             | -1              | -1              | baidu2            | event02       | networkType   | 3G           | 2          | 0.1111     | 20170315        |
+----------------+-----------------+-----------------+-------------------+---------------+---------------+--------------+------------+------------+-----------------+--+
| t.product_key  | t.product_name  | t.app_ver_code  | t.app_channel_id  | t.event_name  | t.param_name  | t.param_val  | t.val_cnt  | t.val_pct  | t.src_file_day  |
+----------------+-----------------+-----------------+-------------------+---------------+---------------+--------------+------------+------------+-----------------+--+
| -1             | -1              | -1              | baidu2            | event02       | networkType   | 4G           | 1          | 0.0556     | 20170315        |
| -1             | -1              | -1              | baidu2            | event02       | networkType   | WIFI         | 15         | 0.8333     | 20170315        |
| -1             | -1              | -1              | baidu2            | event02       | result        | 0            | 16         | 0.9412     | 20170315        |
| -1             | -1              | -1              | baidu2            | event02       | result        | 2            | 1          | 0.0588     | 20170315        |
| -1             | -1              | -1              | baidu2            | event02       | type          | 5            | 16         | 0.9412     | 20170315        |
| -1             | -1              | -1              | baidu2            | event02       | type          | 10           | 1          | 0.0588     | 20170315        |
| -1             | -1              | -1              | baidu2            | event12       | ContentID     | yixiuge1     | 1          | 1          | 20170315        |
| -1             | -1              | -1              | baidu2            | event12       | account       | 13856974635  | 1          | 1          | 20170315        |
| -1             | -1              | -1              | baidu2            | event12       | loadTime      | 103435200    | 1          | 1          | 20170315        |
| -1             | -1              | -1              | baidu2            | event12       | networkType   | 3G           | 1          | 1          | 20170315        |
| -1             | -1              | -1              | baidu2            | event12       | result        | 0            | 1          | 1          | 20170315        |
| -1             | -1              | -1              | baidu2            | event12       | type          | 3            | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event01       | ContentID     | yixiuge1     | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event01       | ContentID     | yixiuge1     | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event01       | account       | 13356976635  | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event01       | account       | 13356976635  | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event01       | loadTime      | 1            | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event01       | loadTime      | 1            | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event01       | networkType   | 4G           | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event01       | networkType   | 4G           | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event01       | result        | 0            | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event01       | result        | 0            | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event01       | type          | 12           | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event01       | type          | 12           | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event02       | ContentID     | yixiuge      | 15         | 0.8824     | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event02       | ContentID     | yixiuge      | 15         | 0.8824     | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event02       | ContentID     | 中华小当家        | 2          | 0.1176     | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event02       | ContentID     | 中华小当家        | 2          | 0.1176     | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event02       | account       | 13856576635  | 16         | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event02       | account       | 13856576635  | 16         | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event02       | loadTime      | 2432         | 15         | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event02       | loadTime      | 2432         | 15         | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event02       | networkType   | 4G           | 1          | 0.0588     | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event02       | networkType   | 4G           | 1          | 0.0588     | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event02       | networkType   | 3G           | 1          | 0.0588     | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event02       | networkType   | 3G           | 1          | 0.0588     | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event02       | networkType   | WIFI         | 15         | 0.8824     | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event02       | networkType   | WIFI         | 15         | 0.8824     | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event02       | result        | 0            | 16         | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event02       | result        | 0            | 16         | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event02       | type          | 5            | 16         | 1          | 20170315        |
| 10             | 咪咕直播            | -1              | baidu2            | event02       | type          | 5            | 16         | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event01       | ContentID     | yixiuge1     | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event01       | ContentID     | yixiuge1     | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event01       | account       | 13356976635  | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event01       | account       | 13356976635  | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event01       | loadTime      | 1            | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event01       | loadTime      | 1            | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event01       | networkType   | 4G           | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event01       | networkType   | 4G           | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event01       | result        | 0            | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event01       | result        | 0            | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event01       | type          | 12           | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event01       | type          | 12           | 1          | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event02       | ContentID     | yixiuge      | 15         | 0.8824     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event02       | ContentID     | yixiuge      | 15         | 0.8824     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event02       | ContentID     | 中华小当家        | 2          | 0.1176     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event02       | ContentID     | 中华小当家        | 2          | 0.1176     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event02       | account       | 13856576635  | 16         | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event02       | account       | 13856576635  | 16         | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event02       | loadTime      | 2432         | 15         | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event02       | loadTime      | 2432         | 15         | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event02       | networkType   | 3G           | 1          | 0.0588     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event02       | networkType   | 3G           | 1          | 0.0588     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event02       | networkType   | 4G           | 1          | 0.0588     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event02       | networkType   | 4G           | 1          | 0.0588     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event02       | networkType   | WIFI         | 15         | 0.8824     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event02       | networkType   | WIFI         | 15         | 0.8824     | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event02       | result        | 0            | 16         | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event02       | result        | 0            | 16         | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event02       | type          | 5            | 16         | 1          | 20170315        |
| 10             | 咪咕直播            | 2.1.3demo1      | baidu2            | event02       | type          | 5            | 16         | 1          | 20170315        |
+----------------+-----------------+-----------------+-------------------+---------------+---------------+--------------+------------+------------+-----------------+--+

------------------------------------------------------------------

with stg_cpa_sec_event_params_last7_daily as
(
select a1.event_name
,a1.param_name
,a1.param_val
from (
select a.event_name
,a.param_name
,a.param_val
,sum(a.val_cnt)
,row_number()over(partition by a.event_name, a.param_name, a.param_val order by sum(a.val_cnt) desc) param_val_rank
from rptdata.fact_kesheng_sec_event_params_hourly a
where a.src_file_day <= 20170315
and a.src_file_day > from_unixtime(unix_timestamp('20170315','yyyyMMdd')-60*60*24*7,'yyyyMMdd')
and a.grain_ind = '000'
group by a.event_name
,a.param_name
,a.param_val
) a1
where a1.param_val_rank <= 1000
)
insert overwrite table app.cpa_sec_event_params_last7_daily partition (src_file_day = 20170315)
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
select t0.app_channel_id
,t0.product_key
,t0.app_ver_code
,t0.event_name
,t0.param_name
,nvl(t0.param_val, '-998') as param_val
,t0.val_cnt
,sum(t0.val_cnt) over(partition by t0.app_channel_id, t0.product_key, t0.app_ver_code, t0.event_name, t0.param_name) as all_val_cnt
from (
select if(substr(a.grain_ind,1,1)='0', '-1', a.app_channel_id) as app_channel_id
,if(substr(a.grain_ind,2,1)='0', -1, a.product_key) as product_key
,if(substr(a.grain_ind,3,1)='0', '-1', a.app_ver_code) as app_ver_code
,a.event_name
,a.param_name
,b.param_val
,sum(a.val_cnt) as val_cnt
from rptdata.fact_kesheng_sec_event_params_hourly a
left join stg_cpa_sec_event_params_last7_daily b
on a.event_name = b.event_name and a.param_name = b.param_name and a.param_val = b.param_val
where a.src_file_day <= 20170315
and a.src_file_day > from_unixtime(unix_timestamp('20170315','yyyyMMdd')-60*60*24*7,'yyyyMMdd')
group by if(substr(a.grain_ind,1,1)='0', '-1', a.app_channel_id)
,if(substr(a.grain_ind,2,1)='0', -1, a.product_key)
,if(substr(a.grain_ind,3,1)='0', '-1', a.app_ver_code)
,a.event_name
,a.param_name
,b.param_val
)t0
) t1
left join mscdata.dim_kesheng_sdk_app_pkg b1 on t1.product_key = b1.product_key
where b1.product_key is not null or t1.product_key = -1;

----------------------------------------------------------------------------------------

with stg_cpa_sec_event_params_last30_daily as
(
select a1.event_name
,a1.param_name
,a1.param_val
from (
select a.event_name
,a.param_name
,a.param_val
,sum(a.val_cnt)
,row_number()over(partition by a.event_name, a.param_name, a.param_val order by sum(a.val_cnt) desc) param_val_rank
from rptdata.fact_kesheng_sec_event_params_hourly a
where a.src_file_day <= 20170315
and a.src_file_day > from_unixtime(unix_timestamp('20170315','yyyyMMdd')-60*60*24*30,'yyyyMMdd')
and a.grain_ind = '000'
group by a.event_name
,a.param_name
,a.param_val
) a1
where a1.param_val_rank <= 1000
)
insert overwrite table app.cpa_sec_event_params_last30_daily partition (src_file_day = '20170315')
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
select t0.app_channel_id
,t0.product_key
,t0.app_ver_code
,t0.event_name
,t0.param_name
,nvl(t0.param_val, '-998') as param_val
,t0.val_cnt
,sum(t0.val_cnt) over(partition by t0.app_channel_id, t0.product_key, t0.app_ver_code, t0.event_name, t0.param_name) as all_val_cnt
from (
select if(substr(a.grain_ind,1,1)='0', '-1', a.app_channel_id) as app_channel_id
,if(substr(a.grain_ind,2,1)='0', -1, a.product_key) as product_key
,if(substr(a.grain_ind,3,1)='0', '-1', a.app_ver_code) as app_ver_code
,a.event_name
,a.param_name
,b.param_val
,sum(a.val_cnt) as val_cnt
from rptdata.fact_kesheng_sec_event_params_hourly a
left join stg_cpa_sec_event_params_last30_daily b
on a.event_name = b.event_name and a.param_name = b.param_name and a.param_val = b.param_val
where a.src_file_day <= '20170315'
and a.src_file_day > from_unixtime(unix_timestamp('20170315','yyyyMMdd')-60*60*24*30,'yyyyMMdd')
group by if(substr(a.grain_ind,1,1)='0', '-1', a.app_channel_id)
,if(substr(a.grain_ind,2,1)='0', -1, a.product_key)
,if(substr(a.grain_ind,3,1)='0', '-1', a.app_ver_code)
,a.event_name
,a.param_name
,b.param_val
)t0
) t1
left join mscdata.dim_kesheng_sdk_app_pkg b1 on t1.product_key = b1.product_key
where b1.product_key is not null or t1.product_key = -1;

select * from app.cpa_sec_event_params_last30_daily t;







