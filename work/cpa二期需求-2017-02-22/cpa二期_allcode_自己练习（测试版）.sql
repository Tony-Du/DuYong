
-- 原始数据格式如下
{"customEvent": [
				{
				"eventName": "0x1",
				"du": "123",
				"timestamp": "1480521763049",  // 事件发生时间，app调用时由sdk产生的时间，UTC时间
				"eventParams": {
								"ContentID": "yixiuge",
								"account": "13856976635",
								"networkType": "WIFI",
								"result": "0",
								"type": "11",
								"loadTime": 1000
								}
				},
				{
				"eventName": "xx",
				"du": "xx",
				"timestamp": "1480521763049",   // 事件发生时间，app调用时由sdk产生的时间
				"eventParams": {
								"ContentID": "yixiuge",
								"account": "13856976635",
								"networkType": "WIFI",
								"result": "0",
								"type": "11",
								"loadTime": 1000
								}
				}
				],
 "sdkSessionInfo": {
					"imei": "",
					"udid": "ffffffff-edcd-a967-0000-0000000000001484875370454",
					"idfa": "",
					"idfv": "06759882-FAE1-4234-AA24-5185265BF7F0",
					"installationID": "ffffffff-edcd-a967-0000-0000000000001484875370454",
					"clientId": "1234",
					"appVersion": "2.1.3demo",
					"apppkg": "com.miguvideo.datauploadsdk_1",
					"appchannel": "baidu",
					"os": "AD",
					"networkType": "WIFI",
					"account": ""
					}
}

--1.建一个外部表，指向原数据所在hdfs上的位置=================================
create external table ods.kesheng_sec_json_ex_dy (
json string
)
partitioned by (src_file_day string, src_file_hour string)
location '/tmp/tony';

alter table ods.kesheng_sec_json_ex_dy add if not exists partition(src_file_day='20170208', src_file_hour='15')
location '20170208/15'

--2.解析原数据(第一层：sdkSessionInfo)=================================
drop table if exists temp.odsdata_kesheng_sec_event_json_dy;

create table temp.odsdata_kesheng_sec_event_json_dy(
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

---------------------------------------------------------------------------------------------
insert overwrite table temp.odsdata_kesheng_sec_event_json_dy partition(src_file_day=20170208, src_file_hour=15)
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
where a.src_file_day = 20170208 and a.src_file_hour = 15
and nvl(b.sdkSessionInfo_json,'{}') <> '{}' ;

-- 第一层解析后的数据形式
+-----------------------------------------------------+---------+----------------------------------------------------+---------+---------------------------------------+----------------------------------------------------+-------------+---------------+--------------------------------+---------------+-------+----------------+------------
|                      a.rowkey                       | a.imei  |                       a.udid                       | a.idfa  |                a.idfv                 |                  a.installationid                  | a.clientid  | a.appversion  |            a.apppkg            | a.appchannel  | a.os  | a.networktype  | a.account  
+-----------------------------------------------------+---------+----------------------------------------------------+---------+---------------------------------------+----------------------------------------------------+-------------+---------------+--------------------------------+---------------+-------+----------------+------------
| hdfs://ns1/tmp/tony/20170208/15/input_sample.txt:0  |         | ffffffff-edcd-a967-0000-0000000000001484875370454  |         | 06759882-FAE1-4234-AA24-5185265BF7F0  | ffffffff-edcd-a967-0000-0000000000001484875370454  | 1234        | 2.1.3demo     | com.miguvideo.datauploadsdk_1  | baidu         | AD    | WIFI           |            
+-----------------------------------------------------+---------+----------------------------------------------------+---------+---------------------------------------+----------------------------------------------------+-------------+---------------+--------------------------------+---------------+-------+----------------+------------
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------+------------------+--+
|                                                                                                                                                                              a.customevent_json                                                                                                                                                                              | a.src_file_day  | a.src_file_hour  |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------+------------------+--+
| [{"eventName":"xx","du":"xx","timestamp":"1480521763049","eventParams":{"ContentID":"yixiuge","account":"13856976635","networkType":"WIFI","result":"0","type":"11","loadTime":1000}},{"eventName":"xx","du":"xx","timestamp":"1480521763049","eventParams":{"ContentID":"yixiuge","account":"13856976635","networkType":"WIFI","result":"0","type":"11","loadTime":1000}}]  | 20170208        | 15               |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------+------------------+--+


-- 3.解析原数据(第二层：customEvent)=================================

create or replace view temp.ods_kesheng_sec_event_param_json_v_dy
as
select a.rowkey
,a.imei
,a.udid
,a.idfa
,a.idfv
,a.installationID
,a.clientId
,a.appVersion
,a.apppkg
,a.appchannel
,a.os
,a.networkType
,a.account
,c.eventName
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


-- 第二层解析后的数据形式
+-----------------------------------------------------+---------+----------------------------------------------------+---------+---------------------------------------+----------------------------------------------------+-------------+---------------+--------------------------------+---------------+-------+----------------+------------+--------------+-------+----------------
|                      a.rowkey                       | a.imei  |                       a.udid                       | a.idfa  |                a.idfv                 |                  a.installationid                  | a.clientid  | a.appversion  |            a.apppkg            | a.appchannel  | a.os  | a.networktype  | a.account  | a.eventname  | a.du  |  a.timestamp   
+-----------------------------------------------------+---------+----------------------------------------------------+---------+---------------------------------------+----------------------------------------------------+-------------+---------------+--------------------------------+---------------+-------+----------------+------------+--------------+-------+----------------
| hdfs://ns1/tmp/tony/20170208/15/input_sample.txt:0  |         | ffffffff-edcd-a967-0000-0000000000001484875370454  |         | 06759882-FAE1-4234-AA24-5185265BF7F0  | ffffffff-edcd-a967-0000-0000000000001484875370454  | 1234        | 2.1.3demo     | com.miguvideo.datauploadsdk_1  | baidu         | AD    | WIFI           |            | xx           | xx    | 1480521763049  
| hdfs://ns1/tmp/tony/20170208/15/input_sample.txt:0  |         | ffffffff-edcd-a967-0000-0000000000001484875370454  |         | 06759882-FAE1-4234-AA24-5185265BF7F0  | ffffffff-edcd-a967-0000-0000000000001484875370454  | 1234        | 2.1.3demo     | com.miguvideo.datauploadsdk_1  | baidu         | AD    | WIFI           |            | xx           | xx    | 1480521763049  
+-----------------------------------------------------+---------+----------------------------------------------------+---------+---------------------------------------+----------------------------------------------------+-------------+---------------+--------------------------------+---------------+-------+----------------+------------+--------------+-------+----------------
+----------------------------------------------------------------------------------------------------------------+-----------------+------------------+--+
|                                               a.eventparams_json                                               | a.src_file_day  | a.src_file_hour  |
+----------------------------------------------------------------------------------------------------------------+-----------------+------------------+--+
| {"ContentID":"yixiuge","account":"13856976635","networkType":"WIFI","result":"0","type":"11","loadTime":1000}  | 20170208        | 15               |
| {"ContentID":"yixiuge","account":"13856976635","networkType":"WIFI","result":"0","type":"11","loadTime":1000}  | 20170208        | 15               |
+----------------------------------------------------------------------------------------------------------------+-----------------+------------------+--+

-- 3.解析原数据(第三层：eventParams)=================================
-- 分析:因为eventParams(json格式)中的参数个数不确定，所以用json_tuple()函数无法解析,
-- 在这里通过(",")来分割不同的键值对，再通过(":")来分割键值对中的键和值

-- 1)通过(",")来分割不同的键值对
select  a.eventName
,b.param_pos
,b.param_key_val
from temp.ods_kesheng_sec_event_param_json_v_dy a 
lateral view posexplode(split(regexp_replace(a.eventParams_json, '\\{|\\}', ''), '"\\s*,\\s*"')) b 
as param_pos, param_key_val;
+--------------+--------------+------------------------+--+
| a.eventname  | b.param_pos  |    b.param_key_val     |
+--------------+--------------+------------------------+--+
| xx           | 0            | "ContentID":"yixiuge   |
| xx           | 1            | account":"13856976635  |
| xx           | 2            | networkType":"WIFI     |
| xx           | 3            | result":"0             |
| xx           | 4            | type":"11              |
| xx           | 5            | loadTime":1000         |
| xx           | 0            | "ContentID":"yixiuge   |
| xx           | 1            | account":"13856976635  |
| xx           | 2            | networkType":"WIFI     |
| xx           | 3            | result":"0             |
| xx           | 4            | type":"11              |
| xx           | 5            | loadTime":1000         |
+--------------+--------------+------------------------+--+

--2)通过(":")来分割键值对中的键和值
select a.eventName
,b.param_pos
,split(b.param_key_val, '"\\s*:\\s*"') as param_key_val
from temp.ods_kesheng_sec_event_param_json_v_dy a 
lateral view posexplode(split(regexp_replace(a.eventParams_json, '\\{|\\}', ''), '"\\s*,\\s*"')) b 
as param_pos, param_key_val;
+--------------+--------------+----------------------------+--+
| a.eventname  | b.param_pos  |       param_key_val        |
+--------------+--------------+----------------------------+--+
| xx           | 0            | ["\"ContentID","yixiuge"]  |
| xx           | 1            | ["account","13856976635"]  |
| xx           | 2            | ["networkType","WIFI"]     |
| xx           | 3            | ["result","0"]             |
| xx           | 4            | ["type","11"]              |
| xx           | 5            | ["loadTime\":1000"]        |
| xx           | 0            | ["\"ContentID","yixiuge"]  |
| xx           | 1            | ["account","13856976635"]  |
| xx           | 2            | ["networkType","WIFI"]     |
| xx           | 3            | ["result","0"]             |
| xx           | 4            | ["type","11"]              |
| xx           | 5            | ["loadTime\":1000"]        |
+--------------+--------------+----------------------------+--+

select 
t1.eventName
,t1.param_pos
,t1.param_key_val[0]
,t1.param_key_val[1]
from(
select  a.eventName
,b.param_pos
,split(b.param_key_val, '"\\s*:\\s*"') as param_key_val
from temp.ods_kesheng_sec_event_param_json_v_dy a 
lateral view posexplode(split(regexp_replace(a.eventParams_json, '\\{|\\}', ''), '"\\s*,\\s*"')) b 
as param_pos, param_key_val
)t1 ;
+---------------+---------------+-----------------+--------------+--+
| t1.eventname  | t1.param_pos  |       _c2       |     _c3      |
+---------------+---------------+-----------------+--------------+--+
| xx            | 0             | "ContentID      | yixiuge      |
| xx            | 1             | account         | 13856976635  |
| xx            | 2             | networkType     | WIFI         |
| xx            | 3             | result          | 0            |
| xx            | 4             | type            | 11           |
| xx            | 5             | loadTime":1000  | NULL         |
| xx            | 0             | "ContentID      | yixiuge      |
| xx            | 1             | account         | 13856976635  |
| xx            | 2             | networkType     | WIFI         |
| xx            | 3             | result          | 0            |
| xx            | 4             | type            | 11           |
| xx            | 5             | loadTime":1000  | NULL         |
+---------------+---------------+-----------------+--------------+--+

--由上面的结果可以知道："loadTime":1000 这样的数据无法用正则表达式("\\s*:\\s*")来解析
--同理，如果出现 "loadTime":1000,"type":"11" 这样的数据，用正则表达式("\\s*,\\s*")也是无法解析的
-- 解决方法：提前把 " 替换成空字符串，然后用正则表达式(\\s*,\\s*)和(\\s*:\\s*")来解析，见下面实验

select  a.eventName
,b.param_pos
,b.param_key_val
from temp.ods_kesheng_sec_event_param_json_v_dy a 
lateral view posexplode(split(regexp_replace(regexp_replace(a.eventParams_json, '\\{|\\}', ''), '"', ''), '\\s*,\\s*')) b 
as param_pos, param_key_val;
+--------------+--------------+----------------------+--+
| a.eventname  | b.param_pos  |   b.param_key_val    |
+--------------+--------------+----------------------+--+
| xx           | 0            | ContentID:yixiuge    |
| xx           | 1            | account:13856976635  |
| xx           | 2            | networkType:WIFI     |
| xx           | 3            | result:0             |
| xx           | 4            | type:11              |
| xx           | 5            | loadTime:1000        |
| xx           | 0            | ContentID:yixiuge    |
| xx           | 1            | account:13856976635  |
| xx           | 2            | networkType:WIFI     |
| xx           | 3            | result:0             |
| xx           | 4            | type:11              |
| xx           | 5            | loadTime:1000        |
+--------------+--------------+----------------------+--+

select  a.eventName
,b.param_pos
,split(b.param_key_val, '\\s*:\\s*') as param_key_val
from temp.ods_kesheng_sec_event_param_json_v_dy a 
lateral view posexplode(split(regexp_replace(regexp_replace(a.eventParams_json, '\\{|\\}', ''), '"', ''), '\\s*,\\s*')) b 
as param_pos, param_key_val;
+--------------+--------------+----------------------------+--+
| a.eventname  | b.param_pos  |       param_key_val        |
+--------------+--------------+----------------------------+--+
| xx           | 0            | ["ContentID","yixiuge"]    |
| xx           | 1            | ["account","13856976635"]  |
| xx           | 2            | ["networkType","WIFI"]     |
| xx           | 3            | ["result","0"]             |
| xx           | 4            | ["type","11"]              |
| xx           | 5            | ["loadTime","1000"]        |
| xx           | 0            | ["ContentID","yixiuge"]    |
| xx           | 1            | ["account","13856976635"]  |
| xx           | 2            | ["networkType","WIFI"]     |
| xx           | 3            | ["result","0"]             |
| xx           | 4            | ["type","11"]              |
| xx           | 5            | ["loadTime","1000"]        |
+--------------+--------------+----------------------------+--+

select 
t1.eventName
,t1.param_pos
,t1.param_key_val[0] as param_name
,t1.param_key_val[1] as param_val
from(
select  a.eventName
,b.param_pos
,split(b.param_key_val, '\\s*:\\s*') as param_key_val
from temp.ods_kesheng_sec_event_param_json_v_dy a 
lateral view posexplode(split(regexp_replace(regexp_replace(a.eventParams_json, '\\{|\\}', ''), '"', ''), '\\s*,\\s*')) b  
as param_pos, param_key_val
)t1 ;
+---------------+---------------+--------------+--------------+--+
| t1.eventname  | t1.param_pos  |  param_name  |  param_val   |
+---------------+---------------+--------------+--------------+--+
| xx            | 0             | ContentID    | yixiuge      |
| xx            | 1             | account      | 13856976635  |
| xx            | 2             | networkType  | WIFI         |
| xx            | 3             | result       | 0            |
| xx            | 4             | type         | 11           |
| xx            | 5             | loadTime     | 1000         |
| xx            | 0             | ContentID    | yixiuge      |
| xx            | 1             | account      | 13856976635  |
| xx            | 2             | networkType  | WIFI         |
| xx            | 3             | result       | 0            |
| xx            | 4             | type         | 11           |
| xx            | 5             | loadTime     | 1000         |
+---------------+---------------+--------------+--------------+--+


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
where v.src_file_day = 20170208
and v.src_file_hour = 15
and nvl(v.event_name, '') <> ''
)
insert overwrite table temp.intdata_kesheng_sec_event_params_dy partition(src_file_day=20170208, src_file_hour=15)
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
+-----------------------------------------------------+-----------------+--------------------------------+-------------------+----------------+---------------+---------------+--------------+----------------+-----------------+------------------+--+
|                      t.rowkey                       | t.app_ver_code  |         t.app_pkg_name         | t.app_channel_id  | t.app_os_type  | t.event_name  | t.param_name  | t.param_val  | t.product_key  | t.src_file_day  | t.src_file_hour  |
+-----------------------------------------------------+-----------------+--------------------------------+-------------------+----------------+---------------+---------------+--------------+----------------+-----------------+------------------+--+
| hdfs://ns1/tmp/tony/20170208/15/input_sample.txt:0  | 2.1.3demo       | com.miguvideo.datauploadsdk_1  | baidu             | AD             | xx            | ContentID     | yixiuge      | -998           | 20170208        | 15               |
| hdfs://ns1/tmp/tony/20170208/15/input_sample.txt:0  | 2.1.3demo       | com.miguvideo.datauploadsdk_1  | baidu             | AD             | xx            | account       | 13856976635  | -998           | 20170208        | 15               |
| hdfs://ns1/tmp/tony/20170208/15/input_sample.txt:0  | 2.1.3demo       | com.miguvideo.datauploadsdk_1  | baidu             | AD             | xx            | networkType   | WIFI         | -998           | 20170208        | 15               |
| hdfs://ns1/tmp/tony/20170208/15/input_sample.txt:0  | 2.1.3demo       | com.miguvideo.datauploadsdk_1  | baidu             | AD             | xx            | result        | 0            | -998           | 20170208        | 15               |
| hdfs://ns1/tmp/tony/20170208/15/input_sample.txt:0  | 2.1.3demo       | com.miguvideo.datauploadsdk_1  | baidu             | AD             | xx            | type          | 11           | -998           | 20170208        | 15               |
| hdfs://ns1/tmp/tony/20170208/15/input_sample.txt:0  | 2.1.3demo       | com.miguvideo.datauploadsdk_1  | baidu             | AD             | xx            | loadTime      | 1000         | -998           | 20170208        | 15               |
| hdfs://ns1/tmp/tony/20170208/15/input_sample.txt:0  | 2.1.3demo       | com.miguvideo.datauploadsdk_1  | baidu             | AD             | xx            | ContentID     | yixiuge      | -998           | 20170208        | 15               |
| hdfs://ns1/tmp/tony/20170208/15/input_sample.txt:0  | 2.1.3demo       | com.miguvideo.datauploadsdk_1  | baidu             | AD             | xx            | account       | 13856976635  | -998           | 20170208        | 15               |
| hdfs://ns1/tmp/tony/20170208/15/input_sample.txt:0  | 2.1.3demo       | com.miguvideo.datauploadsdk_1  | baidu             | AD             | xx            | networkType   | WIFI         | -998           | 20170208        | 15               |
| hdfs://ns1/tmp/tony/20170208/15/input_sample.txt:0  | 2.1.3demo       | com.miguvideo.datauploadsdk_1  | baidu             | AD             | xx            | result        | 0            | -998           | 20170208        | 15               |
| hdfs://ns1/tmp/tony/20170208/15/input_sample.txt:0  | 2.1.3demo       | com.miguvideo.datauploadsdk_1  | baidu             | AD             | xx            | type          | 11           | -998           | 20170208        | 15               |
| hdfs://ns1/tmp/tony/20170208/15/input_sample.txt:0  | 2.1.3demo       | com.miguvideo.datauploadsdk_1  | baidu             | AD             | xx            | loadTime      | 1000         | -998           | 20170208        | 15               |
+-----------------------------------------------------+-----------------+--------------------------------+-------------------+----------------+---------------+---------------+--------------+----------------+-----------------+------------------+--+


-- intdata层:事件发生表 ---------------------------------

insert overwrite table temp.intdata_kesheng_sec_event_occur_dy partition (src_file_day = 20170208, src_file_hour = 15)
select v.rowkey
,v.app_ver_code
,v.app_pkg_name
,v.app_channel_id
,v.app_os_type
,v.event_name
,v.du
,v.timestamp
,nvl(d.product_key, -998) product_key
from temp.ods_kesheng_sec_event_param_json_v_dy v 
left join mscdata.dim_kesheng_sdk_app_pkg d on (v.app_pkg_name = d.app_pkg_name and v.app_os_type = d.app_os_type)
where v.src_file_day = 20170208
and v.src_file_hour = 15
and nvl(v.event_name, '') <> '';
+-----------------------------------------------------+-----------------+--------------------------------+-------------------+----------------+---------------+-------+----------------+----------------+-----------------+------------------+--+
|                      t.rowkey                       | t.app_ver_code  |         t.app_pkg_name         | t.app_channel_id  | t.app_os_type  | t.event_name  | t.du  |  t.timestamp   | t.product_key  | t.src_file_day  | t.src_file_hour  |
+-----------------------------------------------------+-----------------+--------------------------------+-------------------+----------------+---------------+-------+----------------+----------------+-----------------+------------------+--+
| hdfs://ns1/tmp/tony/20170208/15/input_sample.txt:0  | 2.1.3demo       | com.miguvideo.datauploadsdk_1  | baidu             | AD             | xx            | xx    | 1480521763049  | -998           | 20170208        | 15               |
| hdfs://ns1/tmp/tony/20170208/15/input_sample.txt:0  | 2.1.3demo       | com.miguvideo.datauploadsdk_1  | baidu             | AD             | xx            | xx    | 1480521763049  | -998           | 20170208        | 15               |
+-----------------------------------------------------+-----------------+--------------------------------+-------------------+----------------+---------------+-------+----------------+----------------+-----------------+------------------+--+


-- rptdata层:事件发生汇总 ---------------------------------   

insert overwrite table temp.rptdata_fact_kesheng_sec_event_occur_hourly_dy partition (src_file_day = 20170208, src_file_hour = 15)
select a.app_channel_id
,a.product_key
,a.app_ver_code
,a.event_name
,count(1) as event_cnt
,sum(a.du) as sum_du
,rpad(reverse(bin(cast(grouping__id as int))),3,'0') grain_ind
from temp.intdata_kesheng_sec_event_occur_dy a
where a.src_file_day = 20170208
and a.src_file_hour = 15
group by app_channel_id, product_key, app_ver_code, event_name
grouping sets((), app_channel_id, product_key,
(app_channel_id, product_key),
(product_key, app_ver_code),
(app_channel_id, product_key, app_ver_code)
);

+-------------------+----------------+-----------------+---------------+--------------+-----------+--------------+-----------------+------------------+--+
| t.app_channel_id  | t.product_key  | t.app_ver_code  | t.event_name  | t.event_cnt  | t.sum_du  | t.grain_ind  | t.src_file_day  | t.src_file_hour  |
+-------------------+----------------+-----------------+---------------+--------------+-----------+--------------+-----------------+------------------+--+
| NULL              | NULL           | NULL            | NULL          | 2            | 0         | 000          | 20170208        | 15               |
| NULL              | -998           | NULL            | NULL          | 2            | 0         | 010          | 20170208        | 15               |
| NULL              | -998           | 2.1.3demo       | NULL          | 2            | 0         | 011          | 20170208        | 15               |
| baidu             | NULL           | NULL            | NULL          | 2            | 0         | 100          | 20170208        | 15               |
| baidu             | -998           | NULL            | NULL          | 2            | 0         | 110          | 20170208        | 15               |
| baidu             | -998           | 2.1.3demo       | NULL          | 2            | 0         | 111          | 20170208        | 15               |
+-------------------+----------------+-----------------+---------------+--------------+-----------+--------------+-----------------+------------------+--+


-- rptdata层:事件参数汇总 ---------------------------------

insert overwrite table temp.rptdata_fact_kesheng_sec_event_params_hourly_dy partition (src_file_day = 20170208, src_file_hour = 15)
select a.app_channel_id
,a.product_key
,a.app_ver_code
,a.event_name
,a.param_name
,a.param_val
,count(1) as val_cnt
,rpad(reverse(bin(cast(grouping__id as int))),3,'0') as grain_ind
from temp.intdata_kesheng_sec_event_params_dy a
where a.src_file_day = 20170208
and a.src_file_hour = 15
group by app_channel_id, product_key, app_ver_code, event_name, param_name, param_val
grouping sets((), app_channel_id, product_key,
(app_channel_id, product_key),
(product_key, app_ver_code),
(app_channel_id, product_key, app_ver_code));

+-------------------+----------------+-----------------+---------------+---------------+--------------+------------+--------------+-----------------+------------------+--+
| t.app_channel_id  | t.product_key  | t.app_ver_code  | t.event_name  | t.param_name  | t.param_val  | t.val_cnt  | t.grain_ind  | t.src_file_day  | t.src_file_hour  |
+-------------------+----------------+-----------------+---------------+---------------+--------------+------------+--------------+-----------------+------------------+--+
| NULL              | NULL           | NULL            | NULL          | NULL          | NULL         | 12         | 000          | 20170208        | 15               |
| NULL              | -998           | NULL            | NULL          | NULL          | NULL         | 12         | 010          | 20170208        | 15               |
| NULL              | -998           | 2.1.3demo       | NULL          | NULL          | NULL         | 12         | 011          | 20170208        | 15               |
| baidu             | NULL           | NULL            | NULL          | NULL          | NULL         | 12         | 100          | 20170208        | 15               |
| baidu             | -998           | NULL            | NULL          | NULL          | NULL         | 12         | 110          | 20170208        | 15               |
| baidu             | -998           | 2.1.3demo       | NULL          | NULL          | NULL         | 12         | 111          | 20170208        | 15               |
+-------------------+----------------+-----------------+---------------+---------------+--------------+------------+--------------+-----------------+------------------+--+

-- app层:事件发生 ---------------------------------

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
where a.src_file_day = 20170208 and a.src_file_hour = 15
group by if(substr(a.grain_ind,1,1) = '0', '-1', a.app_channel_id)
,if(substr(a.grain_ind,2,1) = '0', -1, a.product_key)
,if(substr(a.grain_ind,3,1) = '0', '-1', a.app_ver_code)
,a.event_name
) t1
left join mscdata.dim_kesheng_sdk_app_pkg b1 on t1.product_key = b1.product_key
;
+-----------------+---------------+------------------+--------------------+----------------+---------------+------------+---------+--+
| t1.product_key  | product_name  | t1.app_ver_code  | t1.app_channel_id  | t1.event_name  | t1.event_cnt  | t1.sum_du  | avg_du  |
+-----------------+---------------+------------------+--------------------+----------------+---------------+------------+---------+--+
| -998            |               | -1               | -1                 | NULL           | 2             | 0          | 0       |
| -998            |               | 2.1.3demo        | -1                 | NULL           | 2             | 0          | 0       |
| -1              | -1            | -1               | -1                 | NULL           | 2             | 0          | 0       |
| -998            |               | -1               | baidu              | NULL           | 2             | 0          | 0       |
| -998            |               | 2.1.3demo        | baidu              | NULL           | 2             | 0          | 0       |
| -1              | -1            | -1               | baidu              | NULL           | 2             | 0          | 0       |
+-----------------+---------------+------------------+--------------------+----------------+---------------+------------+---------+--+


insert overwrite table temp.app_cpa_sec_event_occur_hourly_dy partition (src_file_day = 20170208, src_file_hour = 15)
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
where a.src_file_day = 20170208 and a.src_file_hour = 15
group by if(substr(a.grain_ind,1,1) = '0', '-1', a.app_channel_id)
,if(substr(a.grain_ind,2,1) = '0', -1, a.product_key)
,if(substr(a.grain_ind,3,1) = '0', '-1', a.app_ver_code)
,a.event_name
) t1
left join mscdata.dim_kesheng_sdk_app_pkg b1 on t1.product_key = b1.product_key
where b1.product_key is not null or t1.product_key = -1;

+----------------+-----------------+-----------------+-------------------+---------------+--------------+-----------+-----------+-----------------+------------------+--+
| t.product_key  | t.product_name  | t.app_ver_code  | t.app_channel_id  | t.event_name  | t.event_cnt  | t.sum_du  | t.avg_du  | t.src_file_day  | t.src_file_hour  |
+----------------+-----------------+-----------------+-------------------+---------------+--------------+-----------+-----------+-----------------+------------------+--+
| -1             | -1              | -1              | -1                | NULL          | 2            | 0         | 0         | 20170208        | 15               |
| -1             | -1              | -1              | baidu             | NULL          | 2            | 0         | 0         | 20170208        | 15               |
+----------------+-----------------+-----------------+-------------------+---------------+--------------+-----------+-----------+-----------------+------------------+--+








