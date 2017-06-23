
-- 测试

--int层
select distinct(app_channel_id) from intdata.kesheng_event_params where src_file_day = 20170325;
+-----------------+--+
| app_channel_id  |
+-----------------+--+
| 200101          |
| 200102          |
| 200104          |
| 200105          |
| 200106          |
| 200107          |
| 200108          |
+-----------------+--+

select distinct(app_ver_code) from intdata.kesheng_event_params where src_file_day = 20170325;
+---------------+--+
| app_ver_code  |
+---------------+--+
| 2.1.4         |
| 2.3.6         |
| 2.8.1         |
| 3.1.9         |
+---------------+--+

select distinct(product_key) from intdata.kesheng_event_params where src_file_day = 20170325;
+--------------+--+
| product_key  |
+--------------+--+
| 10           |
| 20           |
| 30           |
+--------------+--+

(
(event_name, param_name, param_val), 
(app_channel_id, event_name, param_name, param_val),
(product_key, event_name, param_name, param_val),
(app_channel_id, product_key,event_name, param_name, param_val),
(product_key, app_ver_code, event_name, param_name, param_val),
(app_channel_id, product_key, app_ver_code, event_name, param_name, param_val)
)
 
--(1)全维度 
select t.event_name,t.param_name,t.param_val,count(1) as val_cnt
  from intdata.kesheng_event_params t 
 where src_file_day = 20170325 and t.event_name='EN_is' and t.param_name = 'datasets' 
 group by t.event_name,t.param_name,t.param_val;


select t.event_name,count(t.event_name) as event_cnt,sum(t.du) as sum_du
from intdata.kesheng_event_occur t 
where t.src_file_day = 20170325
group by t.event_name;

--(2)app_channel_id
select t.app_channel_id,t.event_name,t.param_name,t.param_val,count(1) as val_cnt
  from intdata.kesheng_event_params t 
 where src_file_day = 20170325 and t.app_channel_id=200108
 group by t.app_channel_id,t.event_name,t.param_name,t.param_val;
 
 select t.app_channel_id,t.event_name,count(t.event_name) as event_cnt,sum(t.du) as sum_du
from intdata.kesheng_event_occur t 
where t.src_file_day = 20170325 and app_channel_id <> '-1'
group by t.app_channel_id,t.event_name;
 
--(3)product_key
select t.product_key,t.event_name,t.param_name,t.param_val,count(1) as val_cnt
  from intdata.kesheng_event_params t 
 where src_file_day = 20170325 and product_key = 10
 group by t.product_key,t.event_name,t.param_name,t.param_val;
 
--(4)app_channel_id,product_key
select t.app_channel_id, t.product_key,t.event_name,t.param_name,t.param_val,count(1) as val_cnt
  from intdata.kesheng_event_params t 
 where src_file_day = 20170325 and app_channel_id='200105' and product_key = 10
 group by t.app_channel_id, t.product_key, t.event_name, t.param_name, t.param_val;

--(5)product_key,app_ver_code
select t.product_key,t.app_ver_code,t.event_name,t.param_name,t.param_val,count(1) as val_cnt
  from intdata.kesheng_event_params t 
 where src_file_day = 20170325 and product_key = 10 and app_ver_code = '2.3.6'
 group by t.product_key,t.app_ver_code,t.event_name,t.param_name,t.param_val;
 
--(6)product_key,app_ver_code
select t.app_channel_id,t.product_key,t.app_ver_code,t.event_name,t.param_name,t.param_val,count(1) as val_cnt
  from intdata.kesheng_event_params t 
 where src_file_day = 20170325 and app_channel_id='200106' and product_key = 10 and app_ver_code = '3.1.9'
 group by t.app_channel_id,t.product_key,t.app_ver_code,t.event_name,t.param_name,t.param_val;
 
-- app层

--(1)全维度 
select a.* 
from app.cpa_event_params_daily a 
where a.src_file_day = 20170325 
and app_channel_id='-1' and app_ver_code = '-1' and product_key = -1;

select a.* 
from app.cpa_event_occur_daily a 
where a.src_file_day = 20170325 
and app_channel_id='-1' and app_ver_code = '-1' and product_key = -1;

--(2)app_channel_id
select a.* 
from app.cpa_event_params_daily a 
where a.src_file_day = 20170325 
and app_channel_id='200108' and app_ver_code = '-1' and product_key = -1;

select a.* 
from app.cpa_event_occur_daily a 
where a.src_file_day = 20170325 
and app_channel_id <> '-1' and app_ver_code = '-1' and product_key = -1;

--(3)product_key
select a.* 
from app.cpa_event_params_daily a 
where a.src_file_day = 20170325 
and app_channel_id='-1' and app_ver_code = '-1' and product_key = 10;

--(4)app_channel_id,product_key
select a.* 
from app.cpa_event_params_daily a 
where a.src_file_day = 20170325 
and app_channel_id='200105' and app_ver_code = '-1' and product_key = 10;

--(5)product_key,app_ver_code
select a.* 
from app.cpa_event_params_daily a 
where a.src_file_day = 20170325 
and app_channel_id='-1' and app_ver_code = '2.3.6' and product_key = 10;

--(6)app_channel_id, product_key, app_ver_code
select a.* 
from app.cpa_event_params_daily a 
where a.src_file_day = 20170325 
and app_channel_id='200106' and app_ver_code = '3.1.9' and product_key = 10;

select a.* 
from app.cpa_event_params_daily a 
where a.src_file_day = 20170325 
and app_channel_id <> '-1' and app_ver_code <> '-1' and product_key <> -1;