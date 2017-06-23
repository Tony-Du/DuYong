

insert overwrite table rptdata.fact_kesheng_sdk_session_start_hourly partition(src_file_day='${EXTRACT_DATE}', src_file_hour='${EXTRACT_HOUR}')
select nvl(t2.device_key,-998), t1.imei, t1.user_id, t1.idfa, t1.imsi, t1.app_ver_code
      ,t1.app_pkg_name, t1.phone_number, t1.app_os_type
      ,t1.install_id, t1.first_launch_channel_id, t1.app_channel_id
      ,t1.product_key, count(1) as start_cnt
  from (select a1.imei, a1.user_id, a1.idfa, a1.imsi, a1.app_ver_code
              ,a1.app_pkg_name, a1.phone_number
              ,a1.app_os_type, a1.install_id, a1.first_launch_channel_id
              ,a1.app_channel_id ,nvl(d1.product_key,-998) product_key  
          from int.kesheng_sdk_session_start_cpa_v a1
          left join mscdata.dim_kesheng_sdk_app_pkg d1
            on (d1.app_os_type = a1.app_os_type and d1.app_pkg_name = a1.app_pkg_name)
         where a1.src_file_day = '${EXTRACT_DATE}'
           and a1.src_file_hour = '${EXTRACT_HOUR}'
       ) t1
  left join
       (select * from intdata.kesheng_sdk_active_device_hourly b2
         where b2.src_file_day = '${EXTRACT_DATE}'
       ) t2
    on (t1.app_os_type = t2.app_os_type and t1.imei = t2.imei and t1.user_id = t2.user_id
          and t1.idfa = t2.idfa and t1.imsi = t2.imsi and t1.phone_number = t2.phone_number
        )
 group by nvl(t2.device_key,-998), t1.imei, t1.user_id, t1.idfa, t1.imsi, t1.app_ver_code
         ,t1.app_pkg_name, t1.app_channel_id, t1.phone_number, t1.app_os_type
         ,t1.install_id, t1.first_launch_channel_id, t1.product_key;
-- ##################################################################################################################### --

insert overwrite table stg.fact_kesheng_sdk_session_error_daily_01
select t1.app_channel_id, t1.product_key, t1.app_ver_code
      ,sum(t1.start_cnt) start_cnt
      ,0 duration_ms
      ,0 error_cnt
      ,0 error_imei_num
      ,rpad(reverse(bin(cast( grouping__id as int))),3,'0') grain_ind		-- 粒度标识 生成算法
      ,'${EXTRACT_DATE}' src_file_day
 from rptdata.fact_kesheng_sdk_session_start_hourly t1
where t1.src_file_day  = '${EXTRACT_DATE}'
group by app_channel_id, product_key, app_ver_code
grouping sets((), product_key, app_channel_id
            ,(product_key, app_ver_code), (product_key, app_channel_id)
            ,(product_key, app_ver_code, app_channel_id));
			
-- ()=0,product_key=2,app_channel_id=1,(product_key, app_ver_code)=6,(product_key, app_channel_id)=3,(product_key, app_ver_code, app_channel_id)=7
-- reverse以后：()=000,product_key=010,app_channel_id=100,(product_key, app_ver_code)=011,(product_key, app_channel_id)=110,(product_key, app_ver_code, app_channel_id)=111
-- 粒度标识,000 - 将所渠道、产品、版本汇总成一条记录, 010 - 产品, 011 - 产品+版本, 100 - 渠道, 110 - 渠道+产品, 111 - 渠道+产品+版本
			
-- cast(grouping__id as int):把grouping_id 转换为 int类型，默认的grouping_id是什么类型？
-- bin(n):n为bigint类型，返回N的二进制值的字符串表示形式，返回值类型string
-- reverse(str):返回字符串str的字符颠倒顺序
-- rpad()：在PL/SQL中用于往源字符串的左侧填充一些字符
-- 语法：lpad( string1, padded_length, [ pad_string ] )

-- grouping_id的算法：
-- grouping__id与group by后面的字段一一对应，
--如上：app_channel_id, product_key, app_ver_code(反转前)			反转后：
			0				0			0							0--()
			1				0			0							1--app_channel_id
			0				1			0							2--product_key
			0				1			1							6--product_key + app_ver_code
			1				1			0							3--product_key + app_channel_id
			1				1			1							7--product_key + app_ver_code + app_channel_id

NULL    		NULL    NULL    0
201800000010042 NULL    NULL    1
baidu   		NULL    NULL    1
NotSetChannel   NULL    NULL    1
DeveloperDebug  NULL    NULL    1
43243243        NULL    NULL    1
201800020010001 NULL    NULL    1
NULL    		10      NULL    2
NULL    		-998    NULL    2
200300140100007 10      NULL    3
201800000010059 10      NULL    3
201800000000015 10      NULL    3
NULL    		-998    7.0     6
NULL    		10      3.2.1   6
NULL    		10      3.2.0   6
NULL    		-998    2.1.3demo       6
200300080100006 10      3.2.0   7
201800000010098 10      3.2.0   7
200300280000002 10      3.2.0   7
201800000010097 10      3.2.0   7
201800000010096 10      3.2.0   7
201800000010088 10      3.2.0   7

总结：按照group by后面字段的顺序，维度存在取1，维度不存在取0
	  把上述二进制数进项反转，即grouping__id的值

-- ====================================================
select b.product_key,
	a.channel_id,
	a.page_id,
	a.os,
	count(distinct(concat(a.cookie_id,a.ip_addr)))as uv_num
	count(1) as pv_cnt
from intdata.kesheng_h5_common a
left join mscdata.dim_kesheng_h5_product b on a.product_id = b.product_id
where a.src_file_day='201702110'
group by product_key,channel_id,page_id,os
grouping sets(product_key,(product_key,channel_id),(product_key,page_id),(product_key,os),
(product_key,channel_id,page_id),(product_key,channel_id,os),(product_key,page_id,os),(product_key,channel_id,page_id,os));

FAILED: SemanticException [Error 10210]:Grouping sets aggregations (with rollups or cubes) are not allowed if aggregation function parameters overlap with the aggregation functions columns

count(a.page_id)-> count(1)