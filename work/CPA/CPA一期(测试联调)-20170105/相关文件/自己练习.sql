
select src_file_day,count(*),count(1),sum(new_cnt),count(distinct device_key),count(device_key)
from rptdata.fact_kesheng_sdk_new_device_hourly t1 
where src_file_day>= 20170112 
group by src_file_day 
order by src_file_day;

20170112	962178	962178	253554	98654	962178
20170113	979866	979866	232172	100534	979866
20170114	1046016	1046016	251872	108584	1046016
20170115	1047804	1047804	221942	108063	1047804
20170116	1029654	1029654	212053	108179	1029654
20170117	502552	502552	87350	56978	502552
20170118	337776	337776	48137	30163	337776
20170119	352278	352278	51770	31551	352278
20170120	370848	370848	79693	34584	370848
20170121	388158	388158	68396	35417	388158
20170122	396282	396282	57415	35627	396282
20170123	394560	394560	49783	35274	394560
20170124	96294	96294	9606	12044	96294

count(*)=count(1)=count(device)
--为什么sum(new_cnt)>count(distinct device_key)???


with deviceinfo as
(
SELECT
case when t1.imei is null or t1.imei = '' or t1.imei = 'null' then '-998' else t1.imei end as imei,
case when t1.user_id is null or t1.user_id = '' or t1.user_id = 'null' then '-998' else t1.user_id end as user_id,
case when t1.idfa is null or t1.idfa = '' or t1.idfa = 'null' then '-998' else t1.idfa end as idfa,
case when t1.imsi is null or t1.imsi = '' or t1.imsi = 'null' then '-998' else t1.imsi end as imsi,    
t1.app_ver_code,
t2.app_os_type,
nvl(t2.product_key, -998) AS product_key, 
t1.first_launch_channel_id AS app_channel_id,		
t1.start_unix_time,
t1.src_file_day,
t1.src_file_hour
FROM intdata.kesheng_sdk_session_start t1 LEFT JOIN mscdata.dim_kesheng_sdk_app_pkg t2 ON (t1.os = t2.app_os_type AND t1.app_pkg_name = t2.app_pkg_name)
WHERE t1.src_file_day = '20170117'
)
INSERT OVERWRITE TABLE stg.fact_kesheng_sdk_new_device_hourly_01
SELECT device_key, 
app_channel_id, 
product_key,
app_ver_code, 
min(start_unix_time) AS start_unix_time,
src_file_day, 
src_file_hour
FROM (select t2.device_key, t1.* from deviceinfo t1 join intdata.kesheng_sdk_imei_user_device_key t2 on (t1.imei = t2.imei and t1.user_id = t2.user_id) where t1.app_os_type = 'AD' and t1.imei <> '-998' and t1.user_id <> '-998'
union all
select t2.device_key, t1.* from deviceinfo t1 join intdata.kesheng_sdk_imei_imsi_device_key t2 on (t1.imei  = t2.imei and t1.imsi = t2.imsi) where t1.app_os_type = 'AD' and t1.imei <> '-998' and t1.user_id = '-998' and t1.imsi <> '-998'
union all
select t2.device_key, t1.* from deviceinfo t1 join intdata.kesheng_sdk_idfa_user_device_key t2 on (t1.idfa = t2.idfa and t1.user_id = t2.user_id) where t1.app_os_type = 'iOS'
) t
GROUP BY device_key, app_channel_id, product_key, app_ver_code, src_file_day, src_file_hour;

-- ============================================================================================================== --
with deviceinfo as
(
SELECT
case when t1.imei is null or t1.imei = '' or t1.imei = 'null' then '-998' else t1.imei end as imei,
case when t1.user_id is null or t1.user_id = '' or t1.user_id = 'null' then '-998' else t1.user_id end as user_id,
case when t1.idfa is null or t1.idfa = '' or t1.idfa = 'null' then '-998' else t1.idfa end as idfa,
case when t1.imsi is null or t1.imsi = '' or t1.imsi = 'null' then '-998' else t1.imsi end as imsi,    
t1.app_ver_code,
t1.os app_os_type,
nvl(t2.product_key, -998) AS product_key, 
t1.first_launch_channel_id AS app_channel_id,		
t1.start_unix_time,
t1.src_file_day,
t1.src_file_hour
FROM intdata.kesheng_sdk_session_start t1 LEFT JOIN mscdata.dim_kesheng_sdk_app_pkg t2 ON (t1.os = t2.app_os_type AND t1.app_pkg_name = t2.app_pkg_name)
WHERE t1.src_file_day = '20170117'
)
INSERT OVERWRITE TABLE stg.fact_kesheng_sdk_new_device_hourly_01
SELECT device_key, 
app_channel_id, 
product_key,
app_ver_code, 
min(start_unix_time) AS start_unix_time,
src_file_day, 
src_file_hour
FROM (select t2.device_key, t1.* from deviceinfo t1 join intdata.kesheng_sdk_imei_user_device_key t2 on (t1.imei = t2.imei and t1.user_id = t2.user_id) where t1.app_os_type = 'AD' and t1.imei <> '-998' and t1.user_id <> '-998'
union all
select t2.device_key, t1.* from deviceinfo t1 join intdata.kesheng_sdk_imei_imsi_device_key t2 on (t1.imei  = t2.imei and t1.imsi = t2.imsi) where t1.app_os_type = 'AD' and t1.imei <> '-998' and t1.user_id = '-998' and t1.imsi <> '-998'
union all
select t2.device_key, t1.* from deviceinfo t1 join intdata.kesheng_sdk_idfa_user_device_key t2 on (t1.idfa = t2.idfa and t1.user_id = t2.user_id) where t1.app_os_type = 'iOS'
) t
GROUP BY device_key, app_channel_id, product_key, app_ver_code, src_file_day, src_file_hour;


select t1.os,t2.app_os_type
FROM intdata.kesheng_sdk_session_start t1 LEFT JOIN mscdata.dim_kesheng_sdk_app_pkg t2 ON (t1.os = t2.app_os_type AND t1.app_pkg_name = t2.app_pkg_name)
WHERE t1.src_file_day = '20170117';

AD      NULL
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      NULL
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      NULL
AD      AD
AD      AD
AD      AD
AD      AD
AD      AD
AD      NULL

select t1.*,t2.* from intdata.kesheng_sdk_session_start t1 LEFT JOIN mscdata.dim_kesheng_sdk_app_pkg t2 ON (t1.os = t2.app_os_type AND t1.app_pkg_name = t2.app_pkg_name)
WHERE t1.src_file_day = '20170117' and t2.app_os_type is null;

44234234234_0BCA315A-D3D9-404D-82B8-5D3863164125        iOS     40efb734191443f79a6be67635e9bb12                sdk2.1.com      0BCA315A-D3D9-404D-82B8-5D3863164125   360D880D-0648-45C4-8890-F760D47EA719     7.0     10.0.2  (null)  (null)  44234234234     43243243                        1484618507093   3454235435432   20170117       10       NULL    NULL    NULL    NULL    NULL    NULL
44234234234_0BCA315A-D3D9-404D-82B8-5D3863164125        iOS     40efb734191443f79a6be67635e9bb12                sdk2.1.com      0BCA315A-D3D9-404D-82B8-5D3863164125   360D880D-0648-45C4-8890-F760D47EA719     7.0     10.0.2  (null)  (null)  44234234234     43243243                        1484617823333   3454235435432   20170117       09       NULL    NULL    NULL    NULL    NULL    NULL
44234234234_0BCA315A-D3D9-404D-82B8-5D3863164125        iOS     40efb734191443f79a6be67635e9bb12                sdk2.1.com      0BCA315A-D3D9-404D-82B8-5D3863164125   360D880D-0648-45C4-8890-F760D47EA719     7.0     10.0.2  (null)  (null)  44234234234     43243243                        1484618174223   3454235435432   20170117       09       NULL    NULL    NULL    NULL    NULL    NULL
ffffffff-c01d-29af-f7e4-769d000000001484642790186_867066028889106       AD      867066028889106 460002728432955 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo               ffffffff-c01d-29af-f7e4-769d000000001484642790186       baidu                   1484643577315   1234    20170117        16     NULL     NULL    NULL    NULL    NULL    NULL
ffffffff-c01d-29af-f7e4-769d000000001484642790186_867066028889106       AD      867066028889106 460002728432955 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo               ffffffff-c01d-29af-f7e4-769d000000001484642790186       baidu                   1484642790291   1234    20170117        16     NULL     NULL    NULL    NULL    NULL    NULL
ffffffff-c01d-29af-f7e4-769d000000001484639372907_867066028889106       AD      867066028889106 460002728432955 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo               ffffffff-c01d-29af-f7e4-769d000000001484639372907       baidu                   1484639638915   1234    20170117        15     NULL     NULL    NULL    NULL    NULL    NULL
ffffffff-c01d-29af-f7e4-769d000000001484639372907_867066028889106       AD      867066028889106 460002728432955 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo               ffffffff-c01d-29af-f7e4-769d000000001484639372907       baidu                   1484639373025   1234    20170117        15     NULL     NULL    NULL    NULL    NULL    NULL
ffffffff-c01d-29af-f7e4-769d000000001484639372907_867066028889106       AD      867066028889106 460002728432955 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo               ffffffff-c01d-29af-f7e4-769d000000001484639372907       baidu                   1484639696262   1234    20170117        15     NULL     NULL    NULL    NULL    NULL    NULL
ffffffff-c01d-29af-f7e4-769d000000001484639372907_867066028889106       AD      867066028889106 460002728432955 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo               ffffffff-c01d-29af-f7e4-769d000000001484639372907       baidu                   1484639767282   1234    20170117        15     NULL     NULL    NULL    NULL    NULL    NULL
00000000-4c29-8dbd-a0f2-161a000000001484618113805_868331011992179       AD      868331011992179 310260000000000 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       17      ??SDKDemo       13547638356     00000000-4c29-8dbd-a0f2-161a000000001484618113805       baidu                   1484618114278   1234    20170117       09       NULL    NULL    NULL    NULL    NULL    NULL
00000000-4c29-8dbd-a0f2-161a000000001484618113805_868331011992179       AD      868331011992179 310260000000000 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       17      ??SDKDemo       13547638356     00000000-4c29-8dbd-a0f2-161a000000001484618113805       baidu                   1484618136966   1234    20170117       09       NULL    NULL    NULL    NULL    NULL    NULL
00000000-10e8-9023-0000-0000000000001484621259149_      AD                      com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       23      ??SDKDemo      00000000-10e8-9023-0000-0000000000001484621259149        baidu                   1484621259221   1234    20170117        10      NULL    NULL    NULL    NULL    NULL   NULL
ffffffff-c8f5-0a84-eb06-68e4000000001484650737863_28630ee79318f23       AD      28630ee79318f23 460009227542510 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo       13701373580     ffffffff-c8f5-0a84-eb06-68e4000000001484650737863       baidu                   1484650738075   1234    20170117       19       NULL    NULL    NULL    NULL    NULL    NULL
ffffffff-c8f5-0a84-eb06-68e4000000001484650737863_28630ee79318f23       AD      28630ee79318f23 460009227542510 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo       13701373580     ffffffff-c8f5-0a84-eb06-68e4000000001484650737863       baidu                   1484650738075   1234    20170117       19       NULL    NULL    NULL    NULL    NULL    NULL
ffffffff-c8f5-0a84-eb06-68e4000000001484650737863_28630ee79318f23       AD      28630ee79318f23 460009227542510 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo       13701373580     ffffffff-c8f5-0a84-eb06-68e4000000001484650737863       baidu                   1484650738075   1234    20170117       19       NULL    NULL    NULL    NULL    NULL    NULL
ffffffff-c8f5-0a84-eb06-68e4000000001484650737863_28630ee79318f23       AD      28630ee79318f23 460009227542510 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo       13701373580     ffffffff-c8f5-0a84-eb06-68e4000000001484650737863       baidu                   1484650738075   1234    20170117       19       NULL    NULL    NULL    NULL    NULL    NULL
ffffffff-c8f5-0a84-eb06-68e4000000001484650737863_28630ee79318f23       AD      28630ee79318f23 460009227542510 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo       13701373580     ffffffff-c8f5-0a84-eb06-68e4000000001484650737863       baidu                   1484650738075   1234    20170117       19       NULL    NULL    NULL    NULL    NULL    NULL
ffffffff-c8f5-0a84-eb06-68e4000000001484650737863_28630ee79318f23       AD      28630ee79318f23 460009227542510 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo       13701373580     ffffffff-c8f5-0a84-eb06-68e4000000001484650737863       baidu                   1484650738075   1234    20170117       19       NULL    NULL    NULL    NULL    NULL    NULL
ffffffff-c8f5-0a84-eb06-68e4000000001484650737863_28630ee79318f23       AD      28630ee79318f23 460009227542510 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo       13701373580     ffffffff-c8f5-0a84-eb06-68e4000000001484650737863       baidu                   1484650738075   1234    20170117       19       NULL    NULL    NULL    NULL    NULL    NULL
ffffffff-c8f5-0a84-eb06-68e4000000001484650737863_28630ee79318f23       AD      28630ee79318f23 460009227542510 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo       13701373580     ffffffff-c8f5-0a84-eb06-68e4000000001484650737863       baidu                   1484650738075   1234    20170117       19       NULL    NULL    NULL    NULL    NULL    NULL
ffffffff-c8f5-0a84-eb06-68e4000000001484650737863_28630ee79318f23       AD      28630ee79318f23 460009227542510 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo       13701373580     ffffffff-c8f5-0a84-eb06-68e4000000001484650737863       baidu                   1484650738075   1234    20170117       19       NULL    NULL    NULL    NULL    NULL    NULL
ffffffff-c8f5-0a84-eb06-68e4000000001484650737863_28630ee79318f23       AD      28630ee79318f23 460009227542510 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo       13701373580     ffffffff-c8f5-0a84-eb06-68e4000000001484650737863       baidu                   1484650738075   1234    20170117       19       NULL    NULL    NULL    NULL    NULL    NULL
ffffffff-edcd-a967-0000-0000000000001484644500464_      AD                      com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       23      ??SDKDemo      ffffffff-edcd-a967-0000-0000000000001484644500464        baidu                   1484647645903   1234    20170117        18      NULL    NULL    NULL    NULL    NULL   NULL
ffffffff-edcd-a967-0000-0000000000001484644500464_      AD                      com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       23      ??SDKDemo      ffffffff-edcd-a967-0000-0000000000001484644500464        baidu                   1484646881231   1234    20170117        17      NULL    NULL    NULL    NULL    NULL   NULL
ffffffff-edcd-a967-0000-0000000000001484644500464_      AD                      com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       23      ??SDKDemo      ffffffff-edcd-a967-0000-0000000000001484644500464        baidu                   1484644500551   1234    20170117        17      NULL    NULL    NULL    NULL    NULL   NULL
ffffffff-edcd-a967-0000-0000000000001484644500464_      AD                      com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       23      ??SDKDemo      ffffffff-edcd-a967-0000-0000000000001484644500464        baidu                   1484645115841   1234    20170117        17      NULL    NULL    NULL    NULL    NULL   NULL
ffffffff-edcd-a967-0000-0000000000001484644500464_      AD                      com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       23      ??SDKDemo      ffffffff-edcd-a967-0000-0000000000001484644500464        baidu                   1484646369044   1234    20170117        17      NULL    NULL    NULL    NULL    NULL   NULL
ffffffff-c01d-29af-f7e4-769d000000001484642309309_867066028889106       AD      867066028889106 460002728432955 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo               ffffffff-c01d-29af-f7e4-769d000000001484642309309       baidu                   1484642309422   1234    20170117        16     NULL     NULL    NULL    NULL    NULL    NULL
ffffffff-c01d-29af-f7e4-769d000000001484642309309_867066028889106       AD      867066028889106 460002728432955 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo               ffffffff-c01d-29af-f7e4-769d000000001484642309309       baidu                   1484642521284   1234    20170117        16     NULL     NULL    NULL    NULL    NULL    NULL
ffffffff-c01d-29af-f7e4-769d000000001484642309309_867066028889106       AD      867066028889106 460002728432955 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo               ffffffff-c01d-29af-f7e4-769d000000001484642309309       baidu                   1484642492501   1234    20170117        16     NULL     NULL    NULL    NULL    NULL    NULL
ffffffff-c01d-29af-f7e4-769d000000001484646367620_867066028889106       AD      867066028889106 460002728432955 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo               ffffffff-c01d-29af-f7e4-769d000000001484646367620       baidu                   1484647508012   1234    20170117        18     NULL     NULL    NULL    NULL    NULL    NULL
ffffffff-c01d-29af-f7e4-769d000000001484646367620_867066028889106       AD      867066028889106 460002728432955 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo               ffffffff-c01d-29af-f7e4-769d000000001484646367620       baidu                   1484647296786   1234    20170117        18     NULL     NULL    NULL    NULL    NULL    NULL
ffffffff-c01d-29af-f7e4-769d000000001484646367620_867066028889106       AD      867066028889106 460002728432955 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo               ffffffff-c01d-29af-f7e4-769d000000001484646367620       baidu                   1484646367720   1234    20170117        17     NULL     NULL    NULL    NULL    NULL    NULL
ffffffff-c01d-29af-f7e4-769d000000001484646367620_867066028889106       AD      867066028889106 460002728432955 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo               ffffffff-c01d-29af-f7e4-769d000000001484646367620       baidu                   1484646478457   1234    20170117        17     NULL     NULL    NULL    NULL    NULL    NULL
ffffffff-c01d-29af-f7e4-769d000000001484646367620_867066028889106       AD      867066028889106 460002728432955 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo               ffffffff-c01d-29af-f7e4-769d000000001484646367620       baidu                   1484646917151   1234    20170117        17     NULL     NULL    NULL    NULL    NULL    NULL
ffffffff-c01d-29af-f7e4-769d000000001484646367620_867066028889106       AD      867066028889106 460002728432955 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo               ffffffff-c01d-29af-f7e4-769d000000001484646367620       baidu                   1484646849711   1234    20170117        17     NULL     NULL    NULL    NULL    NULL    NULL
ffffffff-c01d-29af-f7e4-769d000000001484646367620_867066028889106       AD      867066028889106 460002728432955 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo               ffffffff-c01d-29af-f7e4-769d000000001484646367620       baidu                   1484647151131   1234    20170117        17     NULL     NULL    NULL    NULL    NULL    NULL
00000000-4c29-8dbd-a0f2-161a000000001484618139069_868331011992179       AD      868331011992179 310260000000000 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       17      ??SDKDemo       13547638356     00000000-4c29-8dbd-a0f2-161a000000001484618139069       baidu                   1484618139521   1234    20170117       09       NULL    NULL    NULL    NULL    NULL    NULL
00000000-4c29-8dbd-a0f2-161a000000001484618139069_868331011992179       AD      868331011992179 310260000000000 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       17      ??SDKDemo       13547638356     00000000-4c29-8dbd-a0f2-161a000000001484618139069       baidu                   1484618161676   1234    20170117       09       NULL    NULL    NULL    NULL    NULL    NULL




44234234234_0BCA315A-D3D9-404D-82B8-5D3863164125        iOS     40efb734191443f79a6be67635e9bb12                sdk2.1.com      0BCA315A-D3D9-404D-82B8-5D3863164125   360D880D-0648-45C4-8890-F760D47EA719     7.0     10.0.2  (null)  (null)  44234234234     43243243                        1484618507093   3454235435432   20170117       10
44234234234_0BCA315A-D3D9-404D-82B8-5D3863164125        iOS     40efb734191443f79a6be67635e9bb12                sdk2.1.com      0BCA315A-D3D9-404D-82B8-5D3863164125   784D7896-7B8B-4BE7-8319-039C714DEBC7     7.0     10.0.2  (null)  (null)  44234234234     43243243                        1484552231749   3454235435432   20170116       15
44234234234_0BCA315A-D3D9-404D-82B8-5D3863164125        iOS     40efb734191443f79a6be67635e9bb12                sdk2.1.com      0BCA315A-D3D9-404D-82B8-5D3863164125   784D7896-7B8B-4BE7-8319-039C714DEBC7     7.0     10.0.2  (null)  (null)  44234234234     43243243                        1484551963759   3454235435432   20170116       15
44234234234_0BCA315A-D3D9-404D-82B8-5D3863164125        iOS     40efb734191443f79a6be67635e9bb12                sdk2.1.com      0BCA315A-D3D9-404D-82B8-5D3863164125   784D7896-7B8B-4BE7-8319-039C714DEBC7     7.0     10.0.2  (null)  (null)  44234234234     43243243                        1484550948218   3454235435432   20170116       15