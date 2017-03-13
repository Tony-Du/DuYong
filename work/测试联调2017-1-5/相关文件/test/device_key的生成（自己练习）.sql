-- 大体的思路 --
-- 1.源表 intdata.kesheng_sdk_session_start
-- 1.有四种判定新用户的方式 (AD)imei+user_id/imei+imsi/(IOS)idfa/idfa+user_id
-- 2.分别给每一种方式的新用户创建唯一标识（device_key）

ffffffff-bc67-753c-0000-0000000000001482297355515_      AD                      com.cmvideo.analitics.sdk       NULL    NULL    2.2.2_debug_new2        23      DataUploadSDK           ffffffff-bc67-753c-0000-0000000000001482297355515       baidu                   1482297453186   1234    20161221        13
ffffffff-bc67-753c-0000-0000000000001482211851601_      AD                      com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       23      ??SDKDemo      ffffffff-bc67-753c-0000-0000000000001482211851601        baidu                   1482298359511   1234    20161221        13
00000000-3d84-8837-0000-0000000000001481778343911_      AD                      com.cmvideo.analitics.sdk       NULL    NULL    2.2.2_debug_new2        24      DataUploadSDK           00000000-3d84-8837-0000-0000000000001481778343911       baidu                   1481778344005   1234    20161221        13
00000000-3d84-8837-0000-0000000000001481778343911_      AD                      com.cmvideo.analitics.sdk       NULL    NULL    2.2.2_debug_new2        24      DataUploadSDK           00000000-3d84-8837-0000-0000000000001481778343911       baidu                   1481778485254   1234    20161221        13
ffffffff-bc67-753c-0000-0000000000001482211851601_      AD                      com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       23      ??SDKDemo      ffffffff-bc67-753c-0000-0000000000001482211851601        baidu                   1482211851707   1234    20161221        13
00000000-3d84-8837-0000-0000000000001481778686648_      AD                      com.cmvideo.analitics.sdk       NULL    NULL    2.2.2_debug_new2        24      DataUploadSDK           00000000-3d84-8837-0000-0000000000001481778686648       baidu                   1481778686740   1234    20161221        13
00000000-3d84-8837-0000-0000000000001481778887261_      AD                      com.cmvideo.analitics.sdk       NULL    NULL    2.2.2_debug_new2        24      DataUploadSDK           00000000-3d84-8837-0000-0000000000001481778887261       baidu                   1481778887332   1234    20161221        13
00000000-6fcc-0628-6c61-24c8000000001482124170111_99000523195098        AD      99000523195098          com.cmvideo.analitics.sdk       NULL    NULL    2.2.2_debug_new19       DataUploadSDK           00000000-6fcc-0628-6c61-24c8000000001482124170111       baidu                   1482124170279   1234    20161221        13
00000000-3d84-8837-0000-0000000000001481691718393_      AD                      com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       24      ??SDKDemo      00000000-3d84-8837-0000-0000000000001481691718393        baidu                   1481778157457   1234    20161221        13
00000000-3d84-8837-0000-0000000000001481691718393_      AD                      com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       24      ??SDKDemo      00000000-3d84-8837-0000-0000000000001481691718393        baidu                   1481691718467   1234    20161221        13
00000000-183c-e20f-6379-ee1c000000001481873214698_867068021200877       AD      867068021200877 460077101332190 com.cmcc.migutvtwo      NULL    NULL    3.2.0   22     ????             00000000-183c-e20f-6379-ee1c000000001481873214698       DeveloperDebug                  1482266468866   593FCFDA9AD319C22FF4A6EAE62BD3D4        201612213
00000000-6fcc-0628-6c61-24c8000000001482283683749_99000523195098        AD      99000523195098          com.cmvideo.analitics.sdk       NULL    NULL    2.2.2_debug_new19       DataUploadSDK           00000000-6fcc-0628-6c61-24c8000000001482283683749       baidu                   1482124113285   1234    20161221        13
00000000-6fcc-0628-6c61-24c8000000001482283683749_99000523195098        AD      99000523195098          com.cmvideo.analitics.sdk       NULL    NULL    2.2.2_debug_new19       DataUploadSDK           00000000-6fcc-0628-6c61-24c8000000001482283683749       baidu                   1482124092725   1234    20161221        13
ffffffff-bc67-753c-0000-0000000000001481260178069_19527 				AD      19527   19527   com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       23      ??SDKDemo      19527    ffffffff-bc67-753c-0000-0000000000001481260178069       baidu   1007    1008    1482211305318   1234    20161221        13
ffffffff-8129-7940-1062-f2ea000000001481198970652_102a873842ffb43       AD      102a873842ffb43 460001295697997 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo       13798054405     ffffffff-8129-7940-1062-f2ea000000001481198970652       baidu                   1481198970857   1234    20161221       13
ffffffff-8129-7940-1062-f2ea000000001481198970652_102a873842ffb43       AD      102a873842ffb43 460001295697997 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo       13798054405     ffffffff-8129-7940-1062-f2ea000000001481198970652       baidu                   1481198970857   1234    20161221       13
ffffffff-8129-7940-1062-f2ea000000001481198970652_102a873842ffb43       AD      102a873842ffb43 460001295697997 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo       13798054405     ffffffff-8129-7940-1062-f2ea000000001481198970652       baidu                   1481198970857   1234    20161221       13
ffffffff-8129-7940-bdaf-210b000000001481198947653_9addfa006be15e3       AD      9addfa006be15e3 460001145324756 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo       13748042178     ffffffff-8129-7940-bdaf-210b000000001481198947653       baidu                   1481198947794   1234    20161221       13
00000000-4c29-8dbd-a0f2-161a000000001482299000043_868331011992179       AD      868331011992179 310260000000000 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       17      ??SDKDemo       13547638356     00000000-4c29-8dbd-a0f2-161a000000001482299000043       baidu                   1482299022487   1234    20161221       13
00000000-4c29-8dbd-a0f2-161a000000001482299000043_868331011992179       AD      868331011992179 310260000000000 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       17      ??SDKDemo       13547638356     00000000-4c29-8dbd-a0f2-161a000000001482299000043       baidu                   1482299000539   1234    20161221       13
ffffffff-8129-7940-bdaf-210b000000001481198947653_9addfa006be15e3       AD      9addfa006be15e3 460001145324756 com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo       19      ??SDKDemo       13748042178     ffffffff-8129-7940-bdaf-210b000000001481198947653       baidu                   1481198947794   1234    20161221       13
ffffffff-9f06-52f5-b9a6-e11a00000000749195394_864593021358626   		AD      864593021358626         com.cmcc.migutvtwo      NULL    NULL    3.2.0   19      ????           ffffffff-9f06-52f5-b9a6-e11a00000000749195394    DeveloperDebug                  1482297775667   63A89B1B6F44C7F8B8D7008C466B3256        20161221        13
ffffffff-b874-29f4-030f-6292000000001482121956114_359596060142365       AD      359596060142365         com.miguvideo.datauploadsdk_1   NULL    NULL    2.1.3demo      22       ??SDKDemo               ffffffff-b874-29f4-030f-6292000000001482121956114       baidu   007     008     1482301296642   1234    20161221        14
00000000-3d84-8837-0000-0000000000001481778887261_      AD                      com.cmvideo.analitics.sdk       NULL    NULL    2.2.2_debug_new2        24      DataUploadSDK           00000000-3d84-8837-0000-0000000000001481778887261       baidu                   1481782495258   1234    20161221        14


insert overwrite table stg.kesheng_sdk_active_device_hourly_01
select a.app_os_tpye
	,a.imei
	,a.user_id
	,a.idfa
	,a.imsi
	,a.phone_number
	,a.src_file_day
	,a.src_file_hour
from int.kesheng_sdk_session_start_cpa_v a 
left join mscdata.cpa_phone_number_blacklist b
on (a.phone_number = b.phone_number)
where a.src_file_day = '${EXTRACT_DAY}';
and a.src_file_hour = '${EXTRACT_HOUR}' 
and (a.app_os_tpye = 'AD' and length(a.imei) in (14,15)
		and (a.user_id = '-998' and length(a.imsi) = 15 and a.imsi like '460%' 
			or a.user_id <> '-998' and b.phone_number is null
			)
	or a.app_os_tpye = 'ios' and a.idfa <> '-998'
		and (a.user_id = '-998' 
			or a.user_id <> '-998' and b.phone_number is null
			)
	)
	
-- ================================================================================ --
