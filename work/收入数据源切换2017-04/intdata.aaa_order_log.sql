set mapreduce.job.name=intdata.aaa_order_log_${SRC_FILE_DAY};
set hive.merge.mapredfiles=true;

WITH ods_order_temp AS
(
	SELECT
		serv_number,
		cp_id,
		product_id,
		opr_login,
		opr_time,
		order_type,
		order_chn,
		chn_id_source,
		product_name,
		product_type,
		node_id,
		program_id,
		substatus,
		rollback_flag,
		boss_id,
		user_id,
		usernum_type,
		broadcast_ip,
		session_id,
		page_id,
		pose_id,
		imei,
		split(CASE
			WHEN t.source_channel_mask = '0-00000000000' THEN concat_ws('_', '-998', '-998', substr(chn_id_source, 3),concat('000',substr(chn_id_source,1,1)))
			WHEN t.source_channel_mask = '0000-00000000000' THEN concat_ws('_', '-998', '-998', substr(chn_id_source, 6),substr(chn_id_source,1,4))
			WHEN t.source_channel_mask = '0000-000000000000000' THEN concat_ws('_', '-998', '-998', substr(chn_id_source, 6),substr(chn_id_source,1,4))
			WHEN t.source_channel_mask = '0000-00000000-00000-000000000000000' THEN concat_ws('_', substr(chn_id_source, 6, 8), substr(chn_id_source, 15, 5),substr(chn_id_source, 21),substr(chn_id_source,1,4))
			WHEN t.source_channel_mask IN ( '00000000000','000000000000000' ) THEN concat_ws('_', '-998', '-998', chn_id_source,'-998')
			WHEN t.source_channel_mask = '00000000-00000-000000000000000' THEN concat_ws('_', substr(chn_id_source, 1, 8), substr(chn_id_source, 10, 5), substr(chn_id_source, 16),'-998')
			WHEN t.source_channel_mask LIKE '0-00000000000-%' THEN concat_ws('_', '-998', '-998', substr(chn_id_source, 3, 11),concat('000',substr(chn_id_source,1,1)))
			WHEN t.source_channel_mask LIKE '0000-00000000000-%' THEN concat_ws('_', '-998', '-998', substr(chn_id_source, 6, 11),substr(chn_id_source,1,4))
			ELSE concat_ws('_', '-998', '-998', '-998', '-998')
		END,'_') AS chn_values
	FROM
	( 
		SELECT
			serv_number,
			cp_id,
			product_id,
			opr_login,
			opr_time,
			order_type,
			order_chn,
			nvl(t2.dest_chn_id, t1.chn_id_source) as chn_id_source,
			product_name,
			product_type,
			node_id,
			program_id,
			substatus,
			rollback_flag,
			boss_id,
			user_id,
			usernum_type,
			broadcast_ip,
			session_id,
			page_id,
			pose_id,
			imei,
			regexp_replace(translate(nvl(t2.dest_chn_id, t1.chn_id_source), '1234567899', '000000000'),'_','-') source_channel_mask
		FROM ods.aaa_10105_order_log_ex t1
        left outer join rptdata.dim_chn_adjust_cfg t2
        on t1.chn_id_source=t2.src_chn_id
		WHERE t1.order_type='1' and t1.src_file_day = '${SRC_FILE_DAY}'		-- order_type 为 1，在定状态
	) t
)
INSERT OVERWRITE TABLE intdata.aaa_order_log PARTITION (src_file_day='${SRC_FILE_DAY}')
SELECT
	serv_number,               --手机号码或非移动注册用户的userNum:	格式：13位数字，移动用户为86+11位手机号码；非移动用户参见【userNum分配表】
	cp_id,                     --CPID
	product_id,                --产品ID
	opr_login,                 --操作员
	opr_time,                  --操作日期
	order_type,                --订购操作类型：1订购、2取消、4停机锁定、5复机激活
	order_chn,                 --订购渠道
	chn_id_source,             --访问渠道ID
	product_name,              --产品名称
	product_type,              --产品业务类型
	node_id,                   --节点ID
	program_id,                --节目ID
	substatus,                 --订购操作子状态
	rollback_flag,             --回滚标志位：00：非回滚引起 01：回滚引起
	boss_id,                   --BOSS流水号*******
	user_id,                   --用户ID
	usernum_type,              --UserNum类型
	broadcast_ip,              --订购源地址IP
	session_id,                --大SESSION,用以标识一次完整的访问行为
	page_id,                   --页面ID
	pose_id,                   --位置ID
	imei,                      --加密IMEI
	chn_values[2] as chn_id,   --渠道id
	substr(chn_values[0], 1, 6) as version_id,	--	版本id
	chn_values[3] as chn_busi_type --临时作为platID
FROM ods_order_temp;

-------------------------------------------------

ods.aaa_10105_order_log_ex		-- AAA-10105 用户订购日志,提供用户在AAA平台的订购日志
hdfs://ns1/user/hadoop/ods/huawei/aaa/aaa-10105

8618822492205|0|2028597454|8618822492205|20170410103150|1|0|310000040040002|大象TV包月10元|0||||||||||||
8615893280190|699013|2028593174|BOSS|20170410103150|5|0|9001_10063302100|精彩15优惠套餐|0||||||||||||
8613849931773|699013|2028594089|BOSS|20170410103151|4|0|9001_10063302100|精彩15体验包|0||||||||||||

+-------------+--+
| order_type  |
+-------------+--+
| 2           |
| 4           |
| 5           |
| 1           |
+-------------+--+

desc ods.aaa_10105_order_log_ex;
OK
serv_number         	string              	                    
cp_id               	string              	                    
product_id          	string              	                    
opr_login           	string              	                    
opr_time            	string              	                    
order_type          	string              	                    
order_chn           	string              	                    
chn_id_source       	string              	                    
product_name        	string              	                    
product_type        	string              	                    
node_id             	string              	                    
program_id          	string              	                    
substatus           	string              	                    
rollback_flag       	string              	                    
boss_id             	string              	                    
user_id             	string              	                    
usernum_type        	string              	                    
broadcast_ip        	string              	                    
session_id          	string              	                    
page_id             	string              	                    
pose_id             	string              	                    
imei                	string              	                    
src_file_day        	string              	                    
src_file_hour       	string  


select * from ods.aaa_10105_order_log_ex t where t.src_file_day=20170410 and t.src_file_hour=10 limit 30;

+----------------+----------+---------------+----------------+-----------------+---------------+--------------+-------------------+-------------------+-----------------+------------+---------------+--------------+------------------+------------+------------+-----------------+-----------------+---------------+------------+------------+---------+-----------------+------------------+--+
| t.serv_number  | t.cp_id  | t.product_id  |  t.opr_login   |   t.opr_time    | t.order_type  | t.order_chn  |  t.chn_id_source  |  t.product_name   | t.product_type  | t.node_id  | t.program_id  | t.substatus  | t.rollback_flag  | t.boss_id  | t.user_id  | t.usernum_type  | t.broadcast_ip  | t.session_id  | t.page_id  | t.pose_id  | t.imei  | t.src_file_day  | t.src_file_hour  |
+----------------+----------+---------------+----------------+-----------------+---------------+--------------+-------------------+-------------------+-----------------+------------+---------------+--------------+------------------+------------+------------+-----------------+-----------------+---------------+------------+------------+---------+-----------------+------------------+--+
| 8615222821795  | 699017   | 2028593493    | BOSS           | 20170410100002  | 5             | 0            | 9001_10063302100  | 芒果TV手机视频包月10元     | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8613689195798  | 0        | 2028596750    | 8613689195798  | 20170410100002  | 1             | 0            | 900000110000000   | 人民视讯-MTV专区包月      | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8615222821795  | 699040   | 2028595110    | BOSS           | 20170410100002  | 5             | 0            | 9001_10063302100  | G客G拍包月            | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8615222821795  | 699040   | 2028595110    | BOSS           | 20170410100002  | 5             | 0            | 9001_10063302100  | G客G拍包月            | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8615222821795  | 699017   | 2028593493    | BOSS           | 20170410100002  | 5             | 0            | 9001_10063302100  | 芒果TV手机视频包月10元     | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8615222610695  | 699199   | 2028600090    | BOSS           | 20170410100003  | 4             | 0            | 9001_10063302100  | 中广创思-睛彩互动专区包月20元  | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8615222610695  | 699017   | 2028593493    | BOSS           | 20170410100003  | 4             | 0            | 9001_10063302100  | 芒果TV手机视频包月10元     | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8615030640983  | 699031   | 2028594115    | BOSS           | 20170410100003  | 5             | 0            | 9001_10063302100  | CCTV纪录片体验包        | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8613820338955  | 699014   | 2028593270    | BOSS           | 20170410100003  | 4             | 0            | 9001_10063302100  | 华数手机视频            | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8615222610695  | 699017   | 2028593493    | BOSS           | 20170410100003  | 4             | 0            | 9001_10063302100  | 芒果TV手机视频包月10元     | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8615222610695  | 699199   | 2028600090    | BOSS           | 20170410100003  | 4             | 0            | 9001_10063302100  | 中广创思-睛彩互动专区包月20元  | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8613820338955  | 699013   | 2028594089    | BOSS           | 20170410100003  | 4             | 0            | 9001_10063302100  | 精彩15体验包           | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8613512623557  | 699019   | 2028593529    | BOSS           | 20170410100004  | 2             | 0            | 9001_10063302100  | V+精选包             | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8613893525062  | 699019   | 2028593529    | BOSS           | 20170410100004  | 2             | 0            | 9001_10063302100  | V+精选包             | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8615114991533  | 0        | 2028597610    | 8615114991533  | 20170410100004  | 1             | 0            | 302000270000000   | 人民网手机视包月20元       | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8613842792176  | 699019   | 2028596512    | BOSS           | 20170410100004  | 1             | 0            | 9001_10063302100  | 和视频-V+业务15元包月     | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8615837161718  | 699013   | 2028593174    | BOSS           | 20170410100005  | 2             | 0            | 9001_10063302100  | 精彩15优惠套餐          | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8613752089501  | 699013   | 2028594075    | BOSS           | 20170410100005  | 5             | 0            | 9001_10063302100  | 央广视讯搞笑栏目体验包       | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8613752089501  | 699013   | 2028594067    | BOSS           | 20170410100005  | 5             | 0            | 9001_10063302100  | 视讯中国娱乐栏目体验包       | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8613654101256  | 0        | 2028599969    | 8613654101256  | 20170410100006  | 1             | 0            | 316200000000006   | CBCMTV包月10元       | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8618813077962  | 699013   | 2028594089    | BOSS           | 20170410100006  | 4             | 0            | 9001_10063302100  | 精彩15体验包           | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8618293122635  | 699013   | 2028593174    | BOSS           | 20170410100008  | 4             | 0            | 9001_10063302100  | 精彩15优惠套餐          | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8615101957948  | 699019   | 2028593529    | BOSS           | 20170410100008  | 5             | 0            | 9001_10063302100  | V+精选包             | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8615120510154  | 699019   | 2028593529    | BOSS           | 20170410100008  | 5             | 0            | 9001_10063302100  | V+精选包             | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8615004246902  | 699013   | 2028594089    | BOSS           | 20170410100009  | 5             | 0            | 9001_10063302100  | 精彩15体验包           | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8615004246902  | 699013   | 2028594089    | BOSS           | 20170410100009  | 5             | 0            | 9001_10063302100  | 精彩15体验包           | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8615004246902  | 699023   | 2028594099    | BOSS           | 20170410100009  | 5             | 0            | 9001_10063302100  | 央广视讯搞笑体验包         | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8615210273155  | 699201   | 2028595272    | BOSS           | 20170410100009  | 5             | 0            | 9001_10063302100  | 华数-索尼美剧包月         | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8613634896648  | 699069   | 2028597454    | BOSS           | 20170410100009  | 4             | 0            | 9001_10063302100  | 大象TV包月10元         | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
| 8615210273155  | 699201   | 2028595272    | BOSS           | 20170410100009  | 5             | 0            | 9001_10063302100  | 华数-索尼美剧包月         | 0               |            |               |              |                  |            |            |                 |                 |               |            |            |         | 20170410        | 10               |
+----------------+----------+---------------+----------------+-----------------+---------------+--------------+-------------------+-------------------+-----------------+------------+---------------+--------------+------------------+------------+------------+-----------------+-----------------+---------------+------------+------------+---------+-----------------+------------------+--+




