（1）会员表
mallcoo.Mallcard (mongo_ex) |--> mongo.mallcard0 |--> customer.mallcard
mallcoo_sp.mallcard (mongo_ex) |--> crm.mallcard |

Mallcoo-hive (default)> desc  customer.mallcard;
OK
id                  	string              	对应mongo表_id         
mallid              	string              	商场ID                
mallname            	string              	商场名                 
cardno              	string              	会员卡号                
uid                 	string              	用户ID         --       
username            	string              	用户名                 
idcard              	string              	身份证                 
mobile              	string              	手机号                 
cardtype            	string              	会员卡类型               
cardtypeid          	string              	会员卡类型ID             
createtime          	timestamp           	创建时间        --
apptype             	int                 	应用ID                
creator             	string              	创建者                 
datasource          	int                 	数据来源                
bonus               	double              	积分                  
cardid              	string              	会员卡ID               
city                	string              	城市                  
address             	string              	地址                  
provincecityarea    	string              	省市区域代码              
gender              	string              	性别                  
birthday            	string              	生日                  
updatetime          	string              	更新时间                
flage               	string              	标准版：Standard，项目版：Program
applytime           	timestamp



datasource=5 -- 微信
datasource=7 -- 口碑

维度：createtime, mallid  
指标：uid 去重


（2）消费积分表
SP_UserCenter.ConsBonusHistory |--> crm.consbonushistory

Mallcoo-hive (default)> desc crm.consbonushistory;
OK
id                  	string              	ID                  
mallid              	bigint              	商场ID                
mallname            	string              	商场名称                
shopid              	bigint              	商铺ID                
shopname            	string              	商铺名称                
cardno              	string              	卡号                  
userid              	bigint              	用户ID                
bonus               	double              	积分                  
mobile              	string              	手机号                 
bonusaction         	int                 	积分动作                
createtime          	timestamp           	创建时间                
tradetime           	timestamp           	交易时间                
amount              	double              	金额                  
tradeid             	string              	                    
tradeserialno       	string              	                    
consbonusorigin     	int

用 userid 关联 mallcard --> 会员积分消费
维度：createtime, mallid  
指标：uid 去重



（3）积分
SP_UserCenter.BonusHistoryUser  crm.BonusHistoryUser


Mallcoo-hive (default)> desc crm.BonusHistoryUser;
OK
id                  	string              	                    
mallid              	string              	                    
shopid              	string              	                    
userid              	string              	                    
mallcardid          	string              	                    
cardno              	string              	                    
opencardno          	string              	                    
mobile              	string              	                    
bonustime           	timestamp           	                    
refid               	string              	                    
bonusaction         	int                 	                    
bonuschannel        	int                 	                    
bonuschannelname    	string              	                    
appchannelid        	string              	                    
appchannelname      	string              	                    
actionname          	string              	                    
bonus               	double              	                    
usablebonus         	double              	                    
realbonus           	double              	                    
isreturnbonus       	boolean             	                    
status              	int                 	                    
desc                	string              	                    
other               	string              	                    
calculatetime       	timestamp           	                    
createtime          	timestamp           	                    
creatorid           	string              	                    
creator             	string              	                    
updatetime          	timestamp           	                    
updatorid           	string



--微信粉丝 

mq.wx_appcount 

mallid              	bigint              	                    
new_user            	bigint              	                    
cancel_user         	bigint              	                    
cumulate_user       	bigint              	                    
date                	string              	                    
	 	 
# Partition Information	 	 
# col_name            	data_type           	comment             
	 	 
date                	string


--微信粉丝 图文
SELECT
	MallID,
	sum(int_page_read_user) IntUser,  --图文页（点击群发图文卡片进入的页面）的阅读人数
	sum(int_page_read_count) IntCount, --图文页的阅读次数
	sum(ori_page_read_user) OriUser,
	sum(ori_page_read_count) OriCount,
	sum(share_user) ShareUser,
	sum(share_count) ShareCount,
	sum(add_to_fav_user) AddUser,
	sum(add_to_fav_count) AddCount
FROM mq.WXGraph
WHERE Type=1 and date={0}
GROUP BY MallID


Mallcoo-hive (default)> desc mq.WXGraph;
OK
ref_date            	string              	from deserializer   
user_source         	int                 	from deserializer   
msgid               	string              	from deserializer   
title               	string              	from deserializer   
int_page_read_user  	bigint              	from deserializer   
int_page_read_count 	bigint              	from deserializer   
ori_page_read_user  	bigint              	from deserializer   
ori_page_read_count 	bigint              	from deserializer   
share_user          	bigint              	from deserializer   
share_count         	bigint              	from deserializer   
add_to_fav_user     	bigint              	from deserializer   
add_to_fav_count    	bigint              	from deserializer   
mallid              	bigint              	from deserializer   
date                	string              	                    
type                	int                 	                    
	 	 
# Partition Information	 	 
# col_name            	data_type           	comment             
	 	 
date                	string              	                    
type                	int                 	


CREATE EXTERNAL TABLE `eyes.MemberBonusUseDaily`(
  `id` string COMMENT 'from deserializer', 
  `mallid` bigint COMMENT 'from deserializer', 
  `mallname` string COMMENT 'from deserializer', 
  `date` timestamp COMMENT 'from deserializer', 
  `createtime` timestamp COMMENT 'from deserializer', 
  `totalbonus` double COMMENT 'from deserializer', 
  `querycount` bigint COMMENT 'from deserializer', 
  `giftbonus` double COMMENT 'from deserializer', 
  `giftnumber` bigint COMMENT 'from deserializer', 
  `giftcount` bigint COMMENT 'from deserializer', 
  `giftverification` bigint COMMENT 'from deserializer', 
  `paybonus` double COMMENT 'from deserializer', 
  `paynumber` bigint COMMENT 'from deserializer', 
  `paycount` double COMMENT 'from deserializer')

STORED BY 
  'com.mongodb.hadoop.hive.MongoStorageHandler' 
WITH SERDEPROPERTIES ( 
  'mongo.columns.mapping'='{\"id\":\"_id\",\"MallID\":\"MallID\",\"MallName\":\"MallName\",\"Date\":\"Date\",\"CreateTime\":\"CreateTime\",\"TotalBonus\":\"TotalBonus\",\"QueryCount\":\"QueryCount\",\"GiftBonus\":\"GiftBonus\",\"GiftNumber\":\"GiftNumber\",\"GiftCount\":\"GiftCount\",\"GiftVerification\":\"GiftVerification\",\"PayBonus\":\"PayBonus\",\"PayNumber\":\"PayNumber\",\"PayCount\":\"PayCount\",\"PhotoBonus\":\"PhotoBonus\"}', 
  'serialization.format'='1')
LOCATION
  'hdfs://hadoop000:9000/user/root/hive/eyes.db/memberbonususedaily'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'mongo.uri'='mongodb://HadoopUser:dTHK93J3cf@mongoss:27017/MallcooStatistics.MemberBonusUseDaily?authSource=admin'
)


_id                  	         唯一标识
MallID              	         商场ID
Date                	         数据时间
CreateTime          	         创建时间
MemberUserCount      	         会员用户数
WechatMemberUserCount	         微信会员用户数
KoubeiMemberUserCount	         口碑会员用户数
MemberConsumActiveCount  	     会员消费活跃数
MemberConsumAmount	             会员消费金额
WechatMemberConsumActiveCount	 微信会员消费活跃数
WechatMemberConsumAmount	     微信会员消费金额
KoubeiMemberConsumActiveCount	 口碑会员消费活跃数
KoubeiMemberConsumAmount	     口碑会员消费金额
BonusMemberActiveCount	         积分会员活跃数
WechatGraphUserCount	         微信图文阅读人数
WechatFansCount     	         微信粉丝数



UE 
1.3会员洞察:          对应mongo表中的字段：
会员增长              MemberUserCount
会员消费活跃数        MemberConsumActiveCount
会员消费金额          MemberConsumAmount
口碑会员新增          KoubeiMemberUserCount
口碑会员活跃数        KoubeiMemberConsumActiveCount
口碑会员消费金额      KoubeiMemberConsumAmount
微信会员新增          WechatMemberUserCount
微信会员活跃数        WechatMemberConsumActiveCount
微信会员消费金额      WechatMemberConsumAmount

3.2.1活动ROI:         对应mongo表中的字段：
会员增量              MemberUserCount
积分活跃会员量        MemberConsumActiveCount
会员消费额            MemberConsumAmount


3.2.4用户影响         对应mongo表中的字段：      
粉丝增长              WechatFansCount
粉丝活跃              WechatGraphUserCount
会员增长              MemberUserCount
会员活跃数            MemberConsumActiveCount
口碑会员新增          KoubeiMemberUserCount
微信会员新增          WechatMemberUserCount
口碑会员活跃数        KoubeiMemberConsumActiveCount
微信会员活跃数        WechatMemberConsumActiveCount


