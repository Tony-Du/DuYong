
create database koubei;

create external table koubei.MongodbInsightMemberDailyCount (  --未创建
id string,
mallid bigint,
date timestamp,
createtime timestamp,
member_usr_cnt bigint,
wechat_member_usr_cnt bigint,
koubei_member_usr_cnt bigint,
member_consum_active_cnt bigint,
member_consum_amount double,
wechat_member_consum_active_cnt bigint,
wechat_member_consum_amount double,
koubei_member_consum_active_cnt bigint,
koubei_member_consum_amount double,
bonus_member_active_cnt bigint,
wechat_graph_usr_cnt bigint,
wechat_fans_cnt bigint
)
stored by 'com.mongodb.hadoop.hive.MongoStorageHandler'
with serdeproperties (
'mongo.columns.mapping'='{\"id\":\"_id\",\"mallid\":\"MallID\",\"date\":\"Date\",\"createtime\":\"CreateTime\",
                          \"member_usr_cnt\":\"MemberUserCount\",\"wechat_member_usr_cnt\":\"WechatMemberUserCount\",
                          \"koubei_member_usr_cnt\":\"KoubeiMemberUserCount\",\"member_consum_active_cnt\":\"MemberConsumActiveCount\",
                          \"member_consum_amount\":\"MemberConsumAmount\",\"wechat_member_consum_active_cnt\":\"WechatMemberConsumActiveCount\",
                          \"wechat_member_consum_amount\":\"WechatMemberConsumAmount\",\"koubei_member_consum_active_cnt\":\"KoubeiMemberConsumActiveCount\",
                          \"koubei_member_consum_amount\":\"KoubeiMemberConsumAmount\",\"bonus_member_active_cnt\":\"BonusMemberActiveCount\",
                          \"wechat_graph_usr_cnt\":\"WechatGraphUserCount\",\"wechat_fans_cnt\":\"WechatFansCount\",}',
'serialization.format'='1'
)
tblproperties (
'mongo.uri'='mongodb://HadoopUser:dTHK93J3cf@mongoss:27017/MallcooStatistics.InsightMemberDailyCount?authSource=admin'
)

---------------------------------------------------

create 'InsightMemberDailyCount', 'data'

create external table koubei.HbaseInsightMemberDailyCount(
id string,
mallid bigint,
date string,
member_usr_cnt bigint,
wechat_member_usr_cnt bigint,
koubei_member_usr_cnt bigint,
member_consum_active_cnt bigint,
member_consum_amount double,
wechat_member_consum_active_cnt bigint,
wechat_member_consum_amount double,
koubei_member_consum_active_cnt bigint,
koubei_member_consum_amount double,
bonus_member_active_cnt bigint,
wechat_graph_usr_cnt bigint,
wechat_fans_cnt bigint
)
stored by'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping" = ":key,data:mallid,
                                                     data:date,
                                                     data:member_usr_cnt,
                                                     data:wechat_member_usr_cnt,
                                                     data:koubei_member_usr_cnt,
                                                     data:member_consum_active_cnt,
                                                     data:member_consum_amount,
                                                     data:wechat_member_consum_active_cnt,
                                                     data:wechat_member_consum_amount,
                                                     data:koubei_member_consum_active_cnt,
                                                     data:koubei_member_consum_amount,
                                                     data:bonus_member_active_cnt,
                                                     data:wechat_graph_usr_cnt,
                                                     data:wechat_fans_cnt")
tblproperties("hbase.table.name" = "InsightMemberDailyCount");


---------------------------------------------------

--会员增长
--口碑会员新增
--微信会员新增
create external table koubei.HbaseMember(
id string,
mallid bigint,
date string,
member_usr_cnt bigint,
wechat_member_usr_cnt bigint,
koubei_member_usr_cnt bigint
)
stored by'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping" = ":key,data:mallid,
                                                     data:date,
                                                     data:member_usr_cnt,
                                                     data:wechat_member_usr_cnt,
                                                     data:koubei_member_usr_cnt")
tblproperties("hbase.table.name" = "InsightMemberDailyCount");


---------------------------------------------------

--会员消费

--会员消费活跃数
--会员消费金额

--口碑会员消费活跃数
--口碑会员消费金额

--微信会员活跃数
--微信会员消费金额

create external table koubei.HbaseMemberConsume(
id string,
mallid bigint,
date string,
member_consum_active_cnt bigint,
member_consum_amount double,
wechat_member_consum_active_cnt bigint,
wechat_member_consum_amount double,
koubei_member_consum_active_cnt bigint,
koubei_member_consum_amount double
)
stored by'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping" = ":key,data:mallid,
                                                     data:date,
                                                     data:member_consum_active_cnt,
                                                     data:member_consum_amount,
                                                     data:wechat_member_consum_active_cnt,
                                                     data:wechat_member_consum_amount,
                                                     data:koubei_member_consum_active_cnt,
                                                     data:koubei_member_consum_amount")
tblproperties("hbase.table.name" = "InsightMemberDailyCount");


---------------------------------------------------

--积分活跃会员量
create external table koubei.HbaseMemberBonus(
id string,
mallid bigint,
date string,
bonus_member_active_cnt bigint
)
stored by'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping" = ":key,data:mallid,
                                                     data:date,
                                                     data:bonus_member_active_cnt")
tblproperties("hbase.table.name" = "InsightMemberDailyCount");


---------------------------------------------------

--微信图文阅读人数
create external table koubei.HbaseWechatGraph(
id string,
mallid bigint,
date string,
wechat_graph_usr_cnt bigint
)
stored by'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping" = ":key,data:mallid,
                                                     data:date,
                                                     data:wechat_graph_usr_cnt")
tblproperties("hbase.table.name" = "InsightMemberDailyCount");


---------------------------------------------------

--微信粉丝
create external table koubei.HbaseWechatFans (
id string,
mallid bigint,
date string,
wechat_fans_cnt bigint
)
stored by'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping" = ":key,data:mallid,
                                                     data:date,
                                                     data:wechat_fans_cnt")
tblproperties("hbase.table.name" = "InsightMemberDailyCount");







