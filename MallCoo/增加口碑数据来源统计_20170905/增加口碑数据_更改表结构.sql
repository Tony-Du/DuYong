drop table eyes.MongodbRollingScreenDailyCount;

create external table eyes.MongodbRollingScreenDailyCount(
id           string   ,
mallid       bigint   ,
mallname     string   ,
shopid       bigint   ,
shopname     string   ,
mallareaid   bigint   ,
mallareaname string   ,
date         timestamp,
createtime   timestamp,
frame        bigint   ,
name         string   ,
count        bigint   ,
andriodcount bigint   ,
ioscount     bigint   ,
wxcount      bigint   ,
lappcount    bigint   ,
koubeicount  bigint   ,
avg          double
)
stored by 'com.mongodb.hadoop.hive.MongoStorageHandler'
with serdeproperties(

 'mongo.columns.mapping'='{\"id\":\"_id\",\"MallID\":\"MallID\",\"MallName\":\"MallName\",\"ShopID\":\"ShopID\",\"ShopName\":\"ShopName\",
                          \"MallAreaID\":\"MallAreaID\",\"MallAreaName\":\"MallAreaName\",\"Date\":\"Date\",\"CreateTime\":\"CreateTime\",\"Frame\":\"Frame\",
                          \"Name\":\"Name\",\"Count\":\"Count\",\"AndriodCount\":\"AndriodCount\",\"IOSCount\":\"IOSCount\",\"WXCount\":\"WXCount\",
                          \"LAPPCount\":\"LAPPCount\",\"KouBeiCount\":\"KouBeiCount\",\"Avg\":\"Avg\"}',
 'serialization.format'='1'
)
tblproperties(
 'mongo.uri'='mongodb://HadoopUser:dTHK93J3cf@mongoss:27017/MallcooStatistics.RollingScreenDailyCount?authSource=admin'
)

==============================================================================================================================

drop table eyes.MongodbCardMemberDailyCount;

CREATE EXTERNAL TABLE eyes.MongodbCardMemberDailyCount(
id               string ,
mallid           bigint ,
mallname         string ,
shopid           bigint ,
shopname         string ,
mallareaid       bigint ,
mallareaname     string ,
date             timestamp ,
createtime       timestamp ,
appsettingid     bigint ,
count            bigint ,
opencardcount    bigint ,
bindingcardcount bigint,
ioscount         bigint ,
androidcount     bigint ,
lappcount        bigint ,
wxcount          bigint ,
appcount         bigint ,
koubeicount      bigint ,
avg              double ,
upgradecount     bigint ,
degradecount     bigint
 )
STORED BY
  'com.mongodb.hadoop.hive.MongoStorageHandler'
WITH SERDEPROPERTIES (
  'mongo.columns.mapping'='{\"id\":\"_id\",\"MallID\":\"MallID\",\"MallName\":\"MallName\",\"ShopID\":\"ShopID\",
                            \"ShopName\":\"ShopName\",\"MallAreaID\":\"MallAreaID\",\"MallAreaName\":\"MallAreaName\",
                            \"Date\":\"Date\",\"CreateTime\":\"CreateTime\",\"AppSettingID\":\"AppSettingID\",
                            \"Count\":\"Count\",\"OpenCardCount\":\"OpenCardCount\",\"BindingCardCount\":\"BindingCardCount\",
                            \"IOSCount\":\"IOSCount\",\"AndroidCount\":\"AndroidCount\",\"LAPPCount\":\"LAPPCount\",
                            \"WXCount\":\"WXCount\",\"AppCount\":\"AppCount\",\"KouBeiCount\":\"KouBeiCount\",\"Avg\":\"Avg\",
                            \"UpgradeCount\":\"UpgradeCount\",\"DegradeCount\":\"DegradeCount\"}',
  'serialization.format'='1'
  )
TBLPROPERTIES (
  'mongo.uri'='mongodb://HadoopUser:dTHK93J3cf@mongoss:27017/MallcooStatistics.CardMemberDailyCount?authSource=admin'
  )

==============================================================================================================================

drop table eyes.MongodbRegisterUserDailyCount;

create external table eyes.MongodbRegisterUserDailyCount(
id                      string    ,
mallid                  bigint    ,
mallname                string    ,
shopid                  bigint    ,
shopname                string    ,
mallareaid              bigint    ,
mallareaname            string    ,
date                    timestamp ,
createtime              timestamp ,
appsettingid            bigint    ,
count                   bigint    ,
ioscount                bigint    ,
androidcount            bigint    ,
lappcount               bigint    ,
wxcount                 bigint    ,
koubeicount             bigint    ,
avg                     double
)
STORED BY
  'com.mongodb.hadoop.hive.MongoStorageHandler'
WITH SERDEPROPERTIES (
  'mongo.columns.mapping'='{\"id\":\"_id\",\"MallID\":\"MallID\",\"MallName\":\"MallName\",\"ShopID\":\"ShopID\",
                            \"ShopName\":\"ShopName\",\"MallAreaID\":\"MallAreaID\",\"MallAreaName\":\"MallAreaName\",
                            \"Date\":\"Date\",\"CreateTime\":\"CreateTime\",\"AppSettingID\":\"AppSettingID\",
                            \"Count\":\"Count\",\"IOSCount\":\"IOSCount\",\"AndroidCount\":\"AndroidCount\",
                            \"LAPPCount\":\"LAPPCount\",\"WXCount\":\"WXCount\",\"KouBeiCount\":\"KouBeiCount\",\"Avg\":\"Avg\"}',
  'serialization.format'='1'
  )
TBLPROPERTIES (
  'mongo.uri'='mongodb://HadoopUser:dTHK93J3cf@mongoss:27017/MallcooStatistics.RegisterUserDailyCount?authSource=admin'
  )


