
CREATE FUNCTION GET_MAP_DISTANCE AS 'cn.mallcoo.udf.MapDistance' USING JAR 'hdfs:///udf/DateUtil-0.0.1-SNAPSHOT.jar';




CREATE EXTERNAL TABLE `eyes.MongodbRollingScreenDailyCount`(
  `id` string COMMENT 'from deserializer', 
  `mallid` bigint COMMENT 'from deserializer', 
  `mallname` string COMMENT 'from deserializer', 
  `shopid` bigint COMMENT 'from deserializer', 
  `shopname` string COMMENT 'from deserializer', 
  `mallareaid` bigint COMMENT 'from deserializer', 
  `mallareaname` string COMMENT 'from deserializer', 
  `date` timestamp COMMENT 'from deserializer', 
  `createtime` timestamp COMMENT 'from deserializer', 
  `frame` bigint COMMENT 'from deserializer', 
  `name` string COMMENT 'from deserializer', 
  `count` bigint COMMENT 'from deserializer', 
  `andriodcount` bigint COMMENT 'from deserializer', 
  `ioscount` bigint COMMENT 'from deserializer', 
  `wxcount` bigint COMMENT 'from deserializer', 
  `lappcount` bigint COMMENT 'from deserializer', 
  `avg` double COMMENT 'from deserializer')

STORED BY 
  'com.mongodb.hadoop.hive.MongoStorageHandler' 
WITH SERDEPROPERTIES ( 
  'mongo.columns.mapping'='{\"id\":\"_id\",\"MallID\":\"MallID\",\"MallName\":\"MallName\",\"ShopID\":\"ShopID\",\"ShopName\":\"ShopName\",\"MallAreaID\":\"MallAreaID\",\"MallAreaName\":\"MallAreaName\",\"Date\":\"Date\",\"CreateTime\":\"CreateTime\",\"Frame\":\"Frame\",\"Name\":\"Name\",\"Count\":\"Count\",\"AndriodCount\":\"AndriodCount\",\"IOSCount\":\"IOSCount\",\"WXCount\":\"WXCount\",\"LAPPCount\":\"LAPPCount\",\"Avg\":\"Avg\"}', 
  'serialization.format'='1')

TBLPROPERTIES (
  'mongo.uri'='mongodb://HadoopUser:dTHK93J3cf@mongoss:27017/MallcooStatistics.RollingScreenDailyCount?authSource=admin', 
)


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
 'mongo.uri'='mongodb://supuser:qwe123tgb@test:27017/MallcooStatistics.Rolling_Screen_Daily_Count_Test?authSource=admin'
)


mongodb://supuser:qwe123tgb@test:27017




eyes.MongodbCardMemberDailyCount


CREATE EXTERNAL TABLE test.MongodbCardMemberDailyCount(
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
  'mongo.uri'='mongodb://supuser:qwe123tgb@test:27017/MallcooStatistics.Card_Member_Daily_Count_Test?authSource=admin'
  )

----  
create external table test.MongodbRegisterUserDailyCount(  
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
  'mongo.uri'='mongodb://supuser:qwe123tgb@test:27017/MallcooStatistics.Register_User_Daily_Count_Test?authSource=admin'
  )
