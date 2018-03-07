--qmiddle.mall 需要修改表结构 

drop table qmiddle.mall;

CREATE TABLE `qmiddle.mall`(
  `mallid` bigint,
   newmallid bigint,  
  `name` string, 
  `city` string, 
  `location` struct<longitude:string,latitude:string>, 
  `maptopleftloc` struct<longitude:string,latitude:string>, 
  `mapbottomrightloc` struct<longitude:string,latitude:string>, 
  `floorlist` array<struct<ID:int,Name:string,IsEntrance:boolean>>, 
  `status` int, 
  `flag` string
  );

--mongo.mall 需要修改表结构

drop table mongo.mall;

CREATE TABLE `mongo.mall`(
  `mallid` bigint,
   newmallid bigint,  
  `name` string, 
  `city` string, 
  `location` struct<longitude:string,latitude:string>, 
  `maptopleftloc` struct<longitude:string,latitude:string>, 
  `mapbottomrightloc` struct<longitude:string,latitude:string>, 
  `floorlist` array<struct<ID:int,Name:string,IsEntrance:boolean>>, 
  `appsettingid` bigint, 
  `flag` string, 
  `status` int
)
STORED AS orc;


--建表
drop table location.MongodbCustomerLocDistributionCount;
create external table location.MongodbCustomerLocDistributionCount(
id                      string    , 
mallid                  bigint    , 
mallname                string    , 
shopid                  string    , 
shopname                string    , 
mallareaid              string    , 
mallareaname            string    , 
date                    timestamp , 
createtime              timestamp , 
factor                  double    ,
distance                bigint    ,
count                   bigint
)  
STORED BY 
  'com.mongodb.hadoop.hive.MongoStorageHandler' 
WITH SERDEPROPERTIES ( 
  'mongo.columns.mapping'='{\"id\":\"_id\",\"MallID\":\"MallID\",\"MallName\":\"MallName\",\"ShopID\":\"ShopID\",
                            \"ShopName\":\"ShopName\",\"MallAreaID\":\"MallAreaID\",\"MallAreaName\":\"MallAreaName\",
                            \"Date\":\"Date\",\"CreateTime\":\"CreateTime\",\"Factor\":\"Factor\",
                            \"Distance\":\"Distance\",\"Count\":\"Count\"}', 
  'serialization.format'='1'
  )
TBLPROPERTIES (
  'mongo.uri'='mongodb://HadoopUser:dTHK93J3cf@mongoss:27017/MallcooStatistics.CustomerLocDistributionCount?authSource=admin'
  );

  
  
-- 其中一个中间的外部表 需要 变为 内部表   

appinterface.CustomerLocation

最终结论 不进行变更，暂时直接从外部表中拿数据