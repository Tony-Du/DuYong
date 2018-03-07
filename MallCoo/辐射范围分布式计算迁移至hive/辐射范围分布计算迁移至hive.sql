



location.Attendance
appinterface.customer_macloccount 

desc appinterface.CustomerLocation;  --客户的位置信息  外部表
id                      string                  
date                    timestamp               
createtime              timestamp               
mac                     string                  
latitude                double                  
longitude               double                  
mallid                  bigint                  
loc                     map<string,double>      
weekday                 boolean                 
weekend                 boolean                 


desc mallcoo.mall;                  --店铺的位置信息 外部表 映射 mongo项目版业务库中的mall表  isolation/commmon.sh

mallid                  bigint                                               
name                    string                                               
city                    string                                               
maptopleftloc           struct<longitude:string,latitude:string>             
mapbottomrightloc       struct<longitude:string,latitude:string>             
floorlist               array<struct<id:int,name:string,isentrance:boolean>>
status                  int  


desc mallcoo_sp.mall;               --店铺的位置信息 外部表 映射 mongo标准版业务库中的mall表  prepare/commmon.sh
OK
mallid                  bigint                                               
name                    string                                               
city                    string                                               
maptopleftloc           struct<longitude:string,latitude:string>             
mapbottomrightloc       struct<longitude:string,latitude:string>             
floorlist               array<struct<id:int,name:string,isentrance:boolean>>
status                  int                                                  
malltype                int    


desc mongo.mall
mallid                  bigint                                      
name                    string                                      
city                    string                                      
maptopleftloc           struct<longitude:string,latitude:string>                        
mapbottomrightloc       struct<longitude:string,latitude:string>                        
floorlist               array<struct<ID:int,Name:string,IsEntrance:boolean>>                        
appsettingid            bigint                                      
flag                    string                                      
status                  int 



--第一步：从数据源接入数据  --增加字段location

drop table mallcoo.mall;

CREATE EXTERNAL TABLE mallcoo.mall (
mallid                  bigint,                                              
name                    string,                                              
city                    string,
location                struct<longitude:string,latitude:string>,                                         
maptopleftloc           struct<longitude:string,latitude:string>,            
mapbottomrightloc       struct<longitude:string,latitude:string>,            
floorlist               array<struct<id:int,name:string,isentrance:boolean>>,
status                  int 
  )
STORED BY 
  'com.mongodb.hadoop.hive.MongoStorageHandler' 
WITH SERDEPROPERTIES ( 
  'mongo.columns.mapping'='{\"mallid\": \"_id\",\"Name\":\"Name\",\"City\":\"City\",
        \"Location\":\"Location\",\"Location.longitude\":\"Location.longitude\",\"Location.latitude\":\"Location.latitude\",
        \"MapTopLeftLoc\":\"MapTopLeftLoc\",\"MapTopLeftLoc.longitude\":\"MapTopLeftLoc.longitude\",\"MapTopLeftLoc.latitude\":\"MapTopLeftLoc.latitude\",
        \"MapBottomRightLoc\":\"MapBottomRightLoc\",\"MapBottomRightLoc.longitude\":\"MapBottomRightLoc.longitude\",\"MapBottomRightLoc.latitude\":\"MapBottomRightLoc.latitude\",
        \"FloorList\":\"FloorList\",\"FloorList.ID\":\"FloorList.ID\",\"FloorList.Name\":\"FloorList.Name\",\"FloorList.IsEntrance\":\"FloorList.IsEntrance\",
        \"Status\":\"Status\"}', 
  'serialization.format'='1'
  )
TBLPROPERTIES (
  'mongo.uri'='mongodb://mallcoo1:27018,mallcoo2:27018/Mallcoo.Mall'
  )
                                              

drop table mallcoo_sp.mall;
                                          
CREATE EXTERNAL TABLE mallcoo_sp.mall (
mallid                  bigint,                                              
name                    string,                                              
city                    string,
location                struct<longitude:string,latitude:string>,                  
maptopleftloc           struct<longitude:string,latitude:string>,            
mapbottomrightloc       struct<longitude:string,latitude:string>,            
floorlist               array<struct<id:int,name:string,isentrance:boolean>>,
status                  int,
malltype                int 
)
STORED BY 
  'com.mongodb.hadoop.hive.MongoStorageHandler' 
WITH SERDEPROPERTIES ( 
  'mongo.columns.mapping'='{\"mallid\": \"_id\",\"Name\":\"Name\",\"City\":\"City\",\"Location\":\"Location\",
        \"Location.longitude\":\"Location.longitude\",\"Location.latitude\":\"Location.latitude\",
        \"MapTopLeftLoc\":\"MapTopLeftLoc\",\"MapTopLeftLoc.longitude\":\"MapTopLeftLoc.longitude\",\"MapTopLeftLoc.latitude\":\"MapTopLeftLoc.latitude\",
        \"MapBottomRightLoc\":\"MapBottomRightLoc\",\"MapBottomRightLoc.longitude\":\"MapBottomRightLoc.longitude\",\"MapBottomRightLoc.latitude\":\"MapBottomRightLoc.latitude\",
        \"FloorList\":\"FloorList\",\"FloorList.isentrance\":\"FloorList.IsEntrance\",\"FloorList.ID\":\"FloorList.ID\",\"FloorList.Name\":\"FloorList.Name\",
        \"Status\":\"Status\",\"MallType\":\"MallType\"}', 
  'serialization.format'='1'
  )
TBLPROPERTIES (
  'mongo.uri'='mongodb://sp1:28001,sp2:28001/SP_Mall.Mall'
  )

  
--第二步:更改中间表的表结构，把老版和新版的数据汇总  更新脚本

drop table qmiddle.mall;

CREATE TABLE `qmiddle.mall`(
  `mallid` bigint, 
  `name` string, 
  `city` string, 
  location struct<longitude:string,latitude:string>,
  `maptopleftloc` struct<longitude:string,latitude:string>, 
  `mapbottomrightloc` struct<longitude:string,latitude:string>, 
  `floorlist` array<struct<ID:int,Name:string,IsEntrance:boolean>>, 
  `status` int, 
  `flag` string
);

insert overwrite table qmiddle.mall 
select *,'Program' as flag
  from mallcoo.mall

union 

select mallid,
       name,
       city,
       location,
       maptopleftloc,
       mapbottomrightloc,
       floorlist,
       status,
       'Standard' as flag 
  from mallcoo_sp.mall 
 where malltype = 1     --正式的，0为测试的


======================================
drop table mongo.mall;

CREATE TABLE `mongo.mall`(
  `mallid` bigint, 
  `name` string, 
  `city` string, 
  location struct<longitude:string,latitude:string>, 
  `maptopleftloc` struct<longitude:string,latitude:string>, 
  `mapbottomrightloc` struct<longitude:string,latitude:string>, 
  `floorlist` array<struct<ID:int,Name:string,IsEntrance:boolean>>, 
  `appsettingid` bigint, 
  `flag` string, 
  `status` int
  )
STORED AS orc;

INSERT OVERWRITE TABLE mongo.mall
SELECT m.mallid,
       m.name,
       m.city,
       m.location,
       m.MapTopLeftLoc,
       m.MapBottomRightLoc,
       m.FloorList,
       s.AppSettingID,
       m.flag,
       m.status
FROM qmiddle.mall m 
LEFT JOIN (
    SELECT min(id) AppSettingID,idx MallID
    FROM mongo.AppSetting0
    LATERAL VIEW explode(idlist) subview AS idx
    WHERE Category=1
    GROUP BY idx
) s ON s.MallID=m.mallid where m.status =1;



--第三步：辐射范围分布计算 并插入mongodb

select count(distinct mac) as count
from (
 select case when distance <= 1  then 1  
             when distance <= 2  then 2  
             when distance <= 3  then 3  
             when distance <= 4  then 4  
             when distance <= 5  then 5  
             when distance <= 6  then 6  
             when distance <= 7  then 7  
             when distance <= 8  then 8  
             when distance <= 10 then 10 
             when distance <= 12 then 12 
             when distance <= 14 then 14 
             when distance <= 16 then 16 
             when distance <= 30 then 30 
             else 100 
        end distance,
        mallid,
        mall_name,
        mac
  from (
        select mallid
              ,mall_name
              ,GET_MAP_DISTANCE(mall_longitude,mall_latitude,customer_longitude,customer_latitude) as distance
              ,mac
          from (
                select a.mallid
                      ,a.name as mall_name
                      ,a.location.longitude as mall_longitude
                      ,a.location.latitude as mall_latitude
                      ,b.longitude as customer_longitude
                      ,b.latitude as customer_latitude
                      ,b.mac
                  from mongo.mall a
                  left join (
				             select * 
							   from appinterface.CustomerLocation
                              where date <= '2016-01-01 00:00:00' --$startday
							) b 
                    on a.mallid = b.mallid
               )t1
       ) x               
     ) y 
group mallid, mallname,distance

        
===========================================================================

select mallid
      ,mall_name
      ,city
      --,TO_MONGO_DATE_TIME({0}) Date,
      --,from_unixtime(unix_timestamp()) CreateTime,
      ,count(case when mall_customer_distance <= 1 then mac end) as onekm_cnt
      ,count(case when mall_customer_distance <= 2 then mac end) as twokm_cnt
      ,count(case when mall_customer_distance <= 3 then mac end) as threekm_cnt
      ,count(case when mall_customer_distance <= 4 then mac end) as fourkm_cnt
      ,count(case when mall_customer_distance <= 5 then mac end) as fivekm_cnt
      ,count(case when mall_customer_distance <= 6 then mac end) as sixkm_cnt
      ,count(case when mall_customer_distance <= 7 then mac end) as sevenkm_cnt
      ,count(case when mall_customer_distance <= 8 then mac end) as eightkm_cnt
      ,count(case when mall_customer_distance <= 10 then mac end) as tenkm_cnt
      ,count(case when mall_customer_distance <= 12 then mac end) as twelvekm_cnt
      ,count(case when mall_customer_distance <= 14 then mac end) as fourteenkm_cnt
      ,count(case when mall_customer_distance <= 16 then mac end) as sixteenkm_cnt
      ,count(case when mall_customer_distance <= 30 then mac end) as thirtykm_cnt           
  from (
        select mallid
              ,mall_name
              ,city
              ,GET_MAP_DISTANCE(mall_longitude,mall_latitude,customer_longitude,customer_latitude) as mall_customer_distance
              ,mac
          from (
                select a.mallid
                      ,a.name as mall_name
                      ,a.city
                      ,a.location.longitude as mall_longitude
                      ,a.location.latitude as mall_latitude
                      ,b.longitude as customer_longitude
                      ,b.latitude as customer_latitude
                      ,b.mac
                  from mongo.mall a
                  left join appinterface.CustomerLocation b 
                    on a.mallid = b.mallid
               )t1
       )t2 
 group by mallid
         ,mall_name
         ,city

 
 desc appinterface.CustomerLocation;  --客户的位置信息  外部表
id                      string                  
date                    timestamp               
createtime              timestamp               
mac                     string                  
latitude                double                  
longitude               double                  
mallid                  bigint                  
loc                     map<string,double>      
weekday                 boolean                 
weekend                 boolean 




