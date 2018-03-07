
--测试环境外部表  
create external table test.MongodbCustomerLocDistributionCount(
id                      string    , 
mallid                  bigint    , 
mallname                string    , 
shopid                  string    , 
shopname                string    , 
mallareaid              string    , 
mallareaname            string    , 
date                    timestamp , 
createtime              timestamp , 
factor                  bigint    ,
distance                double    ,
customercount           bigint
)  
STORED BY   
  'com.mongodb.hadoop.hive.MongoStorageHandler' 
WITH SERDEPROPERTIES ( 
  'mongo.columns.mapping'='{\"id\":\"_id\",\"MallID\":\"MallID\",\"MallName\":\"MallName\",\"ShopID\":\"ShopID\",
                            \"ShopName\":\"ShopName\",\"MallAreaID\":\"MallAreaID\",\"MallAreaName\":\"MallAreaName\",
                            \"Date\":\"Date\",\"CreateTime\":\"CreateTime\",\"Factor\":\"Factor\",
                            \"CustomerCount\":\"CustomerCount\",\"Distance\":\"Distance\"}', 
  'serialization.format'='1'
  )
TBLPROPERTIES (
  'mongo.uri'='mongodb://supuser:qwe123tgb@test:27017/MallcooStatistics.CustomerLocDistributionCount?authSource=admin'
  )



--测试库中间（内部）表
create table test.CustomerLocDistributionCount_Test2(
id                      string    ,
mallid                  bigint    , 
mallname                string    , 
shopid                  string    , 
shopname                string    , 
mallareaid              string    , 
mallareaname            string    , 
date                    timestamp , 
createtime              timestamp , 
factor                  bigint    ,
distance                double    ,
customercount           bigint
)   


--  
set hive.auto.convert.join=false;

with dup as ( 
select mallid
      ,mac
      ,last_date
  from (
        select mallid 
              ,mac
              ,max(date) as last_date
              ,count(*) as cnt
          from appinterface.CustomerLocation
         where date <= '20170401'                
         group by mallid 
                 ,mac
       ) a
 where cnt > 1
),
stg1_customer_location_distribution as (
select tm.mallid
      ,tm.mac
      ,tm.longitude
      ,tm.latitude
  from appinterface.CustomerLocation tm
  left join dup t1
    on tm.mallid = t1.mallid and tm.mac = t1.mac 
 where (t1.mac is null or tm.date = t1.last_date) 
   and date <= '20170401'
),
stg2_customer_location_distribution as (
select mallid
      ,mall_name
      ,GET_MAP_DISTANCE(mall_longitude,mall_latitude,customer_longitude,customer_latitude)/1000.0 as distance
      ,mac
  from (
        select a.newmallid as mallid                       --newmallid
              ,a.name as mall_name
              ,a.location.longitude as mall_longitude
              ,a.location.latitude as mall_latitude
              ,b.longitude as customer_longitude
              ,b.latitude as customer_latitude
              ,b.mac
          from mongo.mall a 
          left join (select * from stg1_customer_location_distribution) b 
            on a.newmallid = b.mallid            --newmallid               
       ) t1
),
stg_customer_location_distribution_cnt as (
select mallid
      ,mall_name
      ,null as shopid
      ,null as shopname
      ,null as mallareaid
      ,null as mallareaname
      ,TO_MONGO_DATE_TIME('20170401') as date
      ,from_unixtime(unix_timestamp()) as CreateTime
      ,1.0 as factor
      ,null as distance
      ,count(distinct mac) as customercount  
  from stg2_customer_location_distribution
 group by mallid
         ,mall_name
         
 union all 

select mallid
      ,mall_name
      ,null as shopid
      ,null as shopname
      ,null as mallareaid
      ,null as mallareaname
      ,TO_MONGO_DATE_TIME('20170401') as date
      ,from_unixtime(unix_timestamp()) as CreateTime
      ,1.0 as factor
      ,1 as distance
      ,count(distinct mac) as customercount      
  from stg2_customer_location_distribution
 where distance > 0 
   and distance <= 1
 group by mallid
         ,mall_name

union all

select mallid
      ,mall_name
      ,null as shopid
      ,null as shopname
      ,null as mallareaid
      ,null as mallareaname
      ,TO_MONGO_DATE_TIME('20170401') as date
      ,from_unixtime(unix_timestamp()) as CreateTime
      ,1.0 as factor  
      ,2 as distance   
      ,count(distinct mac) as customercount      
  from stg2_customer_location_distribution
 where distance > 0 
   and distance <= 2
 group by mallid
         ,mall_name

 union all
 
select mallid
      ,mall_name
      ,null as shopid
      ,null as shopname
      ,null as mallareaid
      ,null as mallareaname
      ,TO_MONGO_DATE_TIME('20170401') as date
      ,from_unixtime(unix_timestamp()) as CreateTime    
      ,1.0 as factor
      ,3 as distance   
      ,count(distinct mac) as customercount       
  from stg2_customer_location_distribution
 where distance > 0 
   and distance <= 3
 group by mallid
         ,mall_name

 union all
 
select mallid
      ,mall_name
      ,null as shopid
      ,null as shopname
      ,null as mallareaid
      ,null as mallareaname
      ,TO_MONGO_DATE_TIME('20170401') as date
      ,from_unixtime(unix_timestamp()) as CreateTime     
      ,1.0 as factor
      ,4 as distance
      ,count(distinct mac) as customercount       
  from stg2_customer_location_distribution
 where distance > 0 
   and distance <= 4
 group by mallid
         ,mall_name
         
 union all

select mallid
      ,mall_name
      ,null as shopid
      ,null as shopname
      ,null as mallareaid
      ,null as mallareaname
      ,TO_MONGO_DATE_TIME('20170401') as date
      ,from_unixtime(unix_timestamp()) as CreateTime     
      ,1.0 as factor
      ,5 as distance
      ,count(distinct mac) as customercount       
  from stg2_customer_location_distribution
 where distance > 0 
   and distance <= 5
 group by mallid
         ,mall_name
         
 union all
 
select mallid
      ,mall_name
      ,null as shopid
      ,null as shopname
      ,null as mallareaid
      ,null as mallareaname
      ,TO_MONGO_DATE_TIME('20170401') as date
      ,from_unixtime(unix_timestamp()) as CreateTime     
      ,1.0 as factor
      ,6 as distance
      ,count(distinct mac) as customercount       
  from stg2_customer_location_distribution
 where distance > 0 
   and distance <= 6
 group by mallid
         ,mall_name

 union all
 
select mallid
      ,mall_name
      ,null as shopid
      ,null as shopname
      ,null as mallareaid
      ,null as mallareaname
      ,TO_MONGO_DATE_TIME('20170401') as date
      ,from_unixtime(unix_timestamp()) as CreateTime     
      ,1.0 as factor
      ,7 as distance
      ,count(distinct mac) as customercount       
  from stg2_customer_location_distribution
 where distance > 0 
   and distance <= 7
 group by mallid
         ,mall_name
         
 union all
 
select mallid
      ,mall_name
      ,null as shopid
      ,null as shopname
      ,null as mallareaid
      ,null as mallareaname
      ,TO_MONGO_DATE_TIME('20170401') as date
      ,from_unixtime(unix_timestamp()) as CreateTime     
      ,1.0 as factor
      ,8 as distance
      ,count(distinct mac) as customercount       
  from stg2_customer_location_distribution
 where distance > 0 
   and distance <= 8
 group by mallid
         ,mall_name

 union all
 
select mallid
      ,mall_name
      ,null as shopid
      ,null as shopname
      ,null as mallareaid
      ,null as mallareaname
      ,TO_MONGO_DATE_TIME('20170401') as date
      ,from_unixtime(unix_timestamp()) as CreateTime
      ,1.0 as factor
      ,10 as distance
      ,count(distinct mac) as customercount       
  from stg2_customer_location_distribution
 where distance > 0 
   and distance <= 10
 group by mallid
         ,mall_name
         
 union all
 
select mallid
      ,mall_name
      ,null as shopid
      ,null as shopname
      ,null as mallareaid
      ,null as mallareaname
      ,TO_MONGO_DATE_TIME('20170401') as date
      ,from_unixtime(unix_timestamp()) as CreateTime     
      ,1.0 as factor
      ,12 as distance
      ,count(distinct mac) as customercount       
  from stg2_customer_location_distribution
 where distance > 0 
   and distance <= 12
 group by mallid
         ,mall_name
    
 union all
 
select mallid
      ,mall_name
      ,null as shopid
      ,null as shopname
      ,null as mallareaid
      ,null as mallareaname
      ,TO_MONGO_DATE_TIME('20170401') as date
      ,from_unixtime(unix_timestamp()) as CreateTime     
      ,1.0 as factor
      ,14 as distance
      ,count(distinct mac) as customercount       
  from stg2_customer_location_distribution
 where distance > 0 
   and distance <= 14
 group by mallid
         ,mall_name
         
 union all
 
select mallid
      ,mall_name
      ,null as shopid
      ,null as shopname
      ,null as mallareaid
      ,null as mallareaname
      ,TO_MONGO_DATE_TIME('20170401') as date
      ,from_unixtime(unix_timestamp()) as CreateTime
      ,1.0 as factor  
      ,16 as distance
      ,count(distinct mac) as customercount       
  from stg2_customer_location_distribution
 where distance > 0 
   and distance <= 16
 group by mallid
         ,mall_name

 union all
 
select mallid
      ,mall_name
      ,null as shopid
      ,null as shopname
      ,null as mallareaid
      ,null as mallareaname
      ,TO_MONGO_DATE_TIME('20170401') as date
      ,from_unixtime(unix_timestamp()) as CreateTime     
      ,1.0 as factor
      ,30 as distance
      ,count(distinct mac) as customercount       
  from stg2_customer_location_distribution
 where distance > 0 
   and distance <= 30
 group by mallid
        ,mall_name
)
insert overwrite table test.CustomerLocDistributionCount_Test2
select concat(TO_HIVE_DATE('20170401'),'_',mallid,'_',nvl(distance,998)) as id
      ,mallid
      ,mall_name
      ,shopid
      ,shopname
      ,mallareaid
      ,mallareaname
      ,date
      ,createtime
      ,factor
      ,distance
      ,customercount 
  from stg_customer_location_distribution_cnt;



   