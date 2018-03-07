


--appinterface.CustomerLocation 
insert into table appinterface.CustomerLocation                 
select  concat($startday,'_',m2.mallid,'_',m2.mac),            --id
        to_mongo_date_time($startday),                         --date
		from_unixtime(unix_timestamp()),                       --createtime
		m2.mac,
		m2.latitude,
		m2.longitude,
		m2.mallid,
		map('latitude',m2.latitude,'longitude',m2.longitude),
		m3.weekday,
		m3.weekend 
  from (
        select mallid,
		       mac,
			   max(count) as num 
		  from appinterface.customer_macloccount 
		 where date=$startday                     --20170701，这里拿的是三个月789的数据
		   and latitude between 0 and 90 
		   and longitude between -180 and 180 
		 group by mallid,mac
	   ) m1
  join ( select * 
           from appinterface.customer_macloccount 
          where date=$startday                          --20170701，这里拿的是三个月789的数据
		    and latitude between 0 and 90 
		    and longitude between -180 and 180
	   ) m2 
	on (m1.mac = m2.mac and m1.num = m2.count and m1.mallid = m2.mallid)
  join location.Attendance m3 
    on (m3.date=$startday and m3.mallid=m1.mallid and m3.mac=m1.mac );        --假设 $yesterday = 20170930，则 $startday = 20170701
	
=================	
	
insert overwrite table appinterface.customer_macloccount partition (date=$startday)  --这里的分区存的是 三个月的数据
select mallid,
       mac,
       round(longitude,4) as longitude,
	   round(latitude,4) as latitude,
	   count(id) as num      	--应该是 as count  id 全部为null
  from appinterface.customer_location 
 where date = $startday                       --假设 $yesterday = 20170930，则 $startday = 20170701 
 group by mallid,mac,round(latitude,4),round(longitude,4);
 
=================

yesterday=$(date -d "$date -1 day" +%Y%m%d)
startday=$(date -d "$date -3 month" +%Y%m%d)
beforeday=$(date -d "$date -6 month" +%Y%m%d)

insert overwrite table appinterface.customer_location partition (date=$startday)     --$startday = 20170701
select mallid,id,lower(macaddress),latitude,longitude 
  from (
        select t2.latitude,
		       t2.longitude,
			   t2.macaddress,
			   t2.id,
			   t1.mallid,
			   Case When t2.latitude >= t1.TopLeftLocLatitude and t2.longitude >= t1.TopLeftLocLongitude 
			         and t2.latitude <= t1.BottomRightLocLatitude and t2.longitude <= t1.BottomRightLocLongitude 
					Then 1 Else 0 End as Flag
          from mongo.mall_location t1 
          join ( 
		        select mallid,
				       id,
					   macaddress,
					   cast(loc.latitude AS DOUBLE) as latitude,
					   cast(loc.longitude AS DOUBLE) as longitude 
				  from appinterface.appinterfacecall t2 
				 where date between $startday and $yesterday                   --假设 $date = 20171001, 则 $yesterday = 20170930 $startday = 20170701
				   and mallid in (select mallid from location.mall_source ) 
				   and macaddress != '' 
				   and macaddress != '\u0000\u0000:\u0000\u0000:\u0000\u0000:\u0000\u0000:\u0000\u0000:\u0000\u0000' 
				   and macaddress != '00:00:00:00:00:00' 
				   and length(macaddress) = 17 
				   and (loc.longitude > '0.0' or loc.latitude > '0.0')
			   ) t2 
		    on (t2.mallid = t1.mallid)
	   ) t 
 where t.flag =0; 
 
 
 
 
insert into table appinterface.appinterfacecall partition (DATE,MallID)  
select ID,                                                         --NULL
       UID,                                                        --0
	   SystemID,                                                   --1
	   SystemName,                                                 --API
	   AppDataSource,                                              --2
       AppType,                                                    --11
	   AppName,                                                    --之心城
	   ShopID,                                                     --0
	   InterfaceType,                                              --book_getrestaurantsuggest
	   InterfaceCName,                                             --NULL
	   InterfaceUrl,                                               --http://api.mallcoo.cn:8001/book/GetRestaurantSuggest?_at=11&_av=1.4.1&_c=%E4%B8%8A%E6%B5%B7&_i=9133976ff3a646faef1e5420b6084ca7&_la=0&_lo=0&_pm=iPhone&_pv=10.2&_pvt=2&_s=2&_sg=5CF71C311831DBF0&_sh=1136&_sw=640&mallid=6&mid=6&n=w&ps=10
	   InterfaceForm,                                              --
	   InterfaceQueryString,                                       --_at=11&_av=1.4.1&_c=上海&_i=9133976ff3a646faef1e5420b6084ca7&_la=0&_lo=0&_pm=iPhone&_pv=10.2&_pvt=2&_s=2&_sg=5CF71C311831DBF0&_sh=1136&_sw=640&mallid=6&mid=6&n=w&ps=10
	   InterfaceReturnCode,                                        --1
	   InterfaceReturn,                                            --{"m":1,"l":"[{\"id\":223,\"name\":\"STARBUCKS\",\"floor\":\"F2\"},{\"id\":202,\"name\":\"Starbucks\",\"floor\":\"F1\"},{\"id\":21414,\"name\":\"优格丽\",\"floor\":\"F6\"},{\"id\":231,\"name\":\"卡旺卡\",\"floor\":\"F3\"},{\"id\":306,\"name\":\"口渴了\",\"floor\":\"F6\"},{\"id\":24227,\"name\":\"合居寿司\",\"floor\":\"F7\"},{\"id\":304,\"name\":\"周贵妃\",\"floor\":\"F6\"},{\"id\":314,\"name\":\"嘻哈嘻哈\",\"floor\":\"F7\"},{\"id\":299,\"name\":\"外婆家\",\"floor\":\"F6\"},{\"id\":316,\"name\":\"大吉羊羊汤馆\",\"floor\":\"F7\"}]"}
	   IsSuccess,                                                  --true
	   IsException,                                                --false36.7.89.20
	   IP,                                                         
	   PhoneSystemVersion,                                         --10.2
	   PhoneModel,                                                 --iPhone
	   Loc,                                                        --{"longitude":"0.0","latitude":"0.0"}
	   City,                                                       --上海
	   ScreenWidth,                                                --640
	   ScreenHeight,                                               --1136
	   AppVersion,                                                 --1.4
	   IMEI,                                                       --9133976ff3a646faef1e5420b6084ca7
	   Mall,                                                       --0
	   MallLoc,                                                    --0
	   CreateTime,            --                                   --2017-01-01 10:14:17
	   MillisecondsTime,                                           --1483228800000
	   HttpMethod,                                                 --GET
	   Address,                                                    --
	   AppVersionName,                                             --NULL
	   AppVers,                                                    --2
	   MacAddress,                                                 --NULL
	   SpanTime,     --                                            --31.244400000000002
	   StartTime,    --                                            --2017-01-01 10:14:17
	   regexp_replace(to_date(CreateTime),'-',''),                 --20170101  什么不直接用 $yesterday
	   MallID                                                      --6 
  from demand.appinterfacecall 
 where date = '$yesterday';
 
 

 
 CREATE TABLE `demand.appinterfacecall`(
id                  	string       
uid                 	bigint       
systemid            	int          
systemname          	string       
appdatasource       	int          
apptype             	int          
appname             	string       
shopid              	bigint       
interfacetype       	string       
interfacecname      	string       
interfaceurl        	string       
interfaceform       	string       
interfacequerystring	string       
interfacereturncode 	int          
interfacereturn     	string       
issuccess           	boolean      
isexception         	boolean      
ip                  	string       
phonesystemversion  	string       
phonemodel          	string       
loc                 	struct<longitude:string,latitude:string>	  
city                	string      
screenwidth         	int         
screenheight        	int         
appversion          	string      
imei                	string      
mall                	bigint      
mallloc             	bigint      
createtime          	timestamp   
millisecondstime    	bigint      
httpmethod          	string      
address             	string      
appversionname      	string      
appvers             	int         
macaddress          	string      
spantime            	double      
starttime           	timestamp   
mallid              	bigint      
date                	string      
	 	 
# Partition Information	 	 
# col_name            	data_type           	comment             
	 	 
date                	string



=====重新梳理=======

mallcoo_sp.mall
mallcoo.mall      ==>|       mongo.mall  ==>| mongo.mall_location    
                                              mq.trackpageview               ==> |  mq.customer_macloccount	
                demand.appinterfacecall  ==>| appinterface.appinterfacecall  ==> |  appinterface.customer_location  ==>  appinterface.customer_macloccount ==> appinterface.CustomerLocation
                                                   mongo.mall_location                               location.attribute  ==>  location.Attendance

--判断工作日还是周末
INSERT OVERWRITE TABLE location.Attendance partition(date=${startday})
SELECT mallid,
       mac,
	   sum(x.weekday)>0,
	   sum(x.weekend)>0
FROM (
    SELECT if(IS_WEEKDAY(date)='1',1,0) weekday,
	       if(IS_WEEKDAY(date)='2',1,0) weekend,
		   mac,
		   mallid
      FROM location.attribute
     WHERE date BETWEEN ${startday} and ${yesterday} 
	   and floor=-999 
	   and hour=-999
) x
GROUP BY x.mac,x.mallid;


--店铺相关信息（如：位置信息）
insert overwrite table mongo.mall_location 
select mallid,
       cast(MapTopLeftLoc.latitude AS DOUBLE) as latitude1,
	   cast(MapTopLeftLoc.longitude AS DOUBLE) as longitude1,
	   cast(MapBottomRightLoc.latitude AS DOUBLE) as latitude2,
	   cast(MapBottomRightLoc.longitude AS DOUBLE) as longitude2 
  from mongo.mall 
 where MapTopLeftLoc is not null 
   and MapBottomRightLoc is not null;


--   
insert overwrite table appinterface.customer_location partition (date=$startday)
select mallid,
       id,
	   lower(macaddress),
	   latitude,
	   longitude
  from (
        select t2.latitude,
		       t2.longitude,
			   t2.macaddress,
			   t2.id,
			   t1.mallid,
			   Case When t2.latitude>= t1.TopLeftLocLatitude and t2.longitude >= t1.TopLeftLocLongitude 
			        and t2.latitude<=t1.BottomRightLocLatitude and t2.longitude <=t1.BottomRightLocLongitude Then 1 Else 0 End as Flag
        from mongo.mall_location t1
        join (
              select mallid,
			         id,
					 macaddress,
					 cast(loc.latitude AS DOUBLE) as latitude,
					 cast(loc.longitude AS DOUBLE) as longitude
                from appinterface.appinterfacecall t2
               where date between $startday and $yesterday 
			     and mallid in (select mallid from location.mall_source) 
				 and macaddress<>'' and macaddress !='\u0000\u0000:\u0000\u0000:\u0000\u0000:\u0000\u0000:\u0000\u0000:\u0000\u0000' 
				 and macaddress !='00:00:00:00:00:00' 
				 and length(macaddress)=17 
				 and (loc.longitude >'0.0' or loc.latitude >'0.0')
             ) t2 on t2.mallid=t1.mallid
        ) t
where t.flag =0;






add jar /plat/udf/GPSConvert.jar;
create temporary function GPSConvert as 'cn.mallcoo.udf.GPSConvert';
INSERT OVERWRITE TABLE mq.customer_macloccount PARTITION(date=$startday)
select mallid,
       uuid,
	   round(longitude,4),
	   round(latitude,4),
	   count(*)
from (
      select t2.latitude,
	         t2.longitude,
			 t2.uuid,
			 t1.mallid,
			 Case When t2.latitude >= t1.TopLeftLocLatitude and t2.longitude >= t1.TopLeftLocLongitude 
			      and t2.latitude <=t1.BottomRightLocLatitude and t2.longitude <=t1.BottomRightLocLongitude Then 1 Else 0 End as Flag
        from mongo.mall_location t1
        join (
              select mallid,
			         uuid,
					 containersource,
					 Case When containersource in (3,5) then split(GPS,',')[0] else  latitude END latitude,
					 Case When containersource in (3,5) then split(GPS,',')[1] else longitude END longitude
              from (
                    select mallid,
					       containersource,
						   uuid,
						   GPSConvert(loc['latitude'],loc['longitude']) GPS,
						   cast(loc['latitude'] AS DOUBLE) as latitude,
						   cast(loc['longitude'] AS DOUBLE) as longitude
                      from mq.trackpageview
                     where date between $startday and $yesterday 
					   and uuid!='' 
					   and uuid is not null 
					   and (loc['longitude'] >'0.0' or loc['latitude'] >'0.0')
					) m
              )t2 on t2.mallid=t1.mallid
      ) t
where t.flag =0
group by mallid,uuid,round(latitude,4),round(longitude,4);



insert overwrite table appinterface.customer_macloccount partition (date=$startday)
select mallid,
       mac id,
	   round(longitude,4) longitude,
	   round(latitude,4) latitude,
	   count(*) num
  from appinterface.customer_location
where date=$startday
group by mallid,mac,round(latitude,4),round(longitude,4)

union all

select mallid,
       uuid id,
	   round(longitude,4) longitude,
	   round(latitude,4) latitude,
	   count(*) num
  from mq.customer_macloccount
 where date=$startday
 group by mallid,uuid,round(latitude,4),round(longitude,4);

 

insert into table appinterface.CustomerLocation
select concat($startday,'_',m2.mallid,'_',m2.mac),
       to_mongo_date_time($startday),
	   from_unixtime(unix_timestamp()),
	   m2.mac,
	   m2.latitude,
	   m2.longitude,
	   m2.mallid,
	   map('latitude',m2.latitude,'longitude',m2.longitude),
	   nvl(m3.weekday,false) as weekday,
	   nvl(m3.weekend,false) as weekend 
  from (select mallid,
               mac,
			   max(count) as num 
		  from appinterface.customer_macloccount 
		 where date=$startday 
		   and latitude between 0 and 90 
		   and longitude between -180 and 180 
		 group by mallid,mac
		) m1
  join (select * 
          from appinterface.customer_macloccount 
		 where date=$startday 
		   and latitude between 0 and 90 
		   and longitude between -180 and 180
		) m2 
    on (m1.mac=m2.mac and m1.num =m2.count and m1.mallid = m2.mallid)
  left join location.Attendance m3 
    on (m3.date=$startday and m3.mallid=m1.mallid and m3.mac=m1.mac );

	
	
	
	
	