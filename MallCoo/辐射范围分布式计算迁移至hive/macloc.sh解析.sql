
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



insert overwrite table mongo.mall_location 
select mallid,
       cast(MapTopLeftLoc.latitude AS DOUBLE) as latitude1,
	   cast(MapTopLeftLoc.longitude AS DOUBLE) as longitude1,
	   cast(MapBottomRightLoc.latitude AS DOUBLE) as latitude2,
	   cast(MapBottomRightLoc.longitude AS DOUBLE) as longitude2 
  from mongo.mall
 where MapTopLeftLoc is not null 
   and MapBottomRightLoc is not null;


     
insert overwrite table appinterface.customer_location partition (date=$startday)
select mallid,id,lower(macaddress),latitude,longitude 
  from (
        select t2.latitude,
		       t2.longitude,
			   t2.macaddress,
			   t2.id,
			   t1.mallid,
			   Case When t2.latitude >= t1.TopLeftLocLatitude and t2.longitude >= t1.TopLeftLocLongitude 
			         and t2.latitude <=t1.BottomRightLocLatitude and t2.longitude <=t1.BottomRightLocLongitude 
					Then 1 Else 0 End as Flag
          from mongo.mall_location t1
          join ( 
		        select mallid,
				       id,
					   macaddress,
					   cast(loc.latitude AS DOUBLE) as latitude,
					   cast(loc.longitude AS DOUBLE) as longitude 
				  from appinterface.appinterfacecall t2 
				 where date between $startday and $yesterday 
				   and mallid in (select mallid from location.mall_source ) 
				   and macaddress !='' 
				   and macaddress !='\u0000\u0000:\u0000\u0000:\u0000\u0000:\u0000\u0000:\u0000\u0000:\u0000\u0000' 
				   and macaddress !='00:00:00:00:00:00' 
				   and length(macaddress)=17 
				   and (loc.longitude >'0.0' or loc.latitude >'0.0')
			   ) t2 
		    on (t2.mallid=t1.mallid)
	   ) t 
 where t.flag =0;

 
 
insert overwrite table appinterface.customer_macloccount partition (date=$startday)
select mallid,
       mac,
       round(longitude,4) as longitude,
	   round(latitude,4) as latitude,
	   count(id) as num 
  from appinterface.customer_location 
 where date=$startday 
 group by mallid,mac,round(latitude,4),round(longitude,4);



--appinterface.CustomerLocation 
insert into table appinterface.CustomerLocation
select  concat($startday,'_',m2.mallid,'_',m2.mac),
        to_mongo_date_time($startday),
		from_unixtime(unix_timestamp()),
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
		 where date=$startday 
		   and latitude between 0 and 90 
		   and longitude between -180 and 180 
		 group by mallid,mac
	   ) m1
  join ( select * 
           from appinterface.customer_macloccount 
          where date=$startday 
		    and latitude between 0 and 90 
		    and longitude between -180 and 180
	   ) m2 
	on (m1.mac=m2.mac and m1.num =m2.count and m1.mallid = m2.mallid)
  join location.Attendance m3 
    on (m3.date=$startday and m3.mallid=m1.mallid and m3.mac=m1.mac );


add jar /plat/udf/GPSConvert.jar;
create temporary  function  GPSConvert as 'cn.mallcoo.udf.GPSConvert';
insert overwrite table mq.customer_macloccount partition (date=$startday)
select mallid,
       uuid,
	   round(longitude,4),
	   round(latitude,4),
	   count(*) 
  from (select t2.latitude,
               t2.longitude,
			   t2.uuid,
			   t1.mallid,
			   Case When t2.latitude >= t1.TopLeftLocLatitude and t2.longitude >= t1.TopLeftLocLongitude 
			        and t2.latitude <=t1.BottomRightLocLatitude and t2.longitude <=t1.BottomRightLocLongitude 
					Then 1 Else 0 End as Flag
          from mongo.mall_location t1
          join
              (select mallid,uuid,containersource,
			          Case When containersource in (3,5) then split(GPS,',')[0] else latitude END latitude,
					  Case When containersource in (3,5) then split(GPS,',')[1] else  longitude END longitude 
				from (select mallid,containersource,uuid,
				             GPSConvert(loc['latitude'],loc['longitude']) GPS,
							 cast(loc['latitude'] AS DOUBLE) as latitude,
							 cast(loc['longitude'] AS DOUBLE) as longitude 
					    from mq.trackpageview 
					   where date between $startday and $yesterday  
					     and uuid!='' and uuid is not null 
						 and (loc['longitude'] >'0.0' or loc['latitude'] >'0.0')
					 ) m 
			  )t2 
		   on (t2.mallid=t1.mallid)
	    ) t 
  where t.flag =0
  group by mallid,uuid,round(latitude,4),round(longitude,4);

  
insert overwrite table medusa.customer_location partition (date=${startday})
select m2.mallid,
       m2.mac,
	   null,
	   max(workspace) 
  from (
        select mallid,
		       mac,
			   max(count) as num 
		  from appinterface.customer_macloccount 
		 where date=${startday} 
		   and latitude between 0 and 90 
		   and longitude between -180 and 180 
		 group by mallid,mac
	   ) m1
  join (select *,concat(round(latitude,4),'_',round(longitude,4)) workspace 
          from appinterface.customer_macloccount 
		 where  date=${startday} 
		   and latitude between 0 and 90 
		   and longitude between -180 and 180
	   ) m2 
    on (m1.mac=m2.mac and m1.num =m2.count and m1.mallid = m2.mallid)
 GROUP BY m2.mac,m2.mallid
 
 union all 

select  m2.mallid,
        null,
		uuid,
		max(workspace) 
  from (select mallid,
               uuid,
			   max(count) as num 
		  from mq.customer_macloccount 
		 where date=${startday} 
		   and latitude between 0 and 90 
		   and longitude between -180 and 180 
		 group by mallid,uuid
	   ) m1
  join (select *,concat(round(latitude,4),'_',round(longitude,4)) workspace 
          from mq.customer_macloccount 
		 where date=${startday} 
		   and latitude between 0 and 90 
		   and longitude between -180 and 180
	   ) m2 
	on (m1.uuid=m2.uuid and m1.num =m2.count and m1.mallid = m2.mallid)
 GROUP BY m2.uuid, m2.mallid;
 