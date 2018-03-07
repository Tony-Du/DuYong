set hive.auto.convert.join=false;
with stg_customer_location as (
select mallid
      ,mall_name
      ,GET_MAP_DISTANCE(mall_longitude,mall_latitude,customer_longitude,customer_latitude)/1000.0 as distance
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
         where b.longitude is not NULL 
           and b.latitude is not NULL 
       )t1
)     
select mallid,
       mall_name,
       case when 

  from (
        select mallid, 
               mall_name,
               sum(count) over (partition by mallid order by distance) as customer_count  --窗口函数
          from 
		  
		  
		  
		  
		  
		  
		  
		  
		  select t2.mallid,
		         t2.mall_name,
				 t1.distance,
		         case when t2.distance is null then 0 else t1.count end as count,
		  
		    from (mallid, mall_name,distance) t1  
            left join 			
			   (
			    select mallid, 
                       mall_name,
					   distance,
                       count(distinct mac) as count
                  from (
                        select case when distance > 0  and distance <= 1  then 1  
                                    when distance > 1  and distance <= 2  then 2  
                                    when distance > 2  and distance <= 3  then 3  
                                    when distance > 3  and distance <= 4  then 4  
                                    when distance > 4  and distance <= 5  then 5  
                                    when distance > 5  and distance <= 6  then 6  
                                    when distance > 6  and distance <= 7  then 7  
                                    when distance > 7  and distance <= 8  then 8  
                                    when distance > 8  and distance <= 10 then 10 
                                    when distance > 10 and distance <= 12 then 12 
                                    when distance > 12 and distance <= 14 then 14 
                                    when distance > 14 and distance <= 16 then 16 
                                    when distance > 16 and distance <= 30 then 30 
                                    else 100 
                               end distance,
                               mallid,
                               mall_name,
                               mac
                         from stg_customer_location           
                       ) y 
                group by mallid, mall_name,distance
               ) t2
			   
			   

			   
         order by mallid, mall_name,distance  
       ) t 
