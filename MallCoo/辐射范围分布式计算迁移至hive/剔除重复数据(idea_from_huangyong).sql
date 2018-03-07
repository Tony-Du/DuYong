--insert overwrite table appinterface.customer_macloccount partition (date = $startday) 
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
         where date <= $startday                 
         group by mallid 
                 ,mac
       ) a
 where cnt > 1
)
select tm.mallid
      ,tm.mac
      ,round(tm.longitude,4) as longitude
      ,round(tm.latitude,4) as latitude
  from appinterface.CustomerLocation tm
  left join dup t1
    on tm.mallid = t1.mallid and tm.mac = t1.mac 
 where (t1.mac is null or tm.date = t1.last_date) 
   and date <= $startday
   
   
   

======test========

   
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
         where date <= '20170401' --$startday  
           and mallid = 6        
         group by mallid 
                 ,mac
       ) a
 where cnt > 1
)
select tm.mallid
      ,tm.mac
      ,round(tm.longitude,4) as longitude
      ,round(tm.latitude,4) as latitude
  from appinterface.CustomerLocation tm
  left join dup t1
    on tm.mallid = t1.mallid and tm.mac = t1.mac 
 where (t1.mac is null or tm.date = t1.last_date) 
   and tm.date <= '20170401' --$startday  
   and tm.mallid = 6
   and tm.mac = 'f8:a4:5f:34:ec:02'
   