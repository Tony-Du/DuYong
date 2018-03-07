#!/bin/bash

src_start_day=$1 
src_end_day=$2 

hive -v -e "

set mapreduce.job.queuename=long;

SET hive.auto.convert.join=false;

--with pages as(
-- SELECT
--        s.AppSettingID AppSettingID,
--        tpv.MallID MallID,
--        tpv.CreateTime CreateTime,
--        tpv.ContainerSource ContainerSource,
--        tpv.UUID UUID,
--        tpv.UserID UserID,
--        tpv.Loc Loc,
--        tpv.PreMark PreMark,
--        tpv.Mark Mark,
--        pm.Action PreAction,
--        m.Action Action,
--        tpv.Source Source,
--        tpv.date date
--    FROM (
--          SELECT MallID,CreateTime,ContainerSource,UUID,UserID,Loc,PreMark,Mark,Source,date
--            FROM mq.TrackPageView
--           WHERE date between '$src_start_day' and '$src_end_day'
--    ) tpv
--    LEFT JOIN mq.Enum m ON lower(tpv.Mark)=lower(m.Mark)
--    LEFT JOIN mq.Enum pm ON lower(tpv.PreMark)=lower(pm.Mark)
--    LEFT JOIN mongo.mall s ON tpv.MallID=s.MallID
--)
INSERT INTO TABLE eyes.MongodbAppUserRegisterDailyCount
SELECT                                                                                           
      concat(c.date,'_',a.newmallid,'_',2) id,                                                                       
      a.newmallid MallID,                                                                            
      null ,                                                                                      
      null,                                                                                       
      null,                                                                                       
      null,                                                                                       
      null,                                                                                       
      TO_MONGO_DATE_TIME(c.date) as Date,                                                                        
      from_unixtime(unix_timestamp()) as CreateTime,                                                 
      null AppSettingID,                  
      COALESCE(c.num,0) as InstallCount,             
      0 AndroidInstallCount,                                                                      
      0 IOSInstallCount,                                                                          
      COALESCE(f.new_user,0) FollowCount,                                                         
      COALESCE(f.cancel_user,0) CancelCount,                                                      
      COALESCE(f.cumulate_user,0) WXCount,            
      COALESCE(u.vipnum,0) RegisterCount,         
      0 AndroidregCount,                                                                          
      0 IOSRegCount,                                                                              
      round(COALESCE(u.vipnum,0)/COALESCE(c.num,0),4) RegRate,                                    
      0 AndroidRegRate,                                        
      0 IOSRegRate,               
      0 as OutCount,                                             
      0 as OutRate,                 
      0 AndroidOutRate,                                                                           
      0 ISOOutRate,                                                                               
      0 AndroidOutCount,                                                                          
      0 IOSOutCount,                                             
      0 AndriodOpenCount,                                                                         
      0 IOSOpenCount,                                                                             
      0 OpenCount,                                                 
      2 Type,                                                                                     
      0 Avg,                                                                                      
      0 InstallAllCount,          
      round(COALESCE(g.cardcount,0)/COALESCE(c.num,0),4) CardRate,                                
      COALESCE(g.cardcount,0)  cardcount,                                                         
      COALESCE(g.cardopencount,0)  cardopencount,                                                 
      COALESCE(g.cardbindcount,0)  cardbindcount ,                                                
      COALESCE(f.new_user,0)-COALESCE(f.cancel_user,0) NetFollowCount                             
FROM (                                                                                            
      SELECT DISTINCT mallid, newmallid FROM mongo.mall
) a 
LEFT JOIN (
  SELECT mallid,count(uuid) num, date
    FROM qmiddle.mq_newuuid 
   WHERE date between '$src_start_day' and '$src_end_day'
     AND ContainerSource=5
   GROUP BY mallid, date
) c 
ON a.mallid=c.mallid 
LEFT JOIN (
  SELECT mallid,
         new_user,
         cancel_user,
         cumulate_user,
         date
    FROM mq.wx_appcount
   WHERE date between '$src_start_day' and '$src_end_day'
) f 
ON a.mallid=f.mallid 
LEFT JOIN (
  SELECT t1.mallid mallid,
         count(CASE WHEN t1.datasource=5 THEN t1.id END) vipnum,
         t1.date
    FROM (
          SELECT user.userpwd.type type,
                 mallid,
                 id,
                 user.userpwd.datasource datasource,
                 TO_HIVE_DATE(user.userpwd.createtime) as date
            FROM appinterface.user 
           WHERE TO_HIVE_DATE(user.userpwd.createtime) between '$src_start_day' and '$src_end_day'
         ) t1 
    JOIN (
          SELECT AppSettingID,mallid
            FROM AppSettingIDmallid
         ) t2 
      ON t1.type=t2.AppSettingID AND t1.mallid=t2.mallid
   GROUP BY t1.mallid, t1.date
) u 
ON a.mallid=u.mallid
--LEFT JOIN (
--  SELECT u01.mallid,
--         u01.date,
--         count(u01.uuid) as count
--    FROM (
--          SELECT mallid,
--                 uuid,
--                 date
--            FROM qmiddle.mq_newuuid   --新增用户表
--           WHERE date between '$src_start_day' and '$src_end_day'
--             AND ContainerSource=5
--         ) u01 
--    JOIN (
--          SELECT mallid,uuid,count(DISTINCT mark),date
--            FROM pages
--           WHERE date between '$src_start_day' and '$src_end_day'
--             AND ContainerSource=5
--           GROUP BY mallid,uuid,date
--          HAVING count(DISTINCT mark) >1
--         ) u02 
--      ON (u01.mallid=u02.mallid AND u01.uuid=u02.uuid and u01.date=u02.date)
--   GROUP BY u01.mallid, u01.date
--) e 
--ON a.mallid = e.mallid
LEFT JOIN (
  SELECT
        mallid,
        count(DISTINCT uid) cardcount,
        count(DISTINCT case when creator='vipopen' then uid end) cardopencount,
        count(DISTINCT case when creator='vipbind' then uid end) cardbindcount,
        TO_HIVE_DATE(CreateTime) as date
   FROM customer.mallcard
  WHERE TO_HIVE_DATE(CreateTime) between '$src_start_day' and '$src_end_day'
    and datasource=5
  GROUP BY mallid, TO_HIVE_DATE(CreateTime)
) g 
ON a.mallid=g.mallid
where c.date = f.date 
  and c.date = u.date 
  --and c.date = e.date 
  and c.date = g.date;
"
