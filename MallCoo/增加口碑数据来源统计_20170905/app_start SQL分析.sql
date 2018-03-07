
--app_start.py

--pages 子查询
SELECT s.AppSettingID AppSettingID,
       tpv.MallID MallID,
       tpv.CreateTime CreateTime,
       tpv.ContainerSource ContainerSource,
       tpv.UUID UUID,
       tpv.UserID UserID,
       tpv.Loc Loc,
       tpv.PreMark PreMark,
       tpv.Mark Mark,
       pm.Action PreAction,   
       m.Action Action,  
       tpv.Source Source,
       tpv.date date
  FROM (
        SELECT MallID
              ,CreateTime
              ,ContainerSource
              ,UUID       --用户没有登入的情况下。跟硬件绑定
              ,UserID     --注册后分配的 用户ID
              ,Loc
              ,PreMark
              ,Mark
              ,Source
              ,date
          FROM mq.TrackPageView
         WHERE date={0}
       ) tpv
  LEFT JOIN mq.Enum m ON lower(tpv.Mark)=lower(m.Mark)   --找action
  LEFT JOIN mq.Enum pm ON lower(tpv.PreMark)=lower(pm.Mark)  --找preaction
  LEFT JOIN mongo.mall s ON tpv.MallID=s.MallID   --找 app_setting_id      


  
--events 子查询
SELECT s.AppSettingID AppSettingID,
       te.MallID MallID,
       te.CreateTime CreateTime,
       te.ContainerSource ContainerSource,
       te.Source Source,
       te.UUID UUID,
       te.UserID UserID,
       te.Loc Loc,
       te.Action Action,
       te.refid refid,
       te.date date
  FROM (
        SELECT MallID              --
              ,CreateTime          --
              ,ContainerSource     --
              ,Source              --
              ,UUID                --
              ,UserID              --
              ,Loc                 --
              ,Action              --
              ,refid               --
              ,date                --
          FROM mq.TrackEvent
         WHERE date={0}
       ) te 
  LEFT JOIN mongo.mall s 
    ON te.MallID=s.MallID  
  

  
--graph 子查询
SELECT MallID,
       sum(int_page_read_user) IntUser,
       sum(int_page_read_count) IntCount,
       sum(ori_page_read_user) OriUser,
       sum(ori_page_read_count) OriCount,
       sum(share_user) ShareUser,
       sum(share_count) ShareCount,
       sum(add_to_fav_user) AddUser,
       sum(add_to_fav_count) AddCount
  FROM mq.WXGraph
 WHERE Type = 1 and date = {0}
 GROUP BY MallID        

 
 
 
--prepare0.py  prepare    更新每天的appuuid_all和qmiddle.mq_newuuidj这两张表的数据

--appuuid_all


select distinct mallid
      ,uuid
      ,userid
      ,date
      ,Case When containerSource in (1,2) then 1 
            When containerSource in (3) then 3 
            When containerSource in (5) then 2 END as type 
  from mq.trackpageview 
 where date = {0} 
   and containerSource in (1,2,3,5) 
   and uuid != '' 

--qmiddle.mq_newuuid   
select u.appsettingid
      ,u.mallid
      ,u.containersource
      ,u.uuid
      ,{0} as date 
  from (
        select distinct appsettingid
              ,mallid
              ,containersource
              ,uuid 
          from mq.trackpage_view 
         where date = {0} 
           and containersource = source 
           and action != 15114    --action=15114 对应 商场选择列表页面
       ) u 
  left outer join (
                   select * 
                     from qmiddle.mq_newuuid 
                    where date < {0}
                  ) v 
    on (u.appsettingid=v.appsettingid and u.mallid=v.mallid and u.containersource=v.containersource and u.uuid=v.uuid) 
 where v.date is null 
 

--register.py   计算eyes的app,wx,lapp的用户数指标
--app  v1
INSERT INTO TABLE eyes.mongodbAppUserRegisterDailyCount
SELECT concat({0},'_',a.AppSettingID,'_',1) id,
       null MallID,
       null,null,null,null,null,
       '{1} 00:00:00' Date,
       from_unixtime(unix_timestamp()) CreateTime,
       a.AppSettingID AppSettingID,
       COALESCE(c.num,0) InstallCount,
       COALESCE(c.Anum,0) AndroidInstallCount,
       COALESCE(c.Inum,0) IOSInstallCount,
       0 FollowCount,
       0 CancelCount,
       0 WXCount,
       COALESCE(u.vipnum,0) RegisterCount,
       COALESCE(u.vipAnum,0) AndroidregCount,
       COALESCE(u.vipInum,0) IOSRegCount,
       round(COALESCE(u.vipnum,0)/COALESCE(c.num,0),4) RegRate,
       round(COALESCE(u.vipAnum,0)/COALESCE(c.Anum,0),4) AndroidRegRate,
       round(COALESCE(u.vipInum,0)/COALESCE(c.Inum,0),4) IOSRegRate,
       COALESCE(c.num,0)-COALESCE(e.count,0) OutCount,
       round((COALESCE(c.num,0)-COALESCE(e.count,0))/COALESCE(c.num,0),4) OutRate,
       round((COALESCE(c.Anum,0)-COALESCE(e.Acount,0))/COALESCE(c.Anum,0),4) AndroidOutRate,
       round((COALESCE(c.Inum,0)-COALESCE(e.Icount,0))/COALESCE(c.Inum,0),4) ISOOutRate,
       COALESCE(c.Anum,0)-COALESCE(e.Acount,0) AndroidOutCount,
       COALESCE(c.Inum,0)-COALESCE(e.Icount,0) IOSOutCount,
       COALESCE(d.regnum,0) AndriodOpenCount,
       COALESCE(d.regAnum,0) IOSOpenCount,
       COALESCE(d.regInum,0) OpenCount,
       1 Type,
       0 Avg,
       0 InstallAllCount,
       0.0 CardRate,
       0  cardcount,
       0  cardopencount,
       0  cardbindcount,
       0  NetFollowCount
  FROM (
        SELECT DISTINCT AppSettingID 
          FROM mongo.mall 
       ) a 
  LEFT JOIN (
             SELECT t.AppSettingID,
                    count(DISTINCT(CASE WHEN t.ContainerSource=1 OR t.ContainerSource=2 THEN t.uuid END)) num,
                    count(DISTINCT(CASE WHEN t.ContainerSource=1 THEN t.uuid END)) Anum,  --安卓
                    count(DISTINCT(CASE WHEN t.ContainerSource=2 THEN t.uuid END)) Inum   --IOS
               FROM qmiddle.mq_newuuid t
              WHERE t.date={0}
              GROUP BY t.AppSettingID
            ) c 
    ON a.AppSettingID = c.AppSettingID 
  LEFT JOIN (
             SELECT user.UserPwd.type AppSettingID,
                    count(CASE WHEN user.UserPwd.datasource=1 OR user.UserPwd.datasource=2 THEN id END) vipnum,
                    count(CASE WHEN user.UserPwd.datasource=1 THEN id END) vipAnum,
                    count(CASE WHEN user.UserPwd.datasource=2 THEN id END) vipInum
               FROM appinterface.user
              WHERE user.UserPwd.Createtime >= '{1} 00:00:00' 
                AND user.UserPwd.Createtime <= '{1} 24:00:00'
              GROUP BY user.UserPwd.type
            ) u 
    ON a.AppSettingID=u.AppSettingID 
  LEFT JOIN (
             SELECT AppSettingID,
                    count(DISTINCT(CASE WHEN action=1 THEN uuid END)) regnum,
                    count(DISTINCT(CASE WHEN action=1 AND ContainerSource =1 THEN uuid END)) regAnum,
                    count(DISTINCT(CASE WHEN action=1 AND ContainerSource =2 THEN uuid END)) regInum
               FROM pages 
              WHERE date={0} 
                AND (ContainerSource=1 OR ContainerSource=2)
              GROUP BY AppSettingID 
            ) d 
    ON a.AppSettingID=d.AppSettingID 
  LEFT JOIN (
             SELECT u01.AppSettingID,
                    count(u01.uuid) count,
                    count(CASE WHEN u01.ContainerSource=1 THEN u01.uuid END) Acount,
                    count(CASE WHEN u01.ContainerSource=2 THEN u01.uuid END) Icount
               FROM (
                     SELECT DISTINCT AppSettingID
                           ,ContainerSource
                           ,uuid
                       FROM qmiddle.mq_newuuid
                      WHERE date={0}
                    ) u01 
               JOIN (
                     SELECT AppSettingID
                           ,ContainerSource
                           ,uuid
                           ,count(date)   --剔除 一天内登入次数小于1次 的uuid
                       FROM pages 
                      WHERE date={0} 
                        AND (ContainerSource=1 OR ContainerSource=2)
                      GROUP BY AppSettingID,ContainerSource,uuid
                     HAVING count(date)>=2 
                    ) u02 
                 ON u01.AppSettingID = u02.AppSettingID AND u01.ContainerSource = u02.ContainerSource AND u01.uuid = u02.uuid
              GROUP BY u01.AppSettingID
               ) e 
    ON a.AppSettingID=e.AppSettingID

            
            
--qmiddle.mq_newuuid |(统计跳出数)----------> eyes.mongodbAppUserRegisterDailyCount    App用户注册 天 统计      
--pages              |

--pages -----------------------> 

--appinterface.user -----------> 注册用户 维表

--qmiddle.mq_newuuid ----------> 所有的install 用户信息

--mongo.mall  


--register-wx
INSERT INTO TABLE eyes.MongodbAppUserRegisterDailyCount
SELECT concat({0},'_',a.mallid,'_',2) id,
       a.mallid MallID,
       null,null,null,null,null,
       '{1} 00:00:00' Date,
       from_unixtime(unix_timestamp()) CreateTime,
       null AppSettingID,
       COALESCE(c.num,0) InstallCount,
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
       COALESCE(c.num,0)-COALESCE(e.count,0) OutCount,
       round((COALESCE(c.num,0)-COALESCE(e.count,0))/COALESCE(c.num,0),4) OutRate,
       0 AndroidOutRate,
       0 ISOOutRate,
       0 AndroidOutCount,
       0 IOSOutCount,
       0 AndriodOpenCount,
       0 IOSOpenCount,
       0 OpenCount,
       2 Type,
       0 Avg,
       0  InstallAllCount,
       round(COALESCE(g.cardcount,0)/COALESCE(c.num,0),4) CardRate,
       COALESCE(g.cardcount,0)  cardcount,
       COALESCE(g.cardopencount,0)  cardopencount,
       COALESCE(g.cardbindcount,0)  cardbindcount ,
       COALESCE(f.new_user,0)-COALESCE(f.cancel_user,0) NetFollowCount
  FROM (
      SELECT DISTINCT mallid FROM mongo.mall
       ) a 
  LEFT JOIN (
             SELECT mallid,count(uuid) num
               FROM qmiddle.mq_newuuid
              WHERE date = {0} 
                AND ContainerSource = 5
              GROUP BY mallid
            ) c 
   ON a.mallid=c.mallid 
  LEFT JOIN (
             SELECT mallid,new_user,cancel_user,cumulate_user
               FROM mq.wx_appcount
              WHERE date={0}
            ) f 
    ON a.mallid=f.mallid 
  LEFT JOIN (
             SELECT t1.mallid mallid
                   ,count(CASE WHEN t1.datasource=5 THEN t1.id END) vipnum
               FROM (
                     SELECT user.userpwd.type type
                           ,mallid
                           ,id
                           ,user.userpwd.datasource datasource
                       FROM appinterface.user
                      WHERE user.userpwd.createtime>='{1} 00:00:00' 
                        AND user.userpwd.createtime<='{1} 24:00:00'
                    ) t1 
             JOIN (
                   SELECT AppSettingID,mallid
                     FROM AppSettingIDmallid
                  ) t2 ON t1.type = t2.AppSettingID AND t1.mallid=t2.mallid
             GROUP BY t1.mallid
            ) u 
    ON a.mallid=u.mallid
  LEFT JOIN (
             SELECT u01.mallid
                   ,count(u01.uuid) as count
               FROM (
                     SELECT mallid,uuid
                       FROM qmiddle.mq_newuuid
                      WHERE date={0} 
                        AND ContainerSource = 5
                    ) u01 
               JOIN (
                     SELECT mallid
                           ,uuid
                         ,count(DISTINCT mark)
                       FROM pages
                      WHERE date={0} 
                        AND ContainerSource=5
                      GROUP BY mallid,uuid
                     HAVING count(DISTINCT mark) >1
                    ) u02 
                ON u01.mallid=u02.mallid AND u01.uuid=u02.uuid
             GROUP BY u01.mallid
            ) e 
    ON a.mallid=e.mallid
  LEFT JOIN (
             SELECT mallid,
                    count(DISTINCT uid) cardcount,
                    count(DISTINCT case when creator='vipopen' then uid end) cardopencount,
                    count(DISTINCT case when creator='vipbind' then uid end) cardbindcount
               FROM customer.mallcard
              WHERE TO_HIVE_DATE(CreateTime)={0} 
                and datasource =5
              GROUP BY mallid
            ) g 
    ON a.mallid=g.mallid


--register-app v2
 INSERT INTO TABLE eyes.mongodbAppUserRegisterDailyCount
SELECT
      concat({0},'_',a.mallID,'_',4) id,
      a.mallid MallID,
      null,null,null,null,null,
      '{1} 00:00:00' Date,
      from_unixtime(unix_timestamp()) CreateTime,
      null AppSettingID,
      COALESCE(c.num,0) InstallCount,
      COALESCE(c.Anum,0) AndroidInstallCount,
      COALESCE(c.Inum,0) IOSInstallCount,
      0 FollowCount,
      0 CancelCount,
      0 WXCount,
      COALESCE(u.vipnum,0) RegisterCount,
      COALESCE(u.vipAnum,0) AndroidregCount,
      COALESCE(u.vipInum,0) IOSRegCount,
      round(COALESCE(u.vipnum,0)/COALESCE(c.num,0),4) RegRate,
      round(COALESCE(u.vipAnum,0)/COALESCE(c.Anum,0),4) AndroidRegRate,
      round(COALESCE(u.vipInum,0)/COALESCE(c.Inum,0),4) IOSRegRate,
      COALESCE(c.num,0)-COALESCE(e.count,0) OutCount,
      round((COALESCE(c.num,0)-COALESCE(e.count,0))/COALESCE(c.num,0),4) OutRate,
      round((COALESCE(c.Anum,0)-COALESCE(e.Acount,0))/COALESCE(c.Anum,0),4) AndroidOutRate,
      round((COALESCE(c.Inum,0)-COALESCE(e.Icount,0))/COALESCE(c.Inum,0),4) ISOOutRate,
      COALESCE(c.Anum,0)-COALESCE(e.Acount,0) AndroidOutCount,
      COALESCE(c.Inum,0)-COALESCE(e.Icount,0) IOSOutCount,
      0 AndriodOpenCount,
      0 IOSOpenCount,
      0 OpenCount,
      4 Type,
      0 Avg,
      0  InstallAllCount,
      round(COALESCE(f.cardcount,0)/COALESCE(c.num,0),4) CardRate,
      COALESCE(f.cardcount,0)  cardcount,
      COALESCE(f.cardopencount,0)  cardopencount,
      COALESCE(f.cardbindcount,0)  cardbindcount,
      0 NetFollowCount
  FROM (
      SELECT DISTINCT mallid FROM mongo.mall
) a LEFT JOIN (
      SELECT
            t.mallid,
            count(DISTINCT(CASE WHEN t.ContainerSource=1 OR t.ContainerSource=2 THEN t.uuid END)) num,
            count(DISTINCT(CASE WHEN t.ContainerSource=1 THEN t.uuid END)) Anum,
            count(DISTINCT(CASE WHEN t.ContainerSource=2 THEN t.uuid END)) Inum
      FROM eyes.mallid_uuid t
      WHERE t.date={0}
      GROUP BY t.mallid
) c ON a.mallid=c.mallid LEFT JOIN (
      SELECT
            mallid mallid,
            count(distinct CASE WHEN user.UserPwd.datasource=1 OR user.UserPwd.datasource=2 THEN id END) vipnum,
            count(distinct CASE WHEN user.UserPwd.datasource=1 THEN id END) vipAnum,
            count(distinct CASE WHEN user.UserPwd.datasource=2 THEN id END) vipInum
      FROM appinterface.user
      WHERE user.UserPwd.Createtime>='{1} 00:00:00' AND user.UserPwd.Createtime<='{1} 24:00:00'
      GROUP BY mallid
) u ON a.mallid=u.mallid
 LEFT JOIN (
      SELECT
            u01.mallid,
            count(u01.uuid) count,
            count(CASE WHEN u01.ContainerSource=1 THEN u01.uuid END) Acount,
            count(CASE WHEN u01.ContainerSource=2 THEN u01.uuid END) Icount
      FROM (
            SELECT DISTINCT mallid,ContainerSource,uuid
            FROM eyes.mallid_uuid
            WHERE date={0}
      ) u01 JOIN (
            SELECT mallid,ContainerSource,uuid,count(date)
            FROM pages
            WHERE date={0} AND (ContainerSource=1 OR ContainerSource=2)
            GROUP BY mallid,ContainerSource,uuid
            HAVING count(date)>=2
      ) u02 ON u01.mallid=u02.mallid AND u01.ContainerSource= u02.ContainerSource AND u01.uuid=u02.uuid
      GROUP BY u01.mallid
) e ON a.mallid=e.mallid
LEFT JOIN (
      SELECT
            mallid,
            count(DISTINCT uid) cardcount,
            count(DISTINCT case when creator='vipopen' then uid end) cardopencount,
           count(DISTINCT case when creator='vipbind' then uid end) cardbindcount
      FROM customer.mallcard
 WHERE TO_HIVE_DATE(CreateTime)={0} and datasource in (1,2)
      GROUP BY mallid
) f ON a.mallid=f.mallid


            
--behaviour.py  appcla_2

--behaviour-wx
INSERT INTO TABLE eyes.MongodbAppUserBehaviorDailyCountV2
SELECT concat('{0}','_',m.MallID,'_',2) id,
       m.MallID MallID,
       NULL,NULL,NULL,NULL,NULL,
       TO_MONGO_DATE_TIME({0}) Date,
       from_unixtime(unix_timestamp()) CreateTime,
       NULL AppSettingID,
       COALESCE(b.StartCount,0) StartCount,
       0 mallstartcount,
       0 mallstartcountrate,
       COALESCE(b.PersonStartCount,0) PersonStartCount,
       COALESCE(b.ValidStartCount,0) ValidStartCount,
       COALESCE(b.PersonStartCount-b.ValidStartCount,0) OutCount,
       round(COALESCE((b.PersonStartCount-b.ValidStartCount)*1.0/b.PersonStartCount,0),4) OutRate,
       COALESCE(b.RegisterStartCount,0) VIPStartCount,
       COALESCE(b.RegisterPersonStartCount,0) VIPCount,
       round(COALESCE(b.RegisterPersonStartCount*1.0/b.PersonStartCount,0),4) LoginRate,
       round(COALESCE(b.RegisterDuration*1.0/b.RegisterPersonStartCount,0)) MemberDuration,
       round(COALESCE(b.Duration*1.0/b.PersonStartCount,0)) AvgDuration,
       round(COALESCE(b.PersonStartCount*1.0/w.cu,0),4) UsageRate,
       round(COALESCE(b.RegisterPersonStartCount*1.0/u.num,0),4) RegisterUsageRate,
       COALESCE(b.MemberStartCount,0) CardMemberUsageNumber,
       COALESCE(b.MemberPersonStartCount,0) CardMemberUsageCount,
       round(COALESCE(b.MemberDuration*1.0/b.MemberPersonStartCount,0)) CardMemberUsageDuration,
       2 Type,
       0 Avg
  FROM mongo.mall m 
  LEFT JOIN (
             SELECT s.mallid mallid,
                    sum(if(s.uuid IS NOT NULL,s.StartCount,0)) StartCount,
                    count(s.uuid) PersonStartCount,
                    sum(if(s.uuid IS NOT NULL, s.staytime, 0)) Duration,
                    count(if(s.uuid IS NOT NULL AND s.markcount>1, s.uuid, NULL)) ValidStartCount,
                    sum(if(s.userid IS NOT NULL,s.StartCount,0)) RegisterStartCount,
                    count(s.userid) RegisterPersonStartCount,
                    sum(if(s.userid IS NOT NULL, s.staytime, 0)) RegisterDuration,
                    sum(if(s.userid IS NOT NULL AND s.isMember IS NOT NULL,s.StartCount,0)) MemberStartCount,
                    count(if(s.userid IS NOT NULL AND s.isMember IS NOT NULL,s.userid,NULL)) MemberPersonStartCount,
                    sum(if(s.userid IS NOT NULL AND s.isMember IS NOT NULL, s.staytime, 0)) MemberDuration
               FROM (
                     SELECT x.mallid mallid,
                            uuid uuid,
                            x.userid userid,
                            STAY_TIME(x.createtime, 60, 60) staytime,
                            count(DISTINCT x.mark) markcount,  
                            count(*) StartCount,         --页面打开的次数
                            max(y.uid) isMember          --是会员
                       FROM pages x                      --从mq.TrackPageView中来的
                       LEFT JOIN customer.mallcard y 
                         ON (x.mallid=y.mallid AND x.userid=y.uid)
                      WHERE x.date={0} 
                        AND x.ContainerSource = 5        --微信
                      GROUP BY uuid,x.userid,x.mallid
                      GROUPING SETS ((x.mallid,uuid),(x.mallid,x.userid))
                    ) s
              GROUP BY mallid
            ) b 
    ON m.mallid=b.mallid 
  LEFT JOIN (
             SELECT mallid
                   ,cumulate_user cu
               FROM mq.wx_appcount
              WHERE date={0}
            ) w 
    ON m.mallid = w.mallid 
  LEFT JOIN (
             SELECT mallid
                   ,count(*) num
               FROM appinterface.user
              WHERE userpwd.datasource=5 
                AND userpwd.createtime IS NOT NULL 
                AND TO_HIVE_DATE(userpwd.createtime)<={0}
              GROUP BY mallid
            ) u 
    ON m.mallid=u.mallid

    
--behaviour-appv2
INSERT INTO TABLE eyes.MongodbAppUserBehaviorDailyCountV2
SELECT concat('{0}','_',m.mallid,'_',4),
       m.mallid,
       NULL,NULL,NULL,NULL,NULL,
       TO_MONGO_DATE_TIME({0}),
       from_unixtime(unix_timestamp()),
       NULL AppSettingID,
       COALESCE(p.StartCount,0) StartCount,
       COALESCE(p.MallStartCount,0) MallStartCount,
       round(COALESCE(p.MallStartCount*1.0/p.msc,0),4) MallStartCountRate,
       
       COALESCE(if(p.PersonStartCount=0,b.PersonStartCount,p.PersonStartCount),0) PersonStartCount,
       COALESCE(b.prevalid,0) ValidStartCount,
       
       COALESCE(if(b.valid-b.prevalid>0,b.valid-b.prevalid,0),0) OutCount,
       COALESCE(round(COALESCE(if(b.valid-b.prevalid>0,b.valid-b.prevalid,0)*1.0/b.valid,0),4),0) OutRate,
       
       COALESCE(p.RegistStartCount ,0) VIPStartCount,
       COALESCE(if(p.RegistPersonStartCount=0,b.RegistPersonStartCount,p.RegistPersonStartCount),0) VIPCount,
       COALESCE(round(COALESCE( if(p.RegistPersonStartCount=0,b.RegistPersonStartCount,p.RegistPersonStartCount),0)*1.0/COALESCE(if(p.PersonStartCount=0,b.PersonStartCount,p.PersonStartCount),0),4),0) LoginRate,
       COALESCE(b.RegisterDuration,0) MemberDuration,
       COALESCE(b.Duration,0) AvgDuration,
       0 UsageRate,
       0 RegisterUsageRate,
       
       COALESCE(p.MemberStartCount,0) CardMemberUsageNumber,
       COALESCE(if(p.MemberPersonStartCount=0,b.MemberPersonStartCount,p.MemberPersonStartCount),0) CardMemberUsageCount,
       COALESCE(b.MemberDuration,0) CardMemberUsageDuration,
       4 Type,
       0 Avg
  FROM mongo.mall m 
  LEFT JOIN (
             SELECT count(if( (e.longitude>0 OR e.latitude>0) 
                              AND e.latitude>=c.latitude1 AND e.latitude<=c.latitude2 
                              AND e.longitude>=c.longitude1 AND e.longitude <= c.longitude2, 1, NULL) ) MallStartCount, --在场启动次数
                    count(if(e.longitude>0 OR e.latitude>0, 1, NULL)) msc,                                              --
                    count(e.uuid) StartCount,
                    count(DISTINCT e.uuid) PersonStartCount,
                    count(e.userid) RegistStartCount,
                    count(DISTINCT e.userid) RegistPersonStartCount,
                    count(v.uid) MemberStartCount,
                    count(DISTINCT v.uid) MemberPersonStartCount,
                    e.mallid mallid
               FROM (
                     SELECT mallid
                           ,loc['longitude'] longitude
                           ,loc['latitude'] latitude
                           ,uuid
                           ,userid
                       FROM events
                      WHERE date={0} 
                        AND (ContainerSource=1 OR ContainerSource=2) 
                        AND action = 22201  --该 action 对应 首次启动引导页面
                    ) e 
               LEFT JOIN (
                          SELECT cast(MapTopLeftLoc.latitude AS DOUBLE) latitude1,
                                 cast(MapTopLeftLoc.longitude AS DOUBLE) longitude1,
                                 cast(MapBottomRightLoc.latitude AS DOUBLE) latitude2,
                                 cast(MapBottomRightLoc.longitude AS DOUBLE) longitude2,
                                 mallid
                            FROM mongo.mall
                         ) c 
                 ON e.mallid=c.mallid 
               LEFT JOIN (
                          SELECT DISTINCT mallid,uid 
                            FROM customer.mallcard
                         ) v 
                 ON (e.mallid=v.mallid AND e.userid=v.uid)
             GROUP BY e.mallid
             ) p 
    ON m.mallid=p.mallid
  LEFT JOIN (
             SELECT s.mallid mallid,
                    round(sum(if(s.uuid IS NOT NULL, s.staytime, 0))*1.0/count(s.uuid)) Duration,       --人均使用时长
                    round(sum(if(s.userid IS NOT NULL, s.staytime, 0))*1.0/count(s.userid)) RegisterDuration,  --注册用户人均使用时长
                    round(sum(if(s.userid IS NOT NULL AND s.isMember IS NOT NULL, s.staytime, 0))/count(if(s.userid IS NOT NULL AND s.isMember IS NOT NULL,s.userid,NULL))) MemberDuration,  --会员人均使用时长
                    count(if(s.valid>0 , s.uuid, NULL)) valid,       --商场的“查看首页”被打开的次数
                    count(if(s.prevalid>0 , s.uuid, NULL)) prevalid, 
                    count(DISTINCT uuid) PersonStartCount,           --打开页面的有多少人数
                    count(DISTINCT userid) RegistPersonStartCount,   --打开页面的有多少注册用户数
                    count(DISTINCT if(isMember IS NOT NULL,userid,null)) MemberPersonStartCount --打开页面的有多少会员
               FROM (
                     SELECT x.mallid mallid,
                            uuid,
                            x.userid userid,
                            STAY_TIME(x.createtime, 60, 60) staytime,
                            sum(if(x.action=100000, 1, 0)) valid, --该action 对应 查看首页
                            sum(if(x.preaction=100000, 1, 0)) prevalid,
                            max(y.uid) isMember
                       FROM pages x    --pages里的数据都是打开“查看首页”时的数据？？？？
                       LEFT JOIN (
                                  SELECT DISTINCT mallid,uid 
                                    FROM customer.mallcard
                                 ) y 
                         ON (x.mallid=y.mallid AND x.userid=y.uid)
                      WHERE date={0} 
                        AND (ContainerSource=1 OR ContainerSource=2)
                      GROUP BY uuid,x.mallid,x.userid 
                      GROUPING SETS ((x.mallid,uuid),(x.mallid,x.userid))
                     ) s
               GROUP BY s.mallid
            ) b 
         ON m.mallid=b.mallid 




--active.py  计算eyes的app,wx,lapp的用户活跃率指标
INSERT INTO TABLE eyes.AppInstallTotalCount         --非分区表
SELECT concat('{0}','_',t1.MallID,'_',1),           --id
       t1.MallID,                                   --mallid
       null,                                        --mallname
       TO_MONGO_DATE_TIME({0}),                     --date
       from_unixtime(unix_timestamp()),             --createtime
       COALESCE(t1.InstallAllCount,0),              --installcount
       1                                            --type  老版本
  FROM (
        SELECT mallid,
               count(DISTINCT uuid) InstallAllCount
          FROM eyes.mallid_uuid
         WHERE date = {0} 
           and ContainerSource in (1,2) --APP
         GROUP BY mallid
       ) t1
       
 union all
 
SELECT concat('{0}','_',t1.MallID,'_',2),          --id    
       t1.MallID,                                  --mallid
       null,                                       --mallname
       TO_MONGO_DATE_TIME({0}),                    --date
       from_unixtime(unix_timestamp()),            --createtime
       COALESCE(t1.InstallAllCount,0),             --installcount
       2                                           --type
  FROM (
        SELECT mallid,
               count(DISTINCT uuid) InstallAllCount
          FROM qmiddle.mq_newuuid
         WHERE date = {0} 
           and ContainerSource in (5) --微信
         GROUP BY mallid
       ) t1 


       
INSERT INTO TABLE eyes.MongodbAppActiveDailyCount
SELECT
    concat('{0}','_',t1.MallID,'_',4),
    t1.MallID,
    null,
    '{1} 00:00:00',
    from_unixtime(unix_timestamp()),
    null AppSettingID,
    COALESCE(t1.InstallAllCount,0),
    COALESCE(t2.ActiveCount,0),
    round(COALESCE(t2.ActiveCount,0)/COALESCE(t1.InstallAllCount,0),4) ActiveRate,
    4 type,
    0 ArticleReader,
    0.0 ArticleReaderRate
FROM
 (
      SELECT mallid,
             sum(InstallCount) InstallAllCount
        FROM eyes.AppInstallTotalCount
       WHERE to_hive_date(date) <= {0} and  type=1
       GROUP BY mallid
) t1
LEFT JOIN (
           SELECT MallID,count(DISTINCT uuid) ActiveCount
             FROM pages
            WHERE date={0} 
              AND ContainerSource in (1,2)
            GROUP BY MallID
          ) t2 
  ON t1.MallID=t2.mallid

union all

SELECT
    concat('{0}','_',t1.MallID,'_',2),
    t1.MallID,
    null,
    '{1} 00:00:00',
    from_unixtime(unix_timestamp()),
    null AppSettingID,
    COALESCE(t1.InstallAllCount,0),
    COALESCE(t2.ActiveCount,0),
    round(COALESCE(t2.ActiveCount,0)/COALESCE(t1.InstallAllCount,0),4) ActiveRate,
    2 type,
    COALESCE(g.IntUser,0) ArticleReader,
    round(COALESCE(g.IntUser/WeChat.nof*1.0,0),4) ArticleReaderRate
FROM
 (
     SELECT
            mallid,
            sum(InstallCount) InstallAllCount
      FROM eyes.AppInstallTotalCount
      WHERE to_hive_date(date) <= {0} and type =2
      GROUP BY mallid
) t1
LEFT JOIN (
    SELECT MallID,count(DISTINCT uuid) ActiveCount
    FROM pages
    WHERE  date = {0} AND ContainerSource in (5)
    GROUP BY MallID
) t2 ON t1.MallID=t2.mallid
LEFT JOIN (
      SELECT MallID,cumulate_user nof
      FROM mq.wx_appcount
      WHERE date = {0}
) WeChat ON t1.MallID=WeChat.MallID
LEFT JOIN graph g ON t1.MallID=g.MallID    

=========分时统计========================
desc eyes.MongodbAppMemberActiveHourlyCountV2  
id                      string        
mallid                  bigint        
mallname                string        
shopid                  bigint        
shopname                string        
mallareaid              bigint        
mallareaname            string        
date                    timestamp     
createtime              timestamp     
appsettingid            bigint        
count                   bigint        
hour                    bigint        
type                    bigint       

insert into table eyes.MongodbAppMemberActiveHourlyCountV2  
select concat('{0}','_',su.mallid,'_4_',su.hour),               --id            
       su.mallid,                                               --mallid        
       null,                                                    --mallname      
       null,                                                    --shopid        
       null,                                                    --shopname      
       null,                                                    --mallareaid    
       null,                                                    --mallareaname  
       '{1} 00:00:00',                                          --date          
       from_unixtime(unix_timestamp()),                         --createtime    
       null,                                                    --appsettingid  
       if(s.count is null,0,s.count),                           --count     人数    
       su.hour,                                                 --hour          
       4                                                        --type          
  from(
       select sld.mallid,
              u.hour 
         from (
               select distinct mallid 
                 from mongo.mall 
              ) sld 
         join (
               select explode(array(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)) as hour 
                 from temp.quserforuse
              ) u 
      ) su
  left outer join(
                  select mallid,
                         hour(createtime) as hour,
                         count(distinct uuid) as count   --人数
                    from pages 
                   where date = {0} 
                     and containersource in (1,2) 
                     and source=containersource 
                   group by mallid, hour(createtime)
                 ) s
    on (su.mallid=s.mallid and su.hour=s.hour)


insert into table  eyes.MongodbAppMemberActiveHourlyCountV2  
select concat('{0}','_',su.appsettingid,'_','1','_',su.hour),        --id            
       null,                                                         --mallid        
       null,                                                         --mallname      
       null,                                                         --shopid        
       null,                                                         --shopname      
       null,                                                         --mallareaid    
       null,                                                         --mallareaname  
       '{1} 00:00:00',                                               --date          
       from_unixtime(unix_timestamp()),                              --createtime    
       su.appsettingid,                                              --appsettingid  
       if(s.count is null,0,s.count),                                --count     人数  
       su.hour,                                                      --hour          
       1                                                             --type          
  from (
        select sld.appsettingid,u.hour 
          from (
                select distinct appsettingid 
                  from mongo.mall 
               ) sld 
          join (
                select explode(array(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)) as hour 
                  from temp.quserforuse
               ) u 
       ) su
  left outer join (
                   select appsettingid,
                          hour(createtime) as hour,
                          count(distinct uuid) as count 
                     from pages 
                    where date = {0} 
                      and containersource = source 
                      and (containersource=1 or containersource=2) 
                    group by appsettingid, hour(createtime)
                  ) s
  on (su.appsettingid=s.appsettingid and su.hour=s.hour)
       
       

--user.py  计算eyes的线上运营的注册用户统计指标    
INSERT INTO TABLE eyes.MongodbRegisterUserDailyCount
SELECT concat('{0}','_',a.mallid),                --id
       a.mallid,                                  --mallid
       null,                                      --mallname
       null,                                      --shopid
       null,                                      --shopname
       null,                                      --mallareaid
       null,                                      --mallareaname
       TO_MONGO_DATE_TIME({0}),                   --date
       from_unixtime(unix_timestamp()),           --createtime
       null,                                      --appsettingid
       COALESCE(m.lappcount,0)+COALESCE(m.wxcount,0)+COALESCE(t.ioscount,0)+COALESCE(t.andriodcount,0)+COALESCE(s.portalcount,0),   --count
       COALESCE(t.ioscount,0),          --ioscount
       COALESCE(t.andriodcount,0),      --andriodcount
       COALESCE(m.lappcount,0),         --lappcount
       COALESCE(m.wxcount,0),           --wxcount
       0                                --avg
  FROM (
        SELECT mallid 
          FROM appsettingidmallid       --表：default.appsettingidmallid
       ) a 
  LEFT JOIN (
             SELECT t1.mallid mallid
                   ,count(case when t1.datasource=3 then 3 end) as lappcount
                   ,count(case when t1.datasource=5 then 5 end) as wxcount
               FROM (
                     SELECT userpwd.type as type
                           ,mallid
                           ,userpwd.datasource as datasource
                       FROM appinterface.user         --注册用户维表
                      WHERE TO_HIVE_DATE(userpwd.createtime)={0}
                    ) t1 
               JOIN (
                     SELECT appsettingid
                           ,mallid
                       FROM appsettingidmallid
                    ) t2 
                 ON t1.type=t2.appsettingid AND t1.mallid=t2.mallid
              GROUP BY t1.mallid
            ) m 
    ON a.mallid=m.mallid 
  LEFT JOIN (
             SELECT mallid
                   ,count(case when datasource=2 then 2 end) as ioscount
                   ,count(case when datasource=1 then 1 end) as Andriodcount
               FROM qmiddle.user2mallid
              WHERE TO_HIVE_DATE(createtime)={0}
              GROUP BY mallid
            ) t 
    ON a.mallid=t.mallid 
  LEFT JOIN (
            SELECT mallid
                  ,count(case when userpwd.datasource=6 then 6 end) as portalcount   --入口
              FROM appinterface.user
             WHERE TO_HIVE_DATE(userpwd.createtime)={0}
             GROUP BY mallid
            ) s 
    ON a.mallid=s.mallid 
    

INSERT INTO TABLE eyes.MongodbRegisterUserDailyCountV2
SELECT concat('{0}','_',mallid),                    --id
       mallid,                                      --mallid
       null,                                        --mallname
       null,                                        --shopid
       null,                                        --shopname
       null,                                        --mallareaid
       null,                                        --mallareaname
       TO_MONGO_DATE_TIME({0}),                     --date
       from_unixtime(unix_timestamp()),             --createtime
       null,                                        --appsettingid
       COALESCE(count,0),                           --count
       COALESCE(ioscount,0),                        --ioscount
       COALESCE(andriodcount,0),                    --andriodcount
       COALESCE(lappcount,0),                       --lappcount
       COALESCE(wxcount,0),                         --wxcount
       0                                            --avg
  FROM (
        SELECT t1.mallid mallid
              ,count(distinct id) as count
              ,count(distinct case when datasource=6 then id end) as portalcount 
              ,count(distinct case when datasource=1 then id end) as Andriodcount 
              ,count(distinct case when datasource=2 then id end) as ioscount 
              ,count(distinct case when datasource=3 then id end) as lappcount 
              ,count(distinct case when datasource=5 then id end) as wxcount 
        FROM (
              SELECT mallid
                    ,userpwd.datasource as datasource
                    ,id
                FROM appinterface.user
               WHERE TO_HIVE_DATE(userpwd.createtime)={0}
             ) t1
       group by t1.mallid
       ) t

       
--card.py  计算eyes的线上运营的商场卡会员指标
INSERT INTO TABLE eyes.MongodbCardMemberDailyCount
SELECT concat('{0}','_',t1.mallid),                           --id
       t1.mallid,                                             --
       null,                                                  --mallname
       null,                                                  --shopid
       null,                                                  --shopname
       null,                                                  --mallareaid
       null,                                                  --mallareaname
       TO_MONGO_DATE_TIME({0}),                               --date
       from_unixtime(unix_timestamp()),                       --createtime
       null,                                                  --appsettingid
       COALESCE(t1.count,0) count,                            --
       COALESCE(t1.opencardcount,0) opencardcount,            --
       COALESCE(t1.bindingcardcount,0) bindingcardcount,      --
       COALESCE(t1.ioscount,0) ioscount,                      --
       COALESCE(t1.androidcount,0) androidcount,              --
       COALESCE(t1.lappcount,0) lappcount,                    --
       COALESCE(t1.wxcount,0) wxcount,                        --
       COALESCE(t1.appcount,0) appcount,                      --
       0,                                                     --avg
       COALESCE(t2.UpgradeCount,0) UpgradeCount,              --
       COALESCE(t2.DegradeCount,0) DegradeCount               --
  from (
        select mallid,
               count(DISTINCT uid) count,
               count(DISTINCT case when creator = 'vipopen' then uid end) opencardcount,
               count(DISTINCT case when creator='vipbind' then uid end) bindingcardcount,
               count(DISTINCT case when datasource=2 then uid end) ioscount,
               count(DISTINCT case when datasource=1 then uid end) androidcount,
               count(DISTINCT case when datasource=3 then uid end) lappcount,
               count(DISTINCT case when datasource=5 then uid end) wxcount,
               count(DISTINCT case when datasource=1 OR datasource=2 then uid end) appcount
          FROM customer.mallcard 
         WHERE TO_HIVE_DATE(CreateTime)={0}
         GROUP BY mallid
       ) t1
  left join (
             select mallid
                   ,count(distinct Case When GradeType = 0 then userid End) as UpgradeCount     --升级会员数
                   ,count(distinct Case When GradeType = 1 then userid End) as DegradeCount     --降价会员数 
               from crm.GradeRecord  
              where TO_HIVE_DATE(CreateTime)={0} 
              group by mallid 
            ) t2 
    on (t1.mallid = t2.mallid) 


INSERT INTO TABLE eyes.MemberGradeCount_mongos
select concat(t1.mallid,'_',t1.cardtypeid)                               --id
      ,t1.mallid                                                         --
      ,''                                                                --mallname
      ,to_mongo_date_time({0})                                           --date
      ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')             --createtime
      ,t1.cardtypeid                                                     --
      ,t1.CardTitle                                                      --
      ,COALESCE(t1.count,0)                                              --count
  from (
        select mallid
              ,COALESCE(cardtypeid,-1) cardtypeid
              ,Case When cardtypeid is null then '未知卡类型' else max(cardtype) End as CardTitle
              ,count(distinct uid) as count 
          from customer.mallcard 
         group by mallid,cardtypeid
       ) t1
        
--park.py  计算eyes的线上运营的车场分析指标
INSERT INTO TABLE eyes.MongodbParkingDailyCount
SELECT concat(u.date,'_',u.MallID),
       u.MallID,
       null,null,null,null,null,
       TO_MONGO_DATE_TIME(u.date),
       from_unixtime(unix_timestamp()),
       0 In,
       0 Out,
       if(u.count is null,0,u.count) AmontTimes,
       if(u.sum is null,0,u.sum) Amont,
       if(v.count is null,0,v.count) SeekCar,
       0 Avg,
       COALESCE(w.Times,0) Times,
       0 CarNumber,
       COALESCE(u.CashCount,0) CashCount,
       COALESCE(u.BonusCount,0) BonusCount,
       COALESCE(u.CounponCount,0) CounponCount,
       COALESCE(u.ParkingFee,0) ParkingFee,
       COALESCE(u.OrderPrice,0) OrderPrice
  FROM (
        SELECT mallid,
               round(sum(ParkingFee),2) sum,
               count(*) count,
               count(if(OrderPrice>0,orderno,NULL)) CashCount,
               count(if(BonusAmountOffset>0,orderno,NULL)) BonusCount,
               count(if(CouponAmountOffset>0,orderno,NULL)) CounponCount,
               round(sum(ParkingFee),2) ParkingFee,
               round(sum(OrderPrice),2) OrderPrice,
               TO_HIVE_DATE(CreateTime) date
          FROM online.ParkFeeOrder
         WHERE TO_HIVE_DATE(CreateTime)={0} AND status=2
         GROUP BY mallid,TO_HIVE_DATE(CreateTime)
       ) u  
  LEFT JOIN (
             SELECT mallid
                   ,count(distinct userid) count
                   ,date
               FROM events
              WHERE date={0} 
                AND action = 14205
              GROUP BY mallid,date
            ) v 
    ON u.MallID=v.MallID AND u.date=v.date 
  LEFT JOIN (
             SELECT mallid
                   ,count(*) Times 
                   ,TO_HIVE_DATE(CreateTime) date
               FROM online.ParkFeeOrder             --停车费订单
              WHERE TO_HIVE_DATE(CreateTime)={0}
              GROUP BY mallid,TO_HIVE_DATE(CreateTime)
            ) w 
    ON u.MallID=w.MallID AND u.date=w.date


--click.py   计算eyes的app,wx,lapp的页面点击和模块点击指标
INSERT INTO TABLE eyes.AppPageClickDailyCountV2tmp                            --页面点击次数
SELECT concat(c.date,'_',c.id,'_',c.type),                                    --id
       if(c.type=1, NULL , c.id) MallID,                                      --mallid
       TO_MONGO_DATE_TIME(c.date),                                            --date
       from_unixtime(unix_timestamp()),                                       --createtime
       c.type,                                                                --canaltype
       if(c.type=1, c.id, NULL) AppSettingID,                                 --
       count(c.PageCount),                                                    --
       COALESCE(round(count(c.PageCount)/count(DISTINCT c.uuid),4),0)         --
  FROM (
        SELECT CASE WHEN ContainerSource=1 OR ContainerSource=2 THEN AppSettingID
                    WHEN ContainerSource=3 OR ContainerSource=5 THEN MallID
                    END id,
               uuid,
               CASE WHEN length(cast(action AS STRING))<4 THEN 100
                    WHEN Action IS NULL THEN -1
                    ELSE substring(Action,1,length(cast(Action AS STRING))-3)
                    END PageCount,
               CASE WHEN ContainerSource=1 OR ContainerSource=2 THEN 1
                    WHEN ContainerSource=5 THEN 2
                    WHEN ContainerSource=3 THEN 3
                    END type,
               date
          FROM pages
         WHERE ContainerSource = Source  --？？？
           AND date={0} 
           AND ContainerSource in (1,2,3,5)
       ) c
 GROUP BY c.type,c.id,c.date
 
union all

SELECT concat(c.date,'_',c.id,'_',c.type),
       c.id MallID,
       TO_MONGO_DATE_TIME(c.date),
       from_unixtime(unix_timestamp()),
       c.type,
       null AppSettingID,
       count(c.PageCount),
       COALESCE(round(count(c.PageCount)/count(DISTINCT c.uuid),4),0)
  FROM (
        SELECT mallid id,
               uuid,
               CASE WHEN length(cast(action AS STRING))<4 THEN 100
                    WHEN Action IS NULL THEN -1
                    ELSE substring(Action,1,length(cast(Action AS STRING))-3)
                    END PageCount,
               4 type, 
               date
          FROM pages
         WHERE date={0} 
           AND ContainerSource in (1,2) 
       ) c
GROUP BY c.type,c.id,c.date    
            

            
INSERT INTO TABLE eyes.AppModuleClickDailyCountV2tmp           --模块点击次数
SELECT concat(c.date,'_',c.id,'_',c.type,'_',c.ModuleID),      --id
       if(c.type=1, NULL , c.id) MallID,                       --mallid
       TO_MONGO_DATE_TIME(c.date),                             --date
       from_unixtime(unix_timestamp()),                        --createtime
       if(c.type=1, c.id, NULL) AppSettingID,                  --
       if(c.ModuleID=-999, NULL , c.ModuleID) ModuleID,        --
       count(c.ModuleID),                                      --count
       count(DISTINCT c.uuid),                                 --persons
       c.type                                                  --canaltype
  FROM (
        SELECT CASE WHEN ContainerSource=1 OR ContainerSource=2 THEN AppSettingID
                    WHEN ContainerSource=3 OR ContainerSource=5 THEN MallID
                    END id,
               uuid,
               CASE WHEN length(cast(action AS STRING))<4 THEN Action
                    WHEN Action IS NULL THEN -999
                    ELSE substring(Action,1,length(cast(Action AS STRING))-3) --???
                    END ModuleID,
               CASE WHEN ContainerSource=1 OR ContainerSource=2 THEN 1  --老版本
                    WHEN ContainerSource=5 THEN 2
                    WHEN ContainerSource=3 THEN 3
                    END type,
               date
          FROM pages
         WHERE date={0} 
           AND ContainerSource in (1,2,3,5)
       ) c
 GROUP BY c.type,c.id,c.date,c.ModuleID
 
union all

SELECT concat(c.date,'_',c.id,'_',c.type,'_',c.ModuleID),
       c.id MallID,
       TO_MONGO_DATE_TIME(c.date),
       from_unixtime(unix_timestamp()),
       if(c.type=1, c.id, NULL) AppSettingID,
       if(c.ModuleID=-999, NULL , c.ModuleID) ModuleID,
       count(c.ModuleID),
       count(DISTINCT c.uuid),
       c.type
  FROM (
        SELECT MallID id,
               uuid,
               CASE WHEN length(cast(action AS STRING))<4 THEN Action
                    WHEN Action IS NULL THEN -999
                    ELSE substring(Action,1,length(cast(Action AS STRING))-3)
                    END ModuleID,
               4 type,    --新版本
               date 
          FROM pages 
         WHERE date={0} 
           AND ContainerSource in (1,2) 
       ) c
 GROUP BY c.type,c.id,c.date,c.ModuleID

 
--extend.py
INSERT INTO TABLE eyes.MongodbRollingScreenDailyCount
SELECT concat({0}, '_', mallid, '_', refid),                                           --id
       mallid,                                                                         --
       null,                                                                           --mallname
       null,                                                                           --shopid
       null,                                                                           --shopname
       null,                                                                           --mallareaid
       null,                                                                           --mallareaname
       TO_MONGO_DATE_TIME({0}),                                                        --date
       from_unixtime(unix_timestamp()),                                                --createtime
       refid,                                                                          --frame
       null,                                                                           --name
       count(Containersource) count,                                                   --
       count(CASE WHEN Containersource=1 THEN Containersource END) AS Acount,          --
       count(CASE WHEN Containersource=2 THEN Containersource END) AS Icount,          --
       count(CASE WHEN Containersource=5 THEN Containersource END) AS wxcount,         --
       count(CASE WHEN Containersource=3 THEN Containersource END) AS lappcount,       --
       0                                                                               --avg
  FROM events 
 WHERE date = {0} 
   AND refid <> '' 
   AND refid <> 'null' 
   AND action = 2 
 GROUP BY mallid, refid 

 
--retention.py   思路：拿当前月的活跃用户和 过去一年(包括当前月)的新增用户 进行 fulljoin, 两者能匹配上的就是 当前月相当于 过去某个月的新增用户的留存用户 
      --dateArray = datetime.datetime.strptime(date,"%Y%m%d")   20170914
      --{0}     end = dateArray.strftime("%Y%m")     201709
      --{1}     begin = datetime.datetime(dateArray.year-1, dateArray.month, 01).strftime("%Y%m")   201609


INSERT INTO TABLE eyes.AppRetentionCount
SELECT concat(max(s.this_month),'_',s.type,'_',s.id) id,
       TO_MONGO_DATE_TIME('{0}'),
       from_unixtime(unix_timestamp()),
       if(s.type=1, s.id, NULL) AppSettingID,
       if(s.type=1, NULL, s.id) MallID,
       s.type,
       collect_all(list)
  FROM (
        SELECT l.id id,
               l.type type,
               l.this_month this_month,
               l.pre_month pre_month,
               STRUCT(
                      TO_MONGO_DATE_TIME(l.pre_month),
                      l.pre_uuid,
                      COALESCE(l.retention, 0),
                      COALESCE(round(l.retention/l.pre_uuid, 4), 0),
                      COALESCE(round(l.retention/l.this_uuid, 4), 0) 
                     ) list            
          FROM (
                SELECT contrast.id id,
                       contrast.type type,
                       count(DISTINCT contrast.this_uuid),         --当月新增激活用户数
                       count(DISTINCT contrast.pre_uuid) pre_uuid,
                       count(DISTINCT if(contrast.this_uuid IS NOT NULL AND contrast.pre_uuid IS NOT NULL, contrast.this_uuid, NULL)) retention,
                       contrast.this_month this_month,
                       contrast.pre_month pre_month,
                       FIRST_VALUE(count(DISTINCT contrast.this_uuid)) OVER(PARTITION BY contrast.id,contrast.type 
                                             ORDER BY COALESCE(contrast.pre_month,'000000')) this_uuid
                  FROM (
                        SELECT COALESCE(last.id, new.id) id,
                               COALESCE(last.type, new.type) type,
                               COALESCE(last.month,'{0}') this_month,
                               new.month pre_month,
                               last.uuid this_uuid,
                               new.uuid pre_uuid
                          FROM (
                                SELECT DISTINCT a.uuid uuid,
                                       a.type type,
                                       if(a.type=1, m.AppSettingID, a.mallid) id,
                                       a.month month
                                  FROM mongo.mall m 
                                  JOIN (
                                        SELECT uuid,
                                               type,
                                               mallid,
                                               DATE_PARSE(date,3) month     --格式 yyyyMM
                                          FROM appuuid_all                  --活跃用户表
                                         WHERE DATE_PARSE(date,3) = {0}     --201709
                                       ) a 
                                    ON a.mallid=m.mallid
                                 WHERE if(a.type=1, m.AppSettingID, a.mallid) IS NOT NULL 
                               ) last 
                          FULL JOIN (
                                    SELECT DISTINCT
                                           CASE 
                                               WHEN ta.ContainerSource=1 OR ta.ContainerSource=2 THEN 1
                                               WHEN ta.ContainerSource=5 THEN 2
                                               WHEN ta.ContainerSource=3 THEN 3
                                               WHEN ta.ContainerSource=7 THEN 5    --增加口碑数据                          
                                           END Type,
                                           CASE 
                                               WHEN ta.ContainerSource=1 OR ta.ContainerSource=2 THEN AppSettingID
                                               WHEN ta.ContainerSource=5 OR ta.ContainerSource=3 OR ta.ContainerSource=7 THEN tb.newmallid  --增加口碑数据     
                                           END id,
                                           ta.uuid,
                                           DATE_PARSE(ta.date,3) month
                                       FROM qmiddle.mq_newuuid ta
                                       left join (select mallid, newmallid from mongo.mall) tb
                                          on ta.mallid = tb.mallid 
                                       WHERE DATE_PARSE(ta.date,3)>={1} 
                                         AND DATE_PARSE(ta.date,3)<={0} 
                                         AND ta.ContainerSource in (1,2,3,5,7)   --增加口碑数据     
                                         AND ta.AppSettingID IS NOT NULL
                                    ) new 
                            ON (last.uuid=new.uuid AND last.type=new.type AND last.id=new.id)
                       )  contrast 
                 WHERE contrast.pre_month IS NOT NULL    -- 201609 <= x <= 201709 的用户还存在
                 GROUP BY id, contrast.type, contrast.pre_month, contrast.this_month 
                 GROUPING SETS (
                                (id,contrast.type,contrast.this_month),
                                (id,contrast.type,contrast.pre_month,contrast.this_month)
                               )
               ) l
         WHERE l.pre_month IS NOT NULL
       ) s
 GROUP BY s.type,s.id    

--version3
INSERT INTO TABLE eyes.AppRetentionCount
SELECT concat(max(s.this_month),'_4_',s.id) id,
       TO_MONGO_DATE_TIME('{0}'),
       from_unixtime(unix_timestamp()),
       NULL AppSettingID,
       s.id MallID,
       4 type,
       collect_all(list)
  FROM (
        SELECT l.id id,
               l.this_month this_month,
               l.pre_month pre_month,
               STRUCT(
                      TO_MONGO_DATE_TIME(l.pre_month),
                      l.pre_uuid,
                      COALESCE(l.retention, 0),
                      COALESCE(round(l.retention/l.pre_uuid, 4), 0),
                      COALESCE(round(l.retention/l.this_uuid, 4), 0)
                     ) list
          FROM (
                SELECT contrast.id id,
                       count(DISTINCT contrast.this_uuid),
                       count(DISTINCT contrast.pre_uuid) pre_uuid,
                       count(DISTINCT if(contrast.this_uuid IS NOT NULL AND contrast.pre_uuid IS NOT NULL, contrast.this_uuid, NULL)) retention,
                       contrast.this_month this_month,
                       contrast.pre_month pre_month,
                       FIRST_VALUE(count(DISTINCT contrast.this_uuid)) OVER(PARTITION BY contrast.id 
                                         ORDER BY COALESCE(contrast.pre_month,'000000')) this_uuid
                  FROM (
                        SELECT COALESCE(last.id, new.id) id,
                               COALESCE(last.month,'{0}') this_month,
                               new.month pre_month,
                               last.uuid this_uuid,
                               new.uuid pre_uuid
                          FROM (
                                SELECT DISTINCT uuid,
                                       mallid id,
                                       DATE_PARSE(date,3) month
                                  FROM appuuid_all
                                 WHERE DATE_PARSE(date,3) = {0} 
                                   AND mallid IS NOT NULL 
                                   AND type = 1
                               ) last 
                          FULL JOIN (
                                     SELECT DISTINCT MallID id,
                                            uuid,
                                            DATE_PARSE(date,3) month
                                       FROM eyes.mallid_uuid
                                      WHERE DATE_PARSE(date,3)>={1} AND DATE_PARSE(date,3)<={0} 
                                        AND ContainerSource in (1,2)
                                    ) new 
                            ON last.uuid=new.uuid  AND last.id=new.id
                       ) contrast
                 WHERE contrast.pre_month IS NOT NULL
                 GROUP BY id,contrast.pre_month,contrast.this_month
                 GROUPING SETS (
                                (id,contrast.this_month),
                                (id,contrast.pre_month,contrast.this_month)
                               )
               ) l
         WHERE l.pre_month IS NOT NULL
       ) s
 GROUP BY s.id 
 
 
--channel.py
INSERT INTO TABLE eyes.WeChatSourceChannelCount
SELECT concat(m.date, '_',m.mallid,'_',m.type,'_',1),
       m.mallid,
       TO_MONGO_DATE_TIME(m.date),
       from_unixtime(unix_timestamp()) CreateTime,
       COALESCE(r.openid,0),
       m.type,
       1 datetype
  FROM (
        SELECT a.mallid mallid,
               a.type type,
               b.date date
          FROM (
                SELECT mallid,
                       types.type type
                  FROM mongo.mall
               LATERAL VIEW explode(ARRAY(1,2,3,4,5)) types AS type
                 WHERE AppSettingID IS NOT NULL
               ) a 
          JOIN (
                SELECT date
                  FROM temp.calendar
                 WHERE {0}
               ) b
       ) m 
  LEFT JOIN (
            SELECT t.mallid mallid,
                   count(t.openid) openid,
                   t.type type,
                   t.date date
              FROM (
                    SELECT c.mallid mallid,
                           c.openID openID,
                           c.date date,
                           split(min(c.type), '_')[1] type
                      FROM (
                            SELECT a.mallid,
                                   a.openID openID,
                                   a.date date,
                                   COALESCE(concat(abs(a.time-o.time),'_',o.type), 
                                   concat(abs(a.time-s.time),'_',s.type),'0_0') type
                              FROM (
                                    SELECT DISTINCT mallid,
                                           openid,
                                           date,
                                           unix_timestamp(CreateTime) time
                                      FROM pqr.UserSNSEvent
                                     WHERE event=1 
                                       AND {0}
                                   ) a 
                              LEFT JOIN (
                                         SELECT mallid,
                                                openid,
                                                1 Type,
                                                date,
                                                unix_timestamp(CreateTime) time
                                           FROM pqr.scanrecord
                                          WHERE isFirstTime=TRUE 
                                            AND {0}
                                          
                                          UNION ALL
                                          
                                         SELECT y.mallid mallid,
                                                x.openID openID,
                                                4 Type,
                                                x.date date,
                                                x.time time
                                           FROM (
                                                 SELECT regexp_replace(EventKey, 'qrscene_', '') id,
                                                        FromUserName openID,
                                                        date,
                                                        unix_timestamp(CreateTime) time
                                                   FROM pqr.WeixinCallbackMessage
                                                  WHERE Event=2 
                                                    AND {0}
                                                ) x 
                                           JOIN (
                                                 SELECT WXQR_ID_Per id,mallid
                                                   FROM pqr.QrMap
                                                  WHERE QrBusinessType = 40
                                                ) y 
                                             ON x.id=y.id
                                                
                                         UNION ALL
                                         
                                         SELECT y.mallid mallid,
                                                x.openID openID,
                                                2 Type,
                                                x.date date,
                                                x.time time
                                           FROM (
                                                 SELECT regexp_replace(EventKey, 'qrscene_', '') id,
                                                        FromUserName openID,
                                                        date,
                                                        unix_timestamp(CreateTime) time
                                                   FROM pqr.WeixinCallbackMessage
                                                  WHERE {0}
                                                ) x 
                                          JOIN (
                                                SELECT WXQR_ID_Per id,
                                                       mallid
                                                  FROM pqr.QrMap
                                                 WHERE QrBusinessType = 60
                                               ) y 
                                            ON x.id=y.id
                                        ) o 
                           ON a.mallid=o.mallid AND a.openid=o.openid AND a.date=o.date
                         LEFT JOIN (
                                    SELECT weixinid openid,
                                           mallid,
                                           3 Type,
                                           date,
                                           unix_timestamp(CreateTime) time
                                      FROM wifi.authenticationinfo
                                     WHERE AuthenticationType=2 AND {0}
                                    
                                     UNION
                                    
                                    SELECT FromUserName openID,
                                           mallid,
                                           5 Type,
                                           date,
                                           unix_timestamp(CreateTime) time
                                      FROM pqr.WeixinCallbackMessage
                                     WHERE mallid IS NOT NULL 
                                       AND event=29 
                                       AND {0}
                                   ) s 
                           ON (a.mallid=s.mallid AND a.openid=s.openid AND a.date=s.date)
                           ) c
                    GROUP BY c.mallid,c.openID,c.date
                  ) t
            GROUP BY t.mallid,t.date,t.type
     ) r 
  ON (m.mallid=r.mallid AND m.type=r.type AND m.date=r.date)
  
  
  
  
  

--crm.py

--eyes.activemember  --活跃会员
select m1.mallid,
       m1.userid,
       date,
       2 source 
  from (
        select distinct date,
               userid,
               mallid  
          from mq.trackpageview 
         where date = {0} 
           and userid is not null
       ) m1 
  join customer.mallcard m2 
    on (m1.mallid = m2.mallid and m1.userid = m2.uid)
    
 union all

select distinct mallid,
       userid,
       date,
       3 source 
  from crm.BonusHistoryUser 
 where date = {0} 
   and bonus <> 0
   
 union all

select distinct mallid,
       userid,
       to_hive_date(tradetime) date,
       4 source 
  from crm.consBonusHistory 
 where to_hive_date(tradetime) = {0} 
   and CardNo is not null



 INSERT INTO TABLE eyes.MemberActiveCountDaily
    SELECT
        concat('{0}','_',t1.mallid),
        t1.mallid,
        null mallname,
        TO_MONGO_DATE_TIME({0}),
        from_unixtime(unix_timestamp()),
        COALESCE(t2.ActiveCount,0) ActiveCount,                           --活跃会员数
        COALESCE(round(t2.ActiveCount/t1.cardcount,4),0.0) ActiveRate,    --会员活跃率
        COALESCE(t2.UsedCount,0) UsedCount,                               
        COALESCE(t2.TrafficCount,0) TrafficCount,
        COALESCE(t2.BonusCount,0) BonusCount,
        COALESCE(t2.ConsumeCount,0) ConsumeCount,
        COALESCE(t3.ConsumeNumber,0) ConsumeNumber,
        COALESCE(t3.ConsumeAmount,0) ConsumeAmount,
        COALESCE(t3.ConsumeAvg,0) ConsumeAvg,
        0.0,
        COALESCE(t1.cardcount,0) MemberTotalCount
    FROM (
        SELECT mallid,count(distinct uid) cardcount
          FROM customer.mallcard 
         WHERE TO_HIVE_DATE(createtime) <= {0}
         GROUP BY mallid
    ) t1 LEFT JOIN (
        SELECT
            mallid,
            count(distinct uid) ActiveCount,
            count(distinct CASE WHEN source=2 THEN uid END) UsedCount,     --会员使用应用人数
            count(distinct CASE WHEN source=1 THEN uid END) TrafficCount,  --
            count(distinct CASE WHEN source=3 THEN uid END) BonusCount,    --
            count(distinct CASE WHEN source=4 THEN uid END) ConsumeCount   
        FROM eyes.activemember
        WHERE date={0}
        GROUP BY mallid
    ) t2 
    ON t1.mallid=t2.mallid 
    LEFT JOIN (
        SELECT
            mallid,
            count(id) ConsumeNumber,
            sum(amount) ConsumeAmount,
            round(sum(amount)/count(distinct userid),4) ConsumeAvg
        FROM crm.consBonusHistory
        WHERE TO_HIVE_DATE(tradetime)={0} 
          AND CardNo IS NOT NULL
        GROUP BY mallid
    ) t3 ON t1.mallid=t3.mallid   
   

   
   
 INSERT INTO TABLE eyes.MemberBonusAddDaily
SELECT
    concat('{0}','_',t1.mallid),
    t1.mallid,
    null mallname,
    TO_MONGO_DATE_TIME({0}),
    from_unixtime(unix_timestamp()),
    COALESCE(t1.TotalBonus,0) TotalBonus,
    COALESCE(t2.ConsBonus,0) ConsBonus,
    0 ScanCall,
    COALESCE(t2.ScanNumber,0) ScanNumber,
    COALESCE(t2.ScanCount,0) ScanCount,
    COALESCE(t2.ScanBonus,0) ScanBonus,
    0 PhotoCall,
    COALESCE(t2.PhotoNumber,0) PhotoNumber,
    COALESCE(t2.PhotoCount,0) PhotoCount,
    COALESCE(t2.PhotoBonus,0) PhotoBonus,
    COALESCE(t1.ActiveBonus,0) ActiveBonus,
    COALESCE(t1.ActiveNumber,0) ActiveNumber,
    COALESCE(t1.ActiveCount,0) ActiveCount,
    COALESCE(t2.ServiceBonus,0) ServiceBonus,
    COALESCE(t2.ServiceNumber,0) ServiceNumber,
    COALESCE(t2.ServiceCount,0) ServiceCount
FROM
(select mallid,
        sum(bonus) as TotalBonus,
        sum(Case When bonusaction not in (1,2,3,20) then bonus end ) as ActiveBonus,
        count(Case When bonusaction not in (1,2,3,20) then userid end ) as ActiveNumber,
        count(distinct Case When bonusaction not in (1,2,3,20) then userid end ) as ActiveCount
   from crm.BonusHistoryUser 
  where date ={0} 
    and bonus >0  
  group by mallid
) t1
LEFT JOIN
(select mallid,
        sum(bonus) as ConsBonus,
        count(Case When bonusaction =1 and consbonusorigin =2 then userid end ) as ScanNumber, 
        count(distinct Case When bonusaction =1 and consbonusorigin =2 then userid end ) as ScanCount,
        sum(Case When bonusaction =1 and consbonusorigin =2 then bonus end ) as ScanBonus,
        count(Case When bonusaction =1 and consbonusorigin =4 then userid end ) as PhotoNumber,
        count(distinct Case When bonusaction =1 and consbonusorigin =4 then userid end ) as PhotoCount,
        sum(Case When bonusaction =1 and consbonusorigin =4 then bonus end ) as PhotoBonus,
        sum(Case When bonusaction =1 and consbonusorigin =3 then bonus end ) as ServiceBonus,
        count(Case When bonusaction =1 and consbonusorigin =3 then userid end ) as ServiceNumber,
        count(distinct Case When bonusaction =1 and consbonusorigin =3 then userid end ) as ServiceCount 
   from crm.consBonusHistory 
  where to_hive_date(createtime) ={0} 
    and CardNo is not null 
  group by mallid
) t2
on (t1.mallid=t2.mallid)  