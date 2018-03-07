

--register.py

with pages as (
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
        SELECT MallID,CreateTime,ContainerSource,UUID,UserID,Loc,PreMark,Mark,Source,date
          FROM mq.TrackPageView
         WHERE date={0}
        ) tpv
  LEFT JOIN mq.Enum m ON lower(tpv.Mark)=lower(m.Mark)
  LEFT JOIN mq.Enum pm ON lower(tpv.PreMark)=lower(pm.Mark)
  LEFT JOIN mongo.mall s ON tpv.MallID=s.MallID
)
--insert into 
select concat({0}, '_', a.mallid, '_', 5) as id
      ,a.mallid
      ,null as mallname 
      ,null as shopid
      ,null as shopname
      ,null as mallareaid
      ,null as mallareaname 
      ,'{1} 00:00:00' as date
      ,from_unixtime(unix_timestamp()) as createtime
      ,null as appsettingid
      ,nvl(c.kb_new_add_cnt, 0) as installcount
      ,0 as androidinstallcount
      ,0 as IOSintallcount
      ,0 as followcount
      ,0 as cancelcount
      ,0 as WXcount
      ,nvl(u.kb_new_reg_cnt) as registercount
      ,0 as androidregcount
      ,0 as iosregcount
      ,round(nvl(u.kb_new_reg_cnt, 0)/nvl(c.kb_new_add_cnt), 4) as regrate
      ,0 as androidregrate
      ,0 as iosregrate
      ,nvl(c.kb_new_add_cnt, 0)-nvl(e.kb_sec_jump_cnt) as outcount
      ,round((nvl(c.kb_new_add_cnt, 0)-nvl(e.kb_sec_jump_cnt))/nvl(c.kb_new_add_cnt, 0), 4) as outrate
      ,0 as androidoutrate
      ,0 as iosoutrate
      ,0 as androidoutcount
      ,0 as iosoutcount
      ,0 as androidopencount
      ,0 as iosopencount
      ,0 as opencount
      ,5 as type
      ,0 as avg
      ,0 as installallcount
      ,round(nvl(f.kb_new_card_cnt)/nvl(c.kb_new_add_cnt, 0), 4)cardrate
      ,nvl(f.kb_new_card_cnt) as cardcount
      ,nvl(f.kb_new_cardopen_cnt) as cardopencount
      ,nvl(f.kb_new_cardbind_cnt) as cardbindcount
      ,0 as netfollowcount    
  from (
        select mallid
          from mongo.mall
         group by mallid
       ) a
  left join (
             select a.mallid
                   ,count(distinct (case when a.containersource = 7 then a.uuid end)) as kb_new_add_cnt
               from eyes.mallid_uuid a
              where a.date = {0}
              group by a.mallid 
            ) c
    on a.mallid = c.mallid
  left join (
             select a.mallid
                   ,count(distinct (case when user.userpwd.datasource = 7 then a.id end)) as kb_new_reg_cnt
                from appinterface.user a
               where user.userpwd.createtime between '{1} 00:00:00' and '{1} 23:59:59'
               group by a.mallid
            ) u
    on a.mallid = u.mallid
  left join (
             select t1.mallid
                   ,count(t1.uuid) as kb_sec_jump_cnt
               from ( 
                     select a.mallid
                           ,a.containersource
                           ,a.uuid
                       from eyes.mallid_uuid a
                      where a.date = {0}
                      group by a.mallid, a.containersource, a.uuid
                    ) t1
               join (
                      select mallid
                            ,containersource
                            ,uuid
                            ,count(date)
                        from pages
                       where date = {0} 
                         and containersource = 7 
                       group by mallid, containersource, uuid
                      having count(date) >= 2            
                 
                    ) t2
                 on t1.mallid = t2.mallid and t1.containersource = t2.containersource and t1.uuid = t2.uuid    
            ) e 
    on a.mallid = e.mallid
  left join (
             select a.mallid
                   ,count(distinct a.uid) kb_new_card_cnt
                   ,count(distinct case when creator = 'vipopen' then a.uid end) as kb_new_cardopen_cnt
                   ,count(distinct case when creator = 'vipbind' then a.uid end) as kb_new_cardbind_cnt
               from customer.mallcard a 
              where to_hive_date(createtime) = {0} 
                and datasourse = 7  
              group by a.mallid  
            ) f 
    on a.mallid = f.mallid
  



--customer.mallcard  -会员信息维表

--appinterface.user  -注册用户维表

--eyes.mallid_uuid   -新激活(一次访问即可)用户，按天增量

--mq.trackpageview   -页面访问追踪表 分区表date,mallid 按天增量

   



--behaviour.py 
with events as (
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
         WHERE date = {0}
       ) te 
  LEFT JOIN mongo.mall s 
    ON te.MallID = s.MallID  
),
pages as (
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
        SELECT MallID,CreateTime,ContainerSource,UUID,UserID,Loc,PreMark,Mark,Source,date
          FROM mq.TrackPageView
         WHERE date={0}
        ) tpv
  LEFT JOIN mq.Enum m ON lower(tpv.Mark)=lower(m.Mark)
  LEFT JOIN mq.Enum pm ON lower(tpv.PreMark)=lower(pm.Mark)
  LEFT JOIN mongo.mall s ON tpv.MallID=s.MallID
)

--insert into table eyes.mongodb
select concat('{0}','_',m.mallid,'_',5),
       m.mallid,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
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
       5 Type,
       0 Avg
  from mongo.mall m
  left join (
            select e.mallid 
                  ,count(e.uuid) as startcount                                  --商场页面的启动次数
                  ,count(distinct e.uuid) as PersonStartCount                   --商场页面的启动人数
                  ,count(e.userid) as RegistStartCount                          --商场页面被注册用户启动的次数
                  ,count(distinct e.userid) as RegistPersonStartCount           --商场页面启动的注册用户数
                  ,count(v.uid) as MemberStartCount                             --商场页面被会员启动的次数
                  ,count(distinct v.uid) as MemberPersonStartCount              --商场页面启动的会员用户数
                  ,count(if(e.longitude > 0 or e.latitude > 0, 1, null)) as msc --统计经度或者纬度大于0的记录数
                  ,count(if((e.longitude > 0 or e.latitude > 0) 
                         and e.latitude >= c.latitude1 and e.latitude <= c.latitude2
                         and e.longitude >= c.longitude1 and e.longitude <= c.longitude2, 1, null)) as MallStartCount --商场页面被在商场内的人启动的次数
              from (
                    select mallid as mallid
                          ,loc['longitude'] as longitude    --经度
                          ,loc['latitude'] as latitude      --纬度
                          ,uuid
                          ,userid
                      from events
                     where date = {0}
                       and containersource = '7'   --口碑数据标识
                       and action = 22201          --这里拿出来的全是“首次启动引导页”
                   ) e 
              left join (
                         select mallid
                               ,cast(maptopleftloc.latitude as double) as latitude1
                               ,cast(maptopleftloc.longitude as double) as longitude1
                               ,cast(mapbottomrightloc.latitude as double) as latitude2
                               ,cast(mapbottomrightloc.longitude as double) as longitude2
                           from mongo.mall     
                        ) c
                on e.mallid = c.mallid
              left join (
                         select distinct mallid
                               ,uid
                           from customer.mallcard
                        ) v 
                on (e.mallid = v.mallid and e.userid = v.uid)
             group by e.mallid         
            ) p 
    on m.mallid = p.mallid 
  left join (  
            select s.mallid
                  ,round(sum(if(s.uuid is not null, s.staytime, 0))*1.0/count(s.uuid)) as duration
                  ,round(sum(if(s.userid is not null, s.staytime, 0))*1.0/count(s.userid)) as registerduration
                  ,round(sum(if(s.userid is not null and s.ismember is not null, s.staytime, 0))*1.0/count(if(s.userid IS NOT NULL AND s.isMember IS NOT NULL,s.userid,NULL))) as memberduration
                  ,count(if(s.valid > 0, s.uuid, null)) as valid
                  ,count(if(s.prevalid > 0, s.uuid, null)) as valid
                  ,count(distinct uuid) as personstartcount
                  ,count(distinct uuid) as registerstartcount
                  ,count(distinct if(ismember is not null, s.uuid, null)) as memberstartcount
              from (  
                    select x.mallid
                          ,x.uuid
                          ,x.userid
                          ,stay_time(x.createtime, 60, 60) as staytime
                          ,sum(if(x.action = 100000, 1, 0)) as valid 
                          ,sum(if(x.preaction = 100000, 1, 0)) as prevalid
                          ,max(y.uid) as ismember
                      from pages x  
                      left join (
                                 select distinct mallid, uid
                                   from customer.mallcard
                                ) y  
                        on (x.mallid = y.mallid and x.userid = y.uid)
                     where x.date = {0}
                       and x.containersource = 7
                     group by x.mallid
                             ,x.uuid
                             ,x.userid
                     grouping sets((x.mallid, x.uuid),(x.mallid, x.userid))  
                   ) s
             group by s.mallid  
            ) b
    on m.mallid = b.mallid

    
--active.py
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

 union all  --增加口碑的数据
 
select concat('{0}','_',t1.MallID,'_',5),          --id    
       t1.MallID,                                  --mallid
       null,                                       --mallname
       TO_MONGO_DATE_TIME({0}),                    --date
       from_unixtime(unix_timestamp()),            --createtime
       COALESCE(t1.InstallAllCount,0),             --installcount    
       5                                           --type
  FROM (
        SELECT mallid,
               count(DISTINCT uuid) InstallAllCount  --登录人数
          FROM qmiddle.mq_newuuid
         WHERE date = {0} 
           and ContainerSource = 7                 --口碑数据
         GROUP BY mallid
       ) t1 
	   

	   
--INSERT INTO TABLE eyes.MongodbAppActiveDailyCount	
 union all
   	   
SELECT concat('{0}','_',t1.MallID,'_',5),
       t1.MallID,
       null,
       '{1} 00:00:00',
       from_unixtime(unix_timestamp()),
       null AppSettingID,
       COALESCE(t1.InstallAllCount,0),  
       COALESCE(t2.ActiveCount,0),
       round(COALESCE(t2.ActiveCount,0)/COALESCE(t1.InstallAllCount,0),4) ActiveRate,
       5 type,
       0 ArticleReader,
       0.0 ArticleReaderRate
  FROM (
		SELECT mallid,
		       sum(InstallCount) InstallAllCount
		  FROM eyes.AppInstallTotalCount      
		 WHERE to_hive_date(date) <= {0}      
		   and type = 5
		 GROUP BY mallid
       ) t1
  LEFT JOIN (
			SELECT MallID,
			       count(DISTINCT uuid) ActiveCount  --活跃人数
			  FROM pages
			 WHERE date = {0} 
			   AND ContainerSource = 7 
			 GROUP BY MallID
            ) t2 
    ON t1.MallID = t2.mallid

	
with pages as (
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
        SELECT MallID,CreateTime,ContainerSource,UUID,UserID,Loc,PreMark,Mark,Source,date
          FROM mq.TrackPageView
         WHERE date={0}
        ) tpv
  LEFT JOIN mq.Enum m ON lower(tpv.Mark)=lower(m.Mark)
  LEFT JOIN mq.Enum pm ON lower(tpv.PreMark)=lower(pm.Mark)
  LEFT JOIN mongo.mall s ON tpv.MallID=s.MallID
)	
select concat('{1}','_',su.mallid,'_5_',su.hour),
			       su.mallid,
				   null,
				   null,
				   null,
				   null,
				   null,
				   '{1} 00:00:00',
				   from_unixtime(unix_timestamp()),
				   null,
				   if(s.count is null,0,s.count),
				   su.hour,
				   5 
			  from (
			        select sld.mallid,
					       u.hour 
					  from (
					        select mallid 
							  from mongo.mall
					       ) sld 
					  join (
					        select explode(array(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)) as hour 
					          from temp.quserforuse
					       ) u 
				   ) su
              left outer join (
			                   select mallid,
							          hour(createtime) as hour,
									  count(distinct uuid) as count 
							     from pages 
								where date={0} 
								  and containersource=7 
								  and source=7 
								group by mallid, hour(createtime)
			                  ) s
                on (su.mallid=s.mallid and su.hour=s.hour)	
	
	
	

--user.py
--INSERT INTO TABLE eyes.MongodbRegisterUserDailyCountV2         --需要增加字段
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
       COALESCE(count, 0),                           --count
       COALESCE(ioscount, 0),                        --ioscount
       COALESCE(andriodcount, 0),                    --andriodcount
       COALESCE(lappcount, 0),                       --lappcount
       COALESCE(wxcount, 0),                         --wxcount
	   coalesce(publicpraisecount, 0) as publicpraisecount   ----口碑数据
       0                                            --avg
  FROM (
        SELECT t1.mallid mallid
              ,count(distinct id) as count
              ,count(distinct case when datasource = 6 then id end) as portalcount 
              ,count(distinct case when datasource = 1 then id end) as Andriodcount 
              ,count(distinct case when datasource = 2 then id end) as ioscount 
              ,count(distinct case when datasource = 3 then id end) as lappcount 
              ,count(distinct case when datasource = 5 then id end) as wxcount
			  ,count(distinct case when datasource = 7 then id end) as publicpraisecount --口碑数据
        FROM (
              SELECT mallid
                    ,userpwd.datasource as datasource
                    ,id
                FROM appinterface.user
               WHERE TO_HIVE_DATE(userpwd.createtime)={0}
             ) t1
       group by t1.mallid
       ) t	


--card.py
INSERT INTO TABLE eyes.MongodbCardMemberDailyCount         --需增加字段
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
	   COALESCE(t1.publicpraisecount, 0) publicpraisecount,   --增加的字段
       0,                                                     --avg
       COALESCE(t2.UpgradeCount,0) UpgradeCount,              --
       COALESCE(t2.DegradeCount,0) DegradeCount               --
  from (
        select mallid,
               count(DISTINCT uid) count,
               count(DISTINCT case when creator = 'vipopen' then uid end) opencardcount,
               count(DISTINCT case when creator='vipbind' then uid end) bindingcardcount,
               count(DISTINCT case when datasource = 2 then uid end) ioscount,
               count(DISTINCT case when datasource = 1 then uid end) androidcount,
               count(DISTINCT case when datasource = 3 then uid end) lappcount,
               count(DISTINCT case when datasource = 5 then uid end) wxcount,
               count(DISTINCT case when datasource = 1 OR datasource=2 then uid end) appcount,
			   count(DISTINCT case when datasource = 7 then uid end) publicpraisecount
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
	
	

--extend.py
with events as (
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
         WHERE date = {0}
       ) te 
  LEFT JOIN mongo.mall s 
    ON te.MallID = s.MallID  
)
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
	   count(case when )
       0                                                                               --avg
  FROM events 
 WHERE date = {0} 
   AND refid <> '' 
   AND refid <> 'null' 
   AND action = 2 
 GROUP BY mallid, refid 	



--click.py
INSERT INTO TABLE eyes.AppPageClickDailyCountV2tmp                            --页面点击次数
SELECT concat(c.date,'_',c.id,'_',c.type),                                    --id
       if(c.type=1, NULL , c.id) MallID,                                      --mallid
       TO_MONGO_DATE_TIME(c.date),                                            --date
       from_unixtime(unix_timestamp()),                                       --createtime
       c.type,                                                                --canaltype
       if(c.type=1, c.id, NULL) AppSettingID,                                 --
       count(c.PageCount),                                                    --pagecount
       COALESCE(round(count(c.PageCount)/count(DISTINCT c.uuid),4),0)         --pagecountper
  FROM (
        SELECT CASE WHEN ContainerSource=1 OR ContainerSource=2 THEN AppSettingID
                    WHEN ContainerSource=3 OR ContainerSource=5 OR ContainerSource=7 THEN MallID   --有修改
                    END id,
               uuid,
               CASE WHEN length(cast(action AS STRING))<4 THEN 100
                    WHEN Action IS NULL THEN -1
                    ELSE substring(Action,1,length(cast(Action AS STRING))-3)
                    END PageCount,                                            --？？  
               CASE WHEN ContainerSource=1 OR ContainerSource=2 THEN 1
                    WHEN ContainerSource=5 THEN 2
                    WHEN ContainerSource=3 THEN 3
					WHEN ContainerSource=7 THEN 5
                    END type,
               date
          FROM pages
         WHERE ContainerSource = Source  --？？？？
           AND date={0} 
           AND ContainerSource in (1,2,3,5, 7)    --增加口碑的数据 7
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
                    WHEN ContainerSource=3 OR ContainerSource=5 OR ContainerSource=7 THEN MallID  --有修改
                    END id,
               uuid,
               CASE WHEN length(cast(action AS STRING))<4 THEN Action
                    WHEN Action IS NULL THEN -999
                    ELSE substring(Action,1,length(cast(Action AS STRING))-3) --???
                    END ModuleID,
               CASE WHEN ContainerSource=1 OR ContainerSource=2 THEN 1  --老版本
                    WHEN ContainerSource=5 THEN 2
                    WHEN ContainerSource=3 THEN 3
					WHEN ContainerSource=7 THEN 5    --有修改
                    END type,
               date
          FROM pages
         WHERE date={0} 
           AND ContainerSource in (1,2,3,5,7)
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




--retention.py

INSERT INTO TABLE eyes.AppRetentionCount
SELECT concat(max(s.this_month),'_4_',s.id) id,
       TO_MONGO_DATE_TIME('{0}'),
       from_unixtime(unix_timestamp()),
       NULL AppSettingID,
       s.id MallID,
       5 type,
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
								   AND type = 5  --?????
						       ) last 
						  FULL JOIN (
									 SELECT DISTINCT MallID id,
									        uuid,
											DATE_PARSE(date,3) month
									   FROM eyes.mallid_uuid
									  WHERE DATE_PARSE(date,3)>={1} AND DATE_PARSE(date,3)<={0} 
									    AND ContainerSource = 7
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
 