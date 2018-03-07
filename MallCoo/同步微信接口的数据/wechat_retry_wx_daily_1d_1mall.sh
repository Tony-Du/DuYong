#!/bin/bash
export PATH=/apps/jdk/bin:/apps/hadoop/sbin:/apps/hadoop/bin:/apps/hive/bin:/usr/local/bin:$PATH


date_beg='20150402'
date_end='20150531'

mallid=(10114,10183,10278,10345,10348,10509)
mall_filter="and mallid in (${mallid}) "

hive -v -e "
set hive.auto.convert.join=false;

INSERT INTO TABLE eyes.MongodbAppUserRegisterDailyCount
SELECT
    a.id,
    a.mallid,
    a.mallname,
    a.shopid,
    a.shopname,
    a.mallareaid,
    a.mallareaname,
    a.date,
    a.createtime,
    a.appsettingid,
    a.installcount,
    a.androidinstallcount,
    a.iosinstallcount,
    COALESCE(f.new_user,0) FollowCount,
    COALESCE(f.cancel_user,0) CancelCount,
    COALESCE(f.cumulate_user,0) WXCount,
    a.registercount,
    a.androidregcount,
    a.iosregcount,
    a.regrate,
    a.androidregrate,
    a.iosregrate,
    a.outcount,
    a.outrate,
    a.androidoutrate,
    a.iosoutrate,
    a.androidoutcount,
    a.iosoutcount,
    a.andriodopencount,
    a.iosopencount,
    a.opencount,
    a.type,
    a.avg,
    a.installallcount,
    a.cardrate,
    a.cardcount,
    a.cardopencount,
    a.cardbindcount,
    COALESCE(f.new_user,0)-COALESCE(f.cancel_user,0) NetFollowCount
FROM (
    SELECT *
    FROM eyes.MongodbAppUserRegisterDailyCount
    WHERE type=2 and TO_HIVE_DATE(date) between ${date_beg} and ${date_end} ${mall_filter}
) a LEFT JOIN (
      SELECT mallid,new_user,cancel_user,cumulate_user,date
      FROM mq.wx_appcount
      WHERE date between ${date_beg} and ${date_end}
      ${mall_filter}
) f ON a.mallid=f.mallid and TO_HIVE_DATE(a.date)=f.date;


INSERT INTO TABLE eyes.MongodbAppUserRegisterDailyCount
SELECT concat(f.date,'_',f.mallid,'_2') as id,
    f.mallid,
    null as mallname,
    null as shopid,
    null as shopname,
    null as mallareaid,
    null as mallareaname,
    to_mongo_date_time(f.date),
    from_unixtime(unix_timestamp()) as createtime,
    null as appsettingid,
    0 as installcount,
    0 as androidinstallcount,
    0 as iosinstallcount,
    COALESCE(f.new_user,0) FollowCount,
    COALESCE(f.cancel_user,0) CancelCount,
    COALESCE(f.cumulate_user,0) WXCount,
    0 as registercount,
    0 as androidregcount,
    0 as iosregcount,
    0 as regrate,
    0 as androidregrate,
    0 as iosregrate,
    0 as outcount,
    0 as outrate,
    0 as androidoutrate,
    0 as iosoutrate,
    0 as androidoutcount,
    0 as iosoutcount,
    0 as andriodopencount,
    0 as iosopencount,
    0 as opencount,
    2 as type,
    0 as avg,
    0 as installallcount,
    0 as cardrate,
    0 as cardcount,
    0 as cardopencount,
    0 as cardbindcount,
    COALESCE(f.new_user,0)-COALESCE(f.cancel_user,0) NetFollowCount
FROM (select * 
        from mq.wx_appcount 
       where date between ${date_beg} and ${date_end} ${mall_filter}
      ) f
left join (select * 
             from eyes.MongodbAppUserRegisterDailyCount 
            where type=2  
              and TO_HIVE_DATE(date) between ${date_beg} and ${date_end} ${mall_filter}
           ) m 
on m.mallid=f.mallid  and f.date=TO_HIVE_DATE(m.date)  
WHERE  m.id is null;



  INSERT INTO TABLE eyes.MongodbAppUserRegisterMonthlyCount
  SELECT
      concat(GET_MONTH_FIRST_DAY(date),'_',MallID,'_','2'),
      MallID,
      null,null,null,null,null,
      GET_MONTH_FIRST_DAY(date,1),
      from_unixtime(unix_timestamp()),
      NULL AppSettingID,
      sum(InstallCount),
      sum(FollowCount),
      sum(CancelCount),
      max(WXCount),
      sum(AndroidInstallCount),
      sum(IOSInstallCount),
      sum(RegisterCount),
      sum(AndroidRegCount),
      sum(IOSRegCount),
      round(avg(RegRate),4),
      round(avg(AndroidRegRate),4),
      round(avg(IOSRegRate),4),
      sum(OutCount),
      round(avg(OutRate),4),
      round(avg(AndroidOutRate),4),
      round(avg(IOSOutRate),4),
      sum(AndroidOutCount),
      sum(IOSOutCount),
      sum(AndriodOpenCount),
      sum(IOSOpenCount),
      sum(OpenCount),
      2 type,
      0,
      0,
      round(sum(COALESCE(cardcount,0))/sum(InstallCount),4) CardRate,
      sum(COALESCE(cardcount,0))  cardcount,
      sum(COALESCE(cardopencount,0)) cardopencount,
      sum(COALESCE(cardbindcount,0)) cardbindcount,
      sum(COALESCE(NetFollowCount,0)) NetFollowCount
  FROM eyes.MongodbAppUserRegisterDailyCount
  WHERE TO_HIVE_DATE(date) between GET_MONTH_FIRST_DAY(${date_beg}) and GET_MONTH_LAST_DAY(${date_end}) AND type=2 ${mall_filter}
  GROUP by concat(GET_MONTH_FIRST_DAY(date),'_',MallID,'_','2'),MallID,GET_MONTH_FIRST_DAY(date,1);


  INSERT INTO TABLE eyes.MongodbAppUserRegisterWeeklyCount
  SELECT
      concat(GET_MONDAY(date),'_',MallID,'_','2'),
      MallID,
      null,null,null,null,null,
      GET_MONDAY(date,1),
      from_unixtime(unix_timestamp()),
      NULL AppSettingID,
      sum(InstallCount),
      sum(FollowCount),
      sum(CancelCount),
      max(WXCount),
      sum(AndroidInstallCount),
      sum(IOSInstallCount),
      sum(RegisterCount),
      sum(AndroidRegCount),
      sum(IOSRegCount),
      round(avg(RegRate),4),
      round(avg(AndroidRegRate),4),
      round(avg(IOSRegRate),4),
      sum(OutCount),
      round(avg(OutRate),4),
      round(avg(AndroidOutRate),4),
      round(avg(IOSOutRate),4),
      sum(AndroidOutCount),
      sum(IOSOutCount),
      sum(AndriodOpenCount),
      sum(IOSOpenCount),
      sum(OpenCount),
      2 type,
      0,
      0,
      round(sum(COALESCE(cardcount,0))/sum(InstallCount),4) CardRate,
      sum(COALESCE(cardcount,0))  cardcount,
      sum(COALESCE(cardopencount,0))  cardopencount,
      sum(COALESCE(cardbindcount,0))  cardbindcount,
      sum(COALESCE(NetFollowCount,0))  NetFollowCount
  FROM eyes.MongodbAppUserRegisterDailyCount
  WHERE TO_HIVE_DATE(date) between GET_MONDAY(${date_beg}) and GET_SUNDAY(${date_end}) and type=2 ${mall_filter}
  GROUP BY concat(GET_MONDAY(date),'_',MallID,'_','2'),MallID,GET_MONDAY(date,1);
"