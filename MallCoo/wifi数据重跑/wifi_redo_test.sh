#!/bin/bash
export PATH=/appsk/bin:/apps/hadoop/sbin:/apps/hadoop/bin:/apps/hive/bin:$PATH

if  [[ -n "$1" ]] && [[ -n "$2" ]] && [[ -n "$3" ]]  ;then
  start_time=$1
  end_time=$2
  mallidlist=$3
  date_condition="$start_time and $end_time"

  echo "WIFI 数据重算"
  echo "已传参数!!!!!   时间条件:$date_condition , mallidlist : $mallidlist"

  
  hive -v -e"

  INSERT OVERWRITE TABLE wifi.all_macs PARTITION(date,mallid)
  SELECT n.mac,n.day,n.date,n.mallid
  FROM (
    SELECT mac,mallid,to_hive_date(min(logintime)) day,to_hive_date(min(logintime)) date
    FROM wifi.wifilog
    WHERE date BETWEEN $date_condition AND mallid in ($mallidlist)
    GROUP BY mallid,mac
  ) n LEFT JOIN (
    SELECT mac,mallid
    FROM wifi.all_macs
    WHERE date<'$start_time' and mallid in ($mallidlist)
  ) a ON n.mallid=a.mallid AND n.mac=a.mac
  WHERE a.mac IS NULL;

  INSERT overwrite TABLE temp.mac_time PARTITION(date) 
  select mallid,mac,round(sum(if((unix_timestamp(logouttime)-unix_timestamp(logintime))/60>20,20,
         (unix_timestamp(logouttime)-unix_timestamp(logintime))/60))),date 
	from wifi.wifilog 
   where mallid in ($mallidlist) 
     and date between $date_condition 
   group by mallid,mac,date;

  insert into table wifi.WifiDailyCount_mongos
  select concat(a.mallid,'_',a.date),a.mallid,'',NULL,NULL,NULL,NULL,from_unixtime(unix_timestamp(a.date,'yyyyMMdd'),'yyyy-MM-dd HH:mm:ss'),from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'),a.TotalNumber,a.LoginCount,0,0,if(c.MemberCount is null,0,c.MemberCount/a.LoginCount),if(b.First is null,0,b.First/a.LoginCount),0,if(c.MemberCount is null,0,c.MemberCount),a.AvgFlow,COALESCE(d.AvgRetentionTime,0),if(e.adclickrate>0 and e.adclickrate<1,e.adclickrate,1),0,from_unixtime(unix_timestamp(a.date,'yyyyMMdd'),'yyyy-MM-dd HH:mm:ss'),0 from
  (select mallid,count(*) as TotalNumber,count(distinct mac) as LoginCount,sum(totalinoctets)/count(mac) as AvgFlow,date from wifi.wifilog where mallid in ($mallidlist) and date between $date_condition  group by mallid,date) a
  left join
  (select mallid,count(distinct mac) as First,date from wifi.all_macs where mallid in ($mallidlist) group by mallid,date) b on a.mallid=b.mallid and a.date = b.date
  left join
  (select a.mallid,count( distinct a.mac) as MemberCount,date from wifi.wifilog  a join probe.member_mac b on a.mallid in ($mallidlist) and a.date between $date_condition  and a.mallid=b.mallid and a.mac=b.mac group by a.mallid,a.date) c on a.mallid=c.mallid  and a.date = c.date
  left join
  (select mallid,round(sum(time)/count(mac)) as AvgRetentionTime,date from temp.mac_time where mallid in ($mallidlist) group by date,mallid) d on a.mallid=d.mallid and a.date = d.date
  left join
  (select mallid,count(case when act=2 then mac end)/count(case when act=1 then mac end) as ADClickRate,regexp_replace(to_date(createtime),'-','') as date from wifi.AdvertisementInfo where mallid in ($mallidlist) and to_hive_date(createtime) between $date_condition group by mallid,regexp_replace(to_date(createtime),'-',''))e on a.mallid=e.mallid and a.date = e.date;

  "

  hive -v -e "
  INSERT INTO TABLE wifi.WifiAuMobileSystemDailyCount_mongos
  SELECT
    concat(t1.mallid,'_',t1.date),
    t1.mallid,
    '',
    NULL,
    NULL,
    NULL,
    NULL,
    TO_MONGO_DATE_TIME(t1.date),
    FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss'),
    COALESCE(t5.PVCount,0),
    COALESCE(t2.AllAuCount,0),
    COALESCE(t1.allcount,0),
    COALESCE(t5.failcount,0),
    COALESCE(t1.allcount,0)-COALESCE(t2.AllAuCount,0),
    COALESCE(t3.SMSSendCount,0),
    COALESCE(t2.SMSCount,0),
    COALESCE(t6.wechatviewcount,0),
    COALESCE(t2.WechatCount,0),
    0,
    COALESCE(t2.AppCount,0),
    COALESCE(t2.MemberCount,0),
    COALESCE(t1.iPhone+t1.iPad,0),
    COALESCE(t1.iPhone,0),
    COALESCE(t1.iPad,0),
    COALESCE(t1.PC,0),
    COALESCE(t1.Android,0),
    COALESCE(t1.Other,0),
    0,
    COALESCE(t2.AuNewCount,0),
    COALESCE(t2.AuSMSCount,0),
    COALESCE(t2.AuWechatCount,0),
    COALESCE(t2.AuAppCount,0),
    COALESCE(t2.AuMemberCount,0)
  FROM (
    SELECT
      date,
      mallid,
      COUNT(DISTINCT CASE WHEN instr(ClientSysType,'iPhone')>0 THEN mac END) iPhone,
      COUNT(DISTINCT CASE WHEN instr(ClientSysType,'iPad')>0 THEN mac END) iPad,
      COUNT(DISTINCT CASE WHEN instr(ClientSysType,'Android')>0 THEN mac END) Android,
      COUNT(DISTINCT CASE WHEN instr(ClientSysType,'Intel Mac OS')>0 or instr(ClientSysType,'Windows NT')>0 THEN mac END) PC,
      COUNT(DISTINCT mac) allcount,
      COUNT(DISTINCT CASE WHEN instr(ClientSysType,'Intel Mac OS')<=0 and instr(ClientSysType,'Windows NT')<=0 and instr(ClientSysType,'iPhone')<=0 and instr(ClientSysType,'iPad')<=0 and instr(ClientSysType,'iPad')<=0 and instr(ClientSysType,'Android')<=0 THEN mac END) Other
    FROM wifi.wifilog
    WHERE mallid IN ($mallidlist) AND date BETWEEN $date_condition
    GROUP BY mallid,date
  ) t1 LEFT JOIN (
    SELECT
      date,
      Mallid,
      COUNT(DISTINCT CASE WHEN AuthenticationType=1 THEN mac END) SMSCount,
      COUNT(DISTINCT CASE WHEN AuthenticationType=2 THEN mac END) WechatCount,
      COUNT(DISTINCT CASE WHEN AuthenticationType=3 THEN mac END) MemberCount,
      COUNT(DISTINCT CASE WHEN AuthenticationType=4 THEN mac END) AppCount,
      COUNT(DISTINCT mac) AllAuCount,
      COUNT(DISTINCT CASE WHEN newflag THEN mac END) AuNewCount,
      COUNT(DISTINCT CASE WHEN AuthenticationType=1 and newflag THEN mac END) AuSMSCount,
      COUNT(DISTINCT CASE WHEN AuthenticationType=2 and newflag THEN mac END) AuWechatCount,
      COUNT(DISTINCT CASE WHEN AuthenticationType=3 and newflag THEN mac END) AuMemberCount,
      COUNT(DISTINCT CASE WHEN AuthenticationType=4 and newflag THEN mac END) AuAppCount
    FROM wifi.new_au_info
    WHERE mallid IN ($mallidlist) AND date BETWEEN $date_condition
    GROUP BY mallid,date
  ) t2 ON t1.mallid=t2.mallid AND t1.date=t2.date LEFT JOIN (
    SELECT date,mallid,COUNT(DISTINCT mobile) SMSSendCount
    FROM wifi.SMSHistory
    WHERE date BETWEEN $date_condition AND mallid IN ($mallidlist) AND type=900
    GROUP BY mallid,date
  ) t3 ON t1.mallid=t3.mallid AND t1.date=t3.date LEFT JOIN (
    SELECT
      date,
      mallid,
      COUNT(DISTINCT CASE WHEN action=70101 THEN uuid END) as PVCount,
      COUNT(DISTINCT CASE WHEN action=70102 THEN uuid END) as failcount
    FROM mq.trackpage_view
    WHERE mallid IN ($mallidlist) AND date BETWEEN $date_condition AND containersource=6
    GROUP BY mallid,date
  ) t5 ON t1.mallid=t5.mallid AND t1.date=t5.date LEFT JOIN (
    SELECT
      date,
      mallid,
      COUNT(DISTINCT CASE WHEN action=70105 THEN uuid END) wechatviewcount
    FROM mq.trackevent
    WHERE mallid in ($mallidlist) AND date between $date_condition AND containersource=6
    GROUP BY mallid,date
  ) t6 ON t1.mallid=t6.mallid AND t1.date=t6.date
"

else
  echo "没传参数 !!!
  请传 3个参数 : 1.数据重算起始时间(20170629) 2.数据结束时间 (今日时间:20170629) 3.mallid 列表 134,10218 "
fi
