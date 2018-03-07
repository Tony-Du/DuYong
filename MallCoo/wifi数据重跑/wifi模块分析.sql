
一、各MAC 时间 以及 wifi all_macs

INSERT OVERWRITE TABLE wifi.mac_time  --
SELECT
       mallid,
       mac,
       round(sum(if((unix_timestamp(logouttime)-unix_timestamp(logintime))/60>20,20,
	                (unix_timestamp(logouttime)-unix_timestamp(logintime))/60))),   --time
       --单位 ：分钟。 大于20分钟，就等于20分钟
       date
  FROM wifi.wifilog
 WHERE date BETWEEN ${start} AND ${end}
 GROUP BY mallid,mac,date;
 
-- 计算 一个mall下 不同mac（不同人）的 一天使用 wifi 的时间
-- wifi.wifilog 从flume来，但是针对业务是哪些业务的数据还需要注意下？
---------------------------------
Mallcoo-hive (default)> desc wifi.mac_time;
OK
mallid              	bigint              	                    
mac                 	string              	                    
time                	double              	                    
date                	string
----------------------------------
CREATE TABLE `wifi.wifilog`(
  `mobile` string, 
  `mac` string, 
  `logintime` timestamp, 
  `logouttime` timestamp, 
  `apno` string, 
  `clientsystype` string, 
  `totalinoctets` bigint, 
  `createtime` timestamp, 
  `shopid` bigint, 
  `openid` string)
PARTITIONED BY ( 
  `date` string, 
  `mallid` bigint)
LOCATION
  'hdfs://hadoop000:9000/user/root/hive/wifi.db/wifilog'
---------------------------------


SET hive.auto.convert.join=false;  --不使用map端join
INSERT OVERWRITE TABLE wifi.all_macs PARTITION(date,mallid)  --每个商场所有的新增mac
SELECT n.mac,
       n.date day,
       n.date,
	   n.mallid	   
  FROM (
        SELECT mac,mallid,min(date) date
          FROM wifi.wifilog
         WHERE date BETWEEN ${start} AND ${end}
         GROUP BY date,mallid,mac
       ) n 
  LEFT JOIN (
             SELECT mac,mallid
               FROM wifi.all_macs
              WHERE date<${start}
            ) a 
	ON n.mallid=a.mallid AND n.mac=a.mac
 WHERE a.mac IS NULL;     --新增

 
--------------------------------------
CREATE TABLE `wifi.all_macs`(
  `mac` string, 
  `day` string)
PARTITIONED BY ( 
  `date` string, 
  `mallid` bigint)
--------------------------------------

INSERT OVERWRITE TABLE wifi.all_au_macs PARTITION(date)  --每个商场所有认证的mac
SELECT n.mac,n.mallid,n.date
  FROM (
        SELECT mac,
               mallid,
	      	   min(date) date
        FROM wifi.AuthenticationInfo     --(wifi 认证表)
        WHERE date BETWEEN ${start} AND ${end}
        GROUP BY mallid,mac
       ) n 
  LEFT JOIN (
             SELECT mac,mallid FROM wifi.all_au_macs WHERE date<'${start}'
            ) a 
    ON n.mallid=a.mallid AND n.mac=a.mac
 WHERE a.mac IS NULL;    --新增

 
 
-- date=`date -d yesterday +"%Y%m%d"` 
insert overwrite table wifi.AuthenticationInfo partition(date) 
select mallid
      ,mac
	  ,authenticationtype
	  ,WeixinID
	  ,createtime
	  ,to_hive_date(createtime) 
from mallcoo.AuthenticationInfo 
where to_hive_date(createtime)='$date';


CREATE EXTERNAL TABLE `mallcoo.AuthenticationInfo`(
  `mallid` bigint COMMENT 'from deserializer', 
  `mac` string COMMENT 'from deserializer', 
  `authenticationtype` int COMMENT 'from deserializer', 
  `weixinid` string COMMENT 'from deserializer', 
  `createtime` timestamp COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'com.mongodb.hadoop.hive.BSONSerDe' 
STORED BY 
  'com.mongodb.hadoop.hive.MongoStorageHandler' 
WITH SERDEPROPERTIES ( 
  'mongo.columns.mapping'='{\"MallID\":\"MallID\",\"Mac\":\"Mac\",
							\"AuthenticationType\":\"AuthenticationType\",\"WeixinID\":\"WeixinID\",
							\"CreateTime\":\"CreateTime\"}', 
  'serialization.format'='1')
LOCATION
  'hdfs://hadoop000:9000/user/root/hive/mallcoo.db/authenticationinfo'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='false', 
  'mongo.uri'='mongodb://lottery1:27012,lottery2:27012/MallcooExt.AuthenticationInfo', 
  'numFiles'='0', 
  'numRows'='-1', 
  'rawDataSize'='-1', 
  'totalSize'='0', 
  'transient_lastDdlTime'='1461894195')

  
===============================================================================
二、Eyes Wifi 每日统计


INSERT INTO TABLE wifi.WifiDailyCount_mongos
SELECT concat(a.mallid,'_',a.date),
       a.mallid,
       '',
       NULL,
       NULL,
       NULL,
       NULL,
       TO_MONGO_DATE_TIME(a.date) Date,
       FROM_UNIXTIME(UNIX_TIMESTAMP()),
       a.TotalNumber,
       a.LoginCount,
       0 ValidCount,
       0 WorkerCount,
       COALESCE(round(COALESCE(c.MemberCount, a.thirdpartyMemberCount)/a.LoginCount, 4), 0.0) VIPRate,
       COALESCE(round(b.First/a.LoginCount,4),0.0) FirstRate,
       0 UsedRate,
       COALESCE(c.MemberCount,0),
       COALESCE(a.AvgFlow,0),
       COALESCE(d.AvgRetentionTime,0),
       if(e.adclickrate>0 and e.adclickrate<1,e.adclickrate,1) ADClickRate,
       0 HighestNum,
       TO_MONGO_DATE_TIME(a.date) HighestTime,
       0 Avg
  FROM (
		SELECT mallid,
			   count(*) TotalNumber,
			   COUNT(DISTINCT mac) LoginCount,
			   COUNT(DISTINCT CASE WHEN shopid=1 THEN mac END) ThirdPartyMemberCount,
			   sum(TotalInOctets)/count(mac) AvgFlow,
			   date
		  FROM wifi.wifilog
		 WHERE date BETWEEN ${start} AND ${end}
		 GROUP BY mallid,date
       ) a 
  LEFT JOIN (
			 SELECT mallid
			       ,COUNT(DISTINCT mac) First --认证mac数
				   ,date
			   FROM wifi.all_au_macs
			  WHERE date BETWEEN ${start} AND ${end}
			  GROUP BY mallid,date
            ) b 
    ON a.mallid=b.mallid AND a.date=b.date 
  LEFT JOIN (
			 SELECT a.mallid
			       ,count(a.mac) MemberCount
				   ,date
			   FROM (
				     SELECT mallid
					       ,mac
						   ,date
					   FROM wifi.record
				      WHERE date BETWEEN ${start} AND ${end}
				    ) a 
			   JOIN probe.member_mac b   --过滤哪些内容？？
			     ON a.mallid=b.mallid AND a.mac=b.mac
			  GROUP BY a.mallid,a.date
            ) c 
    ON a.mallid=c.mallid AND a.date=c.date 
  LEFT JOIN (
             SELECT mallid
			       ,round(sum(time)/count(mac)) AvgRetentionTime
				   ,date
               FROM wifi.mac_time
              GROUP BY mallid,date
            ) d 
    ON a.mallid=d.mallid AND a.date=d.date 
  LEFT JOIN (
             SELECT mallid
			       ,COUNT(CASE WHEN act=2 THEN mac END)/COUNT(CASE WHEN act=1 THEN mac END) ADClickRate
				   ,date
               FROM wifi.AdvertisementInfo   --( wifi 广告表)
              WHERE date BETWEEN ${start} AND ${end}
              GROUP BY mallid,date
            ) e 
    ON a.mallid=e.mallid AND a.date=e.date;

			
INSERT INTO TABLE wifi.WifiLoginSiteDailyCount_mongos
SELECT concat(a.mallid,'_',b.date,'_',a.sn),
       a.mallid,
       '',
       NULL,
       NULL,
       NULL,
       NULL,
       TO_MONGO_DATE_TIME(b.date),
       FROM_UNIXTIME(UNIX_TIMESTAMP()),
       a.sn,
       a.floorid,
       count(b.mac),
       0,
       a.mac
  FROM (
		SELECT mallid,floorid,sn,lcase(mac) as mac
		  FROM wifi.MallAPInfo
		LATERAL VIEW EXPLODE(macs) SUBVIEW as mac
       ) a 
  JOIN (
        SELECT DISTINCT mac,mallid,apno,date
          FROM wifi.wifilog
         WHERE date BETWEEN ${start} AND ${end}
       ) b ON a.mac=lcase(b.apno)
 GROUP BY a.mallid,a.floorid,a.sn,a.mac,date;



 
INSERT INTO TABLE wifi.WifiHourlyCount_mongos
SELECT concat(d.mallid,'_',d.date,'_',d.in),
       d.mallid,
       '',
       NULL,
       NULL,
       NULL,
       NULL,
       TO_MONGO_DATE_TIME(d.date),
       FROM_UNIXTIME(UNIX_TIMESTAMP(d.date)),
       if(d.newCount is NULL,0,d.newCount)+if(e.RetentionCount is NULL,0,e.RetentionCount) count,
       d.in hour,
       if(e.RetentionCount is NULL,0,e.RetentionCount),
       if(d.newCount is NULL,0,d.newCount)
  FROM (
        SELECT mallid
		      ,hour(logintime) in
			  ,date
			  ,COUNT(DISTINCT mac) newCount
          FROM wifi.wifilog
         WHERE date BETWEEN ${start} AND ${end}
         GROUP BY mallid,hour(logintime),date
       ) d 
  LEFT JOIN (
             SELECT mallid
			       ,in
				   ,COUNT(DISTINCT mac) RetentionCount
				   ,date
               FROM (
                     SELECT mallid,mac,in,date
                       FROM (
                             SELECT * 
							   FROM wifi.wifilog
                              WHERE date BETWEEN ${start} AND ${end}
                            ) b 
                       JOIN appinterface.hour c
                      WHERE hour(b.logintime)<c.in AND hour(b.logouttime)>=c.in
                    ) a
             GROUP BY mallid,in,date
            ) e 
    ON d.mallid=e.mallid AND d.in=e.in AND d.date=e.date;



INSERT INTO TABLE wifi.WifiAvgTimeFrequencyCount_mongos
SELECT concat(mallid,'_',date,'_',time),
       mallid,
       '',
       NULL,
       NULL,
       NULL,
       NULL,
       TO_MONGO_DATE_TIME(date),
       FROM_UNIXTIME(UNIX_TIMESTAMP()),
       time,
       sum(count)
  FROM (
        SELECT mallid,if(time>300,301,time) time,count(time) count,date
          FROM wifi.mac_time
         GROUP BY mallid,time,date
       ) a
 GROUP BY mallid,time,date;

=========================================================================================================
 
三、设备 统计 (新版)

INSERT INTO TABLE wifi.WifiAuMobileSystemDailyCount_mongos
SELECT concat(t1.mallid,'_',t1.date),     --ID
       t1.mallid,                         --MallID
       '',                                --Mallname
       NULL,                              --shopid
       NULL,                              --shopname
       NULL,                              --mall_area_id
       NULL,                              --mall_area_name
       TO_MONGO_DATE_TIME(t1.date),       --date
       FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss'), --create_time 
       COALESCE(t5.PVCount,0),                                --打开portal页人数(uuid 去重)
       COALESCE(t2.AllAuCount,0),                             --验证成功设备数
       COALESCE(t1.allcount,0),                               --上网设备数
       COALESCE(t5.fc,0),                                     --登录失败(登录埋点数-成功人数)
       COALESCE(t1.allcount,0)-COALESCE(t2.AllAuCount,0),     --自动登录人数(总人数-认证人数)
       COALESCE(t3.SMSSendCount,0),                           --
       COALESCE(t2.SMSCount,0),                               --
       COALESCE(t6.wechatviewcount,0),                        --
       COALESCE(t2.WechatCount,0),                            --
       COALESCE(t4.WeChatFollowCount,0),                      --
       COALESCE(t2.AppCount,0),                               --
       COALESCE(t2.MemberCount,0),                            --
       COALESCE(t1.iPhone+t1.iPad,0),                         --
       COALESCE(t1.iPhone,0),                                 --
       COALESCE(t1.iPad,0),                                   --
       COALESCE(t1.PC,0),                                     --
       COALESCE(t1.Android,0),                                --
       COALESCE(t1.Other,0),                                  --
       0,                                                     --
       COALESCE(t2.AuNewCount,0),                             --
       COALESCE(t2.AuSMSCount,0),                             --
       COALESCE(t2.AuWechatCount,0),                          --
       COALESCE(t2.AuAppCount,0),                             --
       COALESCE(t2.AuMemberCount,0)                           --
  FROM (
		SELECT mallid,
		       date,
		       COUNT(DISTINCT CASE WHEN instr(ClientSysType,'iPhone')>0 THEN mac END) iPhone,
		       COUNT(DISTINCT CASE WHEN instr(ClientSysType,'iPad')>0 THEN mac END) iPad,
		       COUNT(DISTINCT CASE WHEN instr(ClientSysType,'Android')>0 THEN mac END) Android,
		       COUNT(DISTINCT CASE WHEN instr(ClientSysType,'Intel Mac OS')>0 or instr(ClientSysType,'Windows NT')>0 THEN mac END) PC,
		       COUNT(DISTINCT mac) allcount,
		       count(DISTINCT CASE WHEN instr(ClientSysType,'Intel Mac OS')<=0 AND instr(ClientSysType,'Windows NT')<=0 AND instr(ClientSysType,'iPhone')<=0 AND instr(ClientSysType,'iPad')<=0 AND instr(ClientSysType,'iPad')<=0 AND instr(ClientSysType,'Android')<=0 THEN mac END) Other
		  FROM wifi.wifilog
		 WHERE date BETWEEN ${start} AND ${end}
		 GROUP BY mallid,date
       ) t1 
  LEFT JOIN (
		     SELECT Mallid,
		            date,
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
		      WHERE date BETWEEN ${start} AND ${end}
		      GROUP BY mallid,date
            ) t2 
    ON t1.mallid=t2.mallid AND t1.date=t2.date 
  LEFT JOIN (
		     SELECT mallid
			       ,count(distinct mobile) SMSSendCount
				   ,date
		       FROM wifi.SMSHistory
		      WHERE date BETWEEN ${start} AND ${end} AND type=900
		      GROUP BY mallid,date
            ) t3 
    ON t1.mallid=t3.mallid AND t1.date=t3.date 
  LEFT JOIN (
		     SELECT a.mallid mallid
			       ,COUNT(DISTINCT a.weixinid) WeChatFollowCount
				   ,a.date date
		       FROM (
		             SELECT DISTINCT mallid,weixinid,date
		               FROM wifi.AuthenticationInfo
		              WHERE date BETWEEN ${start} AND ${end} 
					    AND AuthenticationType=2 
						AND weixinid IS NOT NULL
		            ) a 
			   JOIN (
		             SELECT DISTINCT mallid,openid,date
		               FROM pqr.UserSNSEvent
		              WHERE date BETWEEN ${start} AND ${end} 
					    AND event=1 
					    AND openid IS NOT NULL
		            ) b 
				 ON a.date=b.date AND a.weixinid=b.openid AND a.mallid=b.mallid
		      GROUP BY a.mallid,a.date
            ) t4 
    ON t1.mallid=t4.mallid AND t1.date=t4.date 
  LEFT JOIN (
		     SELECT mallid,
		            date,
		            COUNT(DISTINCT CASE WHEN action=70101 THEN uuid END) PVCount,
		            COUNT(DISTINCT CASE WHEN action=70102 THEN uuid END) fc
		       FROM mq.trackpage_view
		      WHERE date BETWEEN ${start} AND ${end} 
			    AND containersource=6
		      GROUP BY mallid,date
            ) t5 
    ON t1.mallid=t5.mallid AND t1.date=t5.date 
  LEFT JOIN (
		     SELECT mallid
			       ,COUNT(DISTINCT CASE WHEN action=70105 THEN uuid END) wechatviewcount
				   ,date 
		       FROM mq.trackevent 
		      WHERE date BETWEEN ${start} AND ${end} 
			    AND containersource=6 
		      GROUP BY mallid,date
            ) t6 
    ON t1.mallid=t6.mallid AND t1.date=t6.date 

==============================================================================================
四、Wifi  广告 每日统计

INSERT INTO TABLE wifi.WifiAdDailyClickCount_mongos
SELECT concat(mallid,'_','$end'),
       mallid,
       '',
       NULL,
       NULL,
       NULL,
       NULL,
       TO_MONGO_DATE_TIME($end),
       FROM_UNIXTIME(UNIX_TIMESTAMP()),
       COALESCE(PortalCount,0),
       COALESCE(PShowCount,0),
       COALESCE(round(PortalCount/PShowCount,4),0.0) PClickRate,
       COALESCE(BrowserCount,0),
       COALESCE(BShowCount,0),
       COALESCE(round(BrowserCount/BShowCount,4),0.0) BClickRate,
       0,
       COALESCE(HomeCount,0),
       COALESCE(WifiCount,0),
       COALESCE(WelcomeCount,0)
  FROM (
        SELECT mallid,
               COUNT(CASE WHEN type IN (2,3,4) AND act=2 THEN mac END) PortalCount,
               COUNT(CASE WHEN type IN (2,3,4) AND act=1 THEN mac END) PShowCount,
               COUNT(CASE WHEN type=1 AND act=2 THEN mac END) BrowserCount,
               COUNT(CASE WHEN type=1 AND act=1 THEN mac END) BShowCount,
               COUNT(CASE WHEN type=2 AND act=2 THEN mac END) HomeCount,
               COUNT(CASE WHEN type=3 AND act=2 THEN mac END) WifiCount,
               COUNT(CASE WHEN type=4 AND act=2 THEN mac END) WelcomeCount
          FROM wifi.AdvertisementInfo
         WHERE date='$end'
         GROUP BY mallid
       ) a;

INSERT INTO TABLE wifi.WifiAdDailyClickSortCount_mongos
SELECT concat(mallid,'_',$end,'_',adid),
       mallid,
       '',
       NULL,
       NULL,
       NULL,
       NULL,
       TO_MONGO_DATE_TIME($end),
       FROM_UNIXTIME(UNIX_TIMESTAMP()),
       adid,
       adname,
       COUNT(CASE WHEN act=2 THEN mac END),
       COALESCE(round(count(CASE WHEN act=2 THEN mac END)/count(CASE WHEN act=1 THEN mac END),4),0.0),
       COUNT(CASE WHEN act=1 THEN mac END)
  FROM wifi.AdvertisementInfo
 WHERE date='${end}'
 GROUP BY mallid,adid,adname;

INSERT INTO TABLE wifi.WifiAdHistoryClickCount_mongos
SELECT concat(mallid,'_',$end,'_',adid),
       mallid,
       '',
       NULL,
       NULL,
       NULL,
       NULL,
       TO_MONGO_DATE_TIME($end),
       FROM_UNIXTIME(UNIX_TIMESTAMP()),
       adid,
       adname,
       COUNT(CASE WHEN act=2 THEN mac END) count,
       COUNT(DISTINCT TO_HIVE_DATE(CreateTime)) days,
       COUNT(CASE WHEN act=2 THEN mac END)/COUNT(DISTINCT TO_HIVE_DATE(CreateTime))
  FROM wifi.AdvertisementInfo
 GROUP BY mallid,adid,adname;


======================================================
五、wifi 进场日志

INSERT OVERWRITE TABLE wifi.customermacinfo PARTITION(date)
SELECT t1.mallid,
       t1.mac,
       t1.mobile,
       t1.count,
       t1.Duration,
       t1.LastDuration,
       t1.flow,
       t1.LastLoginTime,
       COALESCE(t1.LastSN,'--'),
       t1.FirstLoginTime,
       COALESCE(t1.FirstSN,'--'),
       COALESCE(t2.PClickCount,0),
       COALESCE(t2.PShowCount,0),
       COALESCE(t2.BClickCount,0),
       COALESCE(t2.BShowCount,0),
       t1.date
  FROM (
        SELECT mac,
               mallid,
               date,
               count(*) count,
               substring(max(concat(logintime,mobile)),20) mobile,
               round(sum(cast(logouttime as bigint)-cast(logintime as bigint))/60,4) Duration,
               substring(max(concat(logintime,round((cast(logouttime as bigint)-cast(logintime as bigint))/60),4)),20) LastDuration,
               round(sum(totalinoctets/(1024*1024)),4) Flow,      --流量
               max(logintime) LastLoginTime,
               substring(max(concat(logintime,apno)),20) LastSN,
               min(logintime) FirstLoginTime,
               substring(min(concat(logintime,apno)),20) FirstSN
          FROM wifi.wifilog
         WHERE date=${end}
         GROUP BY mac,mallid,date
       ) t1 
  LEFT JOIN (
             SELECT mallid,
                    mac,
                    TO_HIVE_DATE(CreateTime) date,
                    COUNT(CASE WHEN type=2 AND act=1 THEN mac END) PShowCount,
                    COUNT(CASE WHEN type=2 AND act=2 THEN mac END) PClickCount,
                    COUNT(CASE WHEN type=1 AND act=1 THEN mac END) BShowCount,
                    COUNT(CASE WHEN type=1 AND act=2 THEN mac END) BClickCount
               FROM wifi.AdvertisementInfo  --(wifi 广告表)
              WHERE date=${end}
              GROUP BY mallid,mac,TO_HIVE_DATE(CreateTime)
            ) t2 
    ON t1.mallid=t2.mallid AND t1.mac =t2.mac AND t1.date=t2.date;