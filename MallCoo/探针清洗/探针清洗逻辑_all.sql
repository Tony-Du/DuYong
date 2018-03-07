--  01-Load(imoobox).sh
LOAD DATA LOCAL INPATH '/apps/probe/imoobox/$day' OVERWRITE INTO TABLE probe.probelocdata_imoobox partition(date=$day);


CREATE TABLE `probe.probelocdata_imoobox`(
  `time` bigint, 
  `probemac` string, 
  `mac` string, 
  `rssi` int, 
  `scan` string)
PARTITIONED BY ( 
  `date` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ','

  
INSERT OVERWRITE TABLE probe.probeinfo    --探针配置信息
SELECT mallid
      ,shopid
      ,NULL area
      ,NULL activity
      ,1 bindtype      --1 店铺， 2 区域
      ,MAC_FORMAT(mac) mac
      ,position
      ,COALESCE(filterrssi,-70) filterrssi
      ,NULl FloorID
      ,status
  FROM mallcoo.probe         -- mongodb://mallcoo1:27018,mallcoo2:27018/Mallcoo.Probe
 WHERE status=1              -- 表示正常
   AND MAC_FORMAT(mac) IS NOT NULL   
 UNION  
SELECT mallid
      ,shopid
      ,AreaName area
      ,ActivityName activity
      ,if(bindtype=0,1,bindtype) bindtype
      ,MAC_FORMAT(mac) mac
      ,position
      ,if(rssi is null OR rssi=0,-70,rssi * -1) filterrssi
      ,FloorID
      ,status
  FROM mallcoo.probe2       -- mongodb://HadoopUser:dTHK93J3cf@mongoss:27017/MallcooStatistics.Probe?authSource=admin
 WHERE status=1 
   AND MAC_FORMAT(mac) IS NOT NULL;  
  

INSERT OVERWRITE TABLE probe.probelocdata_source PARTITION(date)
SELECT time
      ,i.mallid
      ,i.shopid
      ,MAC_FORMAT(probemac,true)
      ,MAC_FORMAT(p.mac,true)
      ,rssi
      ,area
      ,activity
      ,bindtype
      ,floorid
      ,date
  FROM probe.probelocdata_imoobox p
  JOIN probe.probeinfo i 
    ON probemac=MAC_FORMAT(i.mac,true) AND date='$day'
 WHERE rssi>filterrssi        -- filterrssi 阈值   rssi表示场强
   AND rssi<>0 
   AND instr(p.mac,'047e4a') = 0     --INT instr(STRING str, STRING substr) 查找字符串str中子字符串substr 第一次出现的位置，0表示没有对应的substr
   AND instr(p.mac,'e4956e') = 0;


   
-- 02-Clean.sh
INSERT OVERWRITE TABLE probe.probelocdata
SELECT time
      ,mallid
      ,shopid
      ,area
      ,activity
      ,bindtype
      ,probemac
      ,mac
      ,rssi
      ,floorid
  FROM probe.probelocdata_source
 WHERE date='${day}' 
   AND time IS NOT NULL 
   AND mac IS NOT NULL 
   AND rssi IS NOT NULL 
   AND rssi > -120;
 
use probe;
ALTER TABLE probelocdata1 ADD IF NOT EXISTS PARTITION(date=$day); 
ALTER TABLE probelocdata2 ADD IF NOT EXISTS PARTITION(date=$day);
-- 上述两个表 probelocdata1 和 probelocdata2 是在 mr中处理的
 
 
--  03-Worker_New.sh  

-- 计算从业人员
SET mapred.job.name=probe_day_03-probelocdata_all&worker;

INSERT OVERWRITE TABLE probe.probelocdata_all PARTITION(date,mallid)
SELECT t.mac
      ,from_unixtime(unix_timestamp())
      ,t.retentiontime
      ,t.in
      ,t.out
      ,t.shopid
      ,s.floorid
      ,t.date
      ,t.mallid
FROM (
      SELECT MAC_FORMAT(mac) mac,
             retentiontime,
             in,
             out,
             shopid,
             date,
             mallid
        FROM probe.probelocdata2
       WHERE date='${day}' 
         AND shopid IS NOT NULL
     ) t 
LEFT JOIN probe.shop1 s 
  ON s.mallid=t.mallid AND s.id=t.shopid;
  
  
CREATE TABLE `probe.shop1`(
  `mallid` bigint, 
  `mallname` string, 
  `id` bigint, 
  `name` string, 
  `mallareaid` bigint, 
  `mallareaname` string, 
  `floorid` int, 
  `floorname` string, 
  `x` double, 
  `y` double, 
  `pinyin` string, 
  `brand` string, 
  `commercialtypename` string, 
  `subcommercialtypename` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://hadoop000:9000/user/root/hive/probe.db/shop1'

insert overwrite table probe.shop1
select mallid,
       mallname,
       id,
       name,
       mallareaid,
       mallareaname,
       COALESCE(ShopFloorList[0].floorid,floorid),
       COALESCE(ShopFloorList[0].floorname,'--'),
       COALESCE(ShopFloorList[0].InnerLocationX,0.0),
       COALESCE(ShopFloorList[0].InnerLocationY,0.0),
       pinyin,
       brand,
       CommercialTypeName,
       SubCommercialTypeName
  from mallcoo.shop          -- mongodb://mallcoo1:27018,mallcoo2:27018/Mallcoo.Shop
 union
select mallid,
       mallname,
       id,
       name,
       mallareaid,
       mallareaname,
       COALESCE(ShopFloorList[0].floorid,floorid),
       COALESCE(ShopFloorList[0].floorname,'--'),
       COALESCE(ShopFloorList[0].InnerLocationX,0.0),
       COALESCE(ShopFloorList[0].InnerLocationY,0.0),
       pinyin,
       brand,
       CommercialTypeName,
       SubCommercialTypeName
  from mallcoo_sp.shop;       -- mongodb://sp1:28001,sp2:28001/SP_Mall.Shop
  
  

INSERT OVERWRITE TABLE probe.worker_t1 PARTITION(date)
SELECT DISTINCT mac
      ,mallid
      ,shopid
      ,date
  FROM probe.probelocdata_all
 WHERE date='${day}' 
   AND (hour(in)<10 OR hour(out)>=21) 
   AND retentiontime>60;


   
DROP TABLE probe.probe_worker;        --为什么先删除再建？？
CREATE TABLE probe.probe_worker(
mac STRING,
shopid BIGINT
) 
PARTITIONED BY(mallid BIGINT);



INSERT INTO TABLE probe.probe_worker PARTITION(mallid)
SELECT mac
      ,shopid
      ,mallid
  FROM probe.worker_t1
 WHERE date>date_sub(TO_MONGO_DATE(${day}),30) AND date<='${day}' --取一个月前 至 今天 范围 的数据
 GROUP BY mac,shopid,mallid
HAVING count(mac) > 10;



-- 计算非从业人员
SET mapred.job.name=probe_clean_04:probelocdata_worker_no;
set hive.exec.dynamic.partition.mode=nostrick;
insert overwrite table probe.probelocdata_worker_no partition(date='${day}',mallid)
select m1.mac
      ,m1.createtime
      ,m1.retentiontime
      ,m1.in
      ,m1.out
      ,m1.shopid
      ,m1.floorid,m1.mallid 
  from (select * from probe.probelocdata_all where date='${day}') m1
  left join  probe.probe_worker m2 
    on m1.mallid =m2.mallid and m1.shopid=m2.shopid and m1.mac=m2.mac
  left join probe.ShopExcludeMac m3  -- mongodb://HadoopUser:dTHK93J3cf@mongoss:27017/ShopStatistics.ShopExcludeMac?authSource=admin
    on m1.mallid =m3.mallid and lcase(regexp_replace(m1.mac,':','')) = lcase(regexp_replace(m3.mac,':','')) 
 where m2.mac is null 
   and m3.mac is null;


   
--  新用户表
SET mapred.job.name=probe_clean_08-all_macs;
set hive.auto.convert.join=false;
INSERT overwrite TABLE probe.all_macs PARTITION(date='${day}',mallid)
SELECT DISTINCT(n.mac)
      ,n.shopid
      ,n.mallid
  FROM (select * from probe.probelocdata_worker_no where date='${day}') n
  LEFT JOIN (
             SELECT mac
                   ,shopid
                   ,mallid 
               FROM probe.all_macs 
              WHERE date < '${day}'
            ) a 
    ON n.mallid=a.mallid and n.shopid=a.shopid AND n.mac=a.mac 
 WHERE a.mac IS NULL;


INSERT OVERWRITE TABLE probe.activity_all_macs PARTITION(date,mallid)
SELECT x.mac
      ,x.Activity
      ,time
      ,date
      ,x.mallid
FROM (
      SELECT MAC_FORMAT(mac) mac
            ,mallid
            ,Activity
            ,min(date) date
            ,min(in) time
        FROM probe.probelocdata2
       WHERE date='${day}' 
         and Activity IS NOT NULL
       GROUP BY mac,mallid,Activity
) x 
LEFT JOIN (
     SELECT mac
           ,mallid
           ,Activity
       FROM probe.activity_all_macs
      WHERE date<'${day}'
) y 
ON x.mac=y.mac and x.mallid=y.mallid and x.Activity=y.Activity
WHERE y.mac is null;



-- 04-Other.sh  探针数据 其他数据 计算

-- 汇总停留信息
SET mapred.job.name=probe_clean_05:probe.stayinfo;
set hive.exec.dynamic.partition.mode=nostrick;
insert overwrite table probe.stayinfo partition(date)
select shopid
      ,mac
      ,sum(unix_timestamp(out,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(in,'yyyy-MM-dd HH:mm:ss'))
      ,count(*)
      ,mallid
      ,date
  from probe.probelocdata_worker_no         -- 非从业人员 即 顾客
 where to_mongo_date(date) between ${where}
 group by shopid,mac,mallid,date;

--where="'$day' and '$day'"

CREATE TABLE `probe.stayinfo`(
  `shopid` bigint, 
  `mac` string, 
  `seconds` bigint, 
  `time` int, 
  `mallid` bigint)
PARTITIONED BY ( 
  `date` string)


-- 收银机信息
SET mapred.job.name=probe_clean_06:Consumption.LargeMerchandise_rssi;
INSERT OVERWRITE TABLE Consumption.LargeMerchandise_rssi PARTITION(date,mallid)
SELECT p.shopid,
       MAC_FORMAT(p.mac),
       p.time,
       p.rssi,
       MAC_FORMAT(p.probemac),
       p.date,
       p.mallid
  from (
        SELECT shopid
              ,mac
              ,time
              ,rssi
              ,mallid
              ,probemac
              ,date
          FROM probe.probelocdata1                
         WHERE shopid IS NOT NULL 
           AND to_mongo_date(date) between ${where}
       ) p 
  join Consumption.CashRegisterProbe c   -- c 中的mac 是 probe mac
    on MAC_FORMAT(c.mac)=MAC_FORMAT(p.probemac) and p.shopid=c.shopid
  join (
        SELECT mac 
              ,shopid 
              ,date 
          FROM probe.stayinfo
         WHERE to_mongo_date(date) between ${where} 
       ) s 
    on MAC_FORMAT(s.mac)=MAC_FORMAT(p.mac) and p.shopid=s.shopid AND p.date=s.date;

--where="'$day' and '$day'"	
	
desc probe.probelocdata1  ;
OK
mac                 	string              	                    
mallid              	bigint              	                    
shopid              	bigint              	                    
time                	timestamp           	                    
timeseconds         	bigint              	                    
probemac            	string              	                    
rssi                	int                 	                    
area                	string              	                    
activity            	string 
floorid             	int                 	                    
date                	string              	                    
	 	 
# Partition Information	 	 
# col_name            	data_type           	comment             
	 	 
date                	string


INSERT OVERWRITE TABLE Consumption.CashRegisterProbe
SELECT t.mallid,t.shopid,p.mac
FROM Consumption.Type t
JOIN probe.probeinfo p
ON t.mallid=p.mallid AND t.shopid=p.shopid
WHERE t.type=3 AND p.Position=1;




--  /apps/script/medusa/insert_event/1.shop.sh $insert_action "$where"

    -- insert_action=$1      # insert_action='into'
    -- insert_condition=${2//-/}       # "'$day' and '$day'"

-- 3.到达店铺 
SELECT mallid
      ,mallname
	  ,result
	  ,event_name
	  ,count(*) 
FROM (
    SELECT
        t1.mallid
	   ,t1.mallname
	   ,'CustomerShop' event_name
       ,kudu_upsert('hadoop002','medusa_event_warehouse',
            'id',concat(unix_timestamp(t2.time),'-',t1.mallid,'_',concat('mac_', t2.mac),'-',10004),
            'distinct_id',concat('mac_',t2.mac),
            'ts',unix_timestamp(t2.time),
            'time',t2.time,
            'hour',concat(from_unixtime(unix_timestamp(t2.time),'HH'),':00:00'),
            'event_name','CustomerShop',
            'event_type',10004,
            'event_source','探针',
            'event_scene','线下',
            'duration',retentiontime,
            'is_member',if(t4.mac IS NULL, FALSE, TRUE),
            'is_new',if(t3.mac IS NULL, FALSE, TRUE),
            'mac',t2.mac,
            'shopname',name,
            'mallid',t1.mallid,
            'mallname',t1.mallname,
            'day',TO_MONGO_DATE(t2.date)
        ) result 
    FROM (SELECT * FROM medusa.ProjectToMall ${project}) t1 
	JOIN (
          SELECT *,concat(TO_MONGO_DATE(date),' ',from_unixtime(unix_timestamp(in),'HH:mm:ss')) time 
		    FROM probe.probelocdata_worker_no 
           WHERE mallid in (SELECT mallid FROM medusa.ProjectToMall ${project}) 
		     AND date BETWEEN ${insert_condition}
          ) t2 
	  ON t1.mallid=t2.mallid
    LEFT JOIN (
               SELECT * FROM probe.all_macs 
                WHERE mallid in (SELECT mallid FROM medusa.ProjectToMall ${project}) 
		          AND date BETWEEN ${insert_condition}
              ) t3 
	  ON t2.mallid=t3.mallid AND t2.date=t3.date AND t2.mac=t3.mac AND t2.shopid=t3.shopid
    LEFT JOIN probe.member_mac t4 
	  ON t2.mallid=t3.mallid AND t2.mac=t4.mac
    LEFT JOIN probe.shop1 t6 
	  ON t2.shopid=t6.id AND t2.mallid=t6.mallid
) m 
GROUP BY mallid,mallname,result,event_name;



SELECT mallid,mallname,result,event_name,count(*) 
FROM (
    SELECT
      a.mallid,mallname,event_name,
      kudu_upsert('hadoop002','medusa_event_warehouse',
          'id',concat(unix_timestamp(time),'-',a.mallid,'-',concat('mac_', a.mac),'-',activity_name,'-',activity_area,'-',event_type),
          'distinct_id',concat('mac_', a.mac),
          'ts',unix_timestamp(time),
          'time',time,
          'hour',concat(from_unixtime(unix_timestamp(time),'HH'),':00:00'),
          'event_name',event_name,
          'event_type',event_type,
          'event_source','探针',
          'event_scene','线下',
          'duration',retentiontime,
          'activity_name',activity_name,
          'activity_area',activity_area,
          'floor_id',a.FloorID,
          'floor_name',FloorName,
          'is_member',v.mac IS NOT NULL,
          'is_new',n.mac IS NOT NULL,
          'mac',a.mac,
          'mallid',a.mallid,
          'mallname',MallName,
          'day',TO_MONGO_DATE(time)
        ) result
    FROM (
      SELECT
        MAC_FORMAT(mac) mac,
        retentiontime,
        activity activity_name,
        area activity_area,
        m.mallid mallid,
        CONCAT(TO_MONGO_DATE(date),' ',DATE_PARSE(in, 'HH:mm:ss')) time,
        m.FloorID floorid,
        FloorName,
        MallName,
        'CustomerActivity' event_name,
        10005 event_type,
        date
      FROM (
        SELECT mallid,floors.id FloorID,floors.name FloorName,name MallName
        FROM (
            SELECT mallid,floorlist,name
            FROM mongo.mall
            WHERE mallid in (SELECT mallid FROM medusa.ProjectToMall ${project})
        ) x
        LATERAL VIEW explode(floorlist) subview AS floors
      ) m JOIN (
        SELECT MAC_FORMAT(mac) mac,retentiontime,activity,area,mallid,FloorID,date,in
        FROM probe.probelocdata2
        WHERE mallid in (SELECT mallid FROM medusa.ProjectToMall ${project}) AND date BETWEEN ${insert_condition} AND activity IS NOT NULL
      ) p ON m.mallid=p.mallid and m.FloorID=p.FloorID
    ) a
    LEFT JOIN (
        SELECT mac,activity,date,mallid 
        FROM probe.activity_all_macs 
        WHERE mallid in (SELECT mallid FROM medusa.ProjectToMall ${project}) AND date BETWEEN ${insert_condition}
    ) n ON a.mallid=n.mallid AND a.date=n.date AND a.mac=n.mac AND a.activity_name=n.activity
    LEFT JOIN probe.member_mac v ON a.mallid=v.mallid AND a.mac=v.mac
) s
GROUP BY mallid,mallname,result,event_name;



