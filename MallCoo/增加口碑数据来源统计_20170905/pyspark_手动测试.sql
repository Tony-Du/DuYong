pyspark

sqlContext.sql("create temporary function date_parse as 'cn.mallcoo.udf.DateParse'")
sqlContext.sql("create temporary function area_loc as 'cn.mallcoo.udf.AreaLoc'")
sqlContext.sql("create temporary function collect_all as 'cn.mallcoo.udf.CollectUDAF'")
sqlContext.sql("create temporary function to_hive_date as 'cn.mallcoo.udf.HiveDate'")
sqlContext.sql("create temporary function to_mongo_date as 'cn.mallcoo.udf.MongoDate'")
sqlContext.sql("create temporary function FloorID2Name as 'cn.mallcoo.udf.FloorID2Name'")
sqlContext.sql("create temporary function stay_time as 'cn.mallcoo.udf.StayTime'")
sqlContext.sql("create temporary function to_mongo_date_time as 'cn.mallcoo.udf.MongoDateTime'")
sqlContext.sql("create temporary function mac_format as 'cn.mallcoo.udf.MacFormat'")
sqlContext.sql("create temporary function get_monday as 'cn.mallcoo.udf.GetMonday'")
sqlContext.sql("create temporary function GET_SUNDAY as 'cn.mallcoo.udf.GetSunday'")
sqlContext.sql("CREATE TEMPORARY FUNCTION GET_MONTH_FIRST_DAY AS 'cn.mallcoo.udf.GetMonthFirstDay'")
sqlContext.sql("CREATE TEMPORARY FUNCTION GET_MONTH_LAST_DAY AS 'cn.mallcoo.udf.GetMonthLastDay'")




date='20170919'
sqlContext.sql(
 """
    INSERT INTO TABLE eyes.MongodbCardMemberDailyCount
    SELECT 
        concat('{0}','_',t1.mallid),
        t1.mallid,
        null,null,null,null,null,
        TO_MONGO_DATE_TIME({0}),
        from_unixtime(unix_timestamp()),
        null,
        COALESCE(t1.count,0) count,
        COALESCE(t1.opencardcount,0) opencardcount,
        COALESCE(t1.bindingcardcount,0) bindingcardcount,
        COALESCE(t1.ioscount,0) ioscount,
        COALESCE(t1.androidcount,0) androidcount,
        COALESCE(t1.lappcount,0) lappcount,
        COALESCE(t1.wxcount,0) wxcount,
        COALESCE(t1.appcount,0) appcount,
		COALESCE(t1.koubeicount,0) koubeicount,
        0,
        COALESCE(t2.UpgradeCount,0) UpgradeCount,
        COALESCE(t2.DegradeCount,0) DegradeCount
        from 
        (select 
        mallid,
        count(DISTINCT uid) count,
        count(DISTINCT case when creator = 'vipopen' then uid end) opencardcount,
        count(DISTINCT case when creator='vipbind' then uid end) bindingcardcount,
        count(DISTINCT case when datasource=2 then uid end) ioscount,
        count(DISTINCT case when datasource=1 then uid end) androidcount,
        count(DISTINCT case when datasource=3 then uid end) lappcount,
        count(DISTINCT case when datasource=5 then uid end) wxcount,
        count(DISTINCT case when datasource=1 OR datasource=2 then uid end) appcount,
        count(DISTINCT case when datasource=7 then uid end) koubeicount    --增加口碑数据  需要增加字段
    FROM customer.mallcard 
    WHERE TO_HIVE_DATE(CreateTime)={0}
    GROUP BY mallid) t1
    left join (
               select mallid,
                      count(distinct Case When GradeType = 0 then userid End) as UpgradeCount,
                      count(distinct Case When GradeType = 1 then userid End) as DegradeCount 
                 from crm.GradeRecord 
                where TO_HIVE_DATE(CreateTime)={0} 
                group by mallid 
              ) t2 on (t1.mallid = t2.mallid )
 """.format(date))



sqlContext.sql(
 """
 INSERT INTO TABLE eyes.MemberGradeCount_mongos
 select concat(t1.mallid,'_',t1.cardtypeid),
        t1.mallid,
        '',
        to_mongo_date_time({0}),
        from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'),
        t1.cardtypeid,
        t1.CardTitle,
        COALESCE(t1.count,0) 
   from (
         select mallid,
                COALESCE(cardtypeid,-1) cardtypeid,
                Case When cardtypeid is null then '未知卡类型' else max(cardtype) End as CardTitle,
                count(distinct uid) as count 
           from customer.mallcard 
          group by mallid,cardtypeid
        ) t1
 """.format(date))

