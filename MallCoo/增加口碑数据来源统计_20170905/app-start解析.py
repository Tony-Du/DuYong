#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import threading
import datetime

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import HiveContext
from pyspark import StorageLevel

from prepare0 import prepare                       #更新每天的appuuid_all和qmiddle.mq_newuuidj这两张表的数据
from register import appcla_1                      #计算eyes的app,wx,lapp的用户数指标
from behaviour import appcla_2                     #计算eyes的app,wx,lapp的打开次数与时长指标
from active import appcla_3                        #计算eyes的app,wx,lapp的用户活跃率指标
from user import user_cal                          #计算eyes的线上运营的注册用户统计指标
from card import card_cal                          #计算eyes的线上运营的商场卡会员指标
from extend import extend_cal                      #计算eyes的线上运营的部分数据
from park import park_cal                          #计算eyes的线上运营的车场分析指标
from week import week_cal                          #计算eyes的相应的周度指标
from retention import retention_cal                #
from click import click_cal                        #计算eyes的app,wx,lapp的页面点击和模块点击指标
from month import month_cal                        #计算eyes的相应的月度指标
from graph import graph_cal                        #
from channel import channel_count                  #
from cardvoucher import cardvoucher_cal            #
from crm import crm_cal                            #
if __name__ =="__main__":

    date=os.environ["date"]
    print("date:%s" % date)
    tag01=os.environ["tag01"]
    tag1=os.environ["tag1"]
    tag02=os.environ["tag02"]
    tag2=os.environ["tag2"]

    today=datetime.date.today()
    print("today:%s" % today)
    #day=datetime.timedelta(days=13)
    #now=today-day
    #print("now:%s"%now)
    week=datetime.datetime.strptime(date,"%Y%m%d")

    sparkConf=SparkConf().setAppName("appTask").setMaster("spark://hadoop000:7077,hadoop001:7077,hadoop002:7077").set("spark.akka.heartbeat.interval", "100")
	# 1.创建SparkConf对象，设置spark应用的配置信息。使用setMaster()可以设置Spark应用程序要连接的Spark集群的master节点的url
    sc = SparkContext(conf=sparkConf)
	# 2.创建SparkContext对象。在Spark中，SparkContext是Spark所有功能的一个入口.
    sqlContext = HiveContext(sc)
	# 3.开发Spark SQL程序，那么需要创建SQLContext、HiveContext对象

	
    sqlContext.sql("create temporary function udf3_mq as 'udf3_mq'") 	
    sqlContext.sql("create temporary function date2date as 'date2date'")
    sqlContext.sql("create temporary function todate as 'udf4'")

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

	
    lock=threading.Lock()
    tasks={}
    def add_Task(function,args):
        lock.acquire()
        tasks[function]=lock
        t=threading.Thread(target=function,args=args+(lock,))
        t.start()

    prepare(sc,sqlContext,date)

	# 相当于创建RDD，数据源是hive表
    uuidtemp=sqlContext.sql("select * from appuuid_all")
    #uuidtemp.persist(storageLevel=StorageLevel(False, True, False, False, 1))
    uuidtemp.registerTempTable("uuidtemp")
    #uuidtemp.persist(StorageLevel(True, True, False, True, 1))
    #sqlContext.cacheTable("uuidtemp")
    #uuidtemp.count()
    print("uuidtemp finished!!!")

    pages=sqlContext.sql(
      """
        SELECT
            s.AppSettingID AppSettingID,
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
      """.format(date))
    pages.registerTempTable("pages")
    # sqlContext.cacheTable("pages")
    # pages.count()
    print("pages finished!!!")

    events=sqlContext.sql(
      """
        SELECT
            s.AppSettingID AppSettingID,
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
            SELECT MallID
			      ,CreateTime
				  ,ContainerSource
				  ,Source
				  ,UUID
				  ,UserID
				  ,Loc
				  ,Action
				  ,refid
				  ,date
              FROM mq.TrackEvent
             WHERE date={0}
        ) te LEFT JOIN mongo.mall s ON te.MallID=s.MallID
      """.format(date))
    events.registerTempTable("events")
    sqlContext.cacheTable("events")
    events.count()
    print("events finished!!!")

    graph=sqlContext.sql(
      """
        SELECT
            MallID,
            sum(int_page_read_user) IntUser,
            sum(int_page_read_count) IntCount,
            sum(ori_page_read_user) OriUser,
            sum(ori_page_read_count) OriCount,
            sum(share_user) ShareUser,
            sum(share_count) ShareCount,
            sum(add_to_fav_user) AddUser,
            sum(add_to_fav_count) AddCount
        FROM mq.WXGraph
        WHERE Type=1 and date={0}
        GROUP BY MallID
      """.format(date))
    graph.registerTempTable("graph")
    sqlContext.cacheTable("graph")
    graph.count()
    print("graph finished!!!")


    add_Task(crm_cal,(sc,sqlContext,date))
    add_Task(appcla_1,(sc,sqlContext,date,tag01))
    add_Task(appcla_2,(sc,sqlContext,date,tag01))
    add_Task(extend_cal,(sc,sqlContext,date,tag01))
    add_Task(user_cal,(sc,sqlContext,date))
    add_Task(card_cal,(sc,sqlContext,date))
    add_Task(park_cal,(sc,sqlContext,date))
    add_Task(click_cal,(sc,sqlContext,date))
    add_Task(graph_cal,(sc,sqlContext,date))
    add_Task(channel_count,(sc,sqlContext,date))
    #add_Task(cardvoucher_cal,(sc,sqlContext,date))
    add_Task(appcla_3,(sc,sqlContext,date,tag01,tag1))

    if week.strftime("%w")=="0" :
        add_Task(week_cal,(sc,sqlContext,date))


    if today.strftime("%d")=="01":
        add_Task(month_cal,(sc,sqlContext,date,tag01,tag02,tag2))
        # print("execute retention")
        # add_Task(retention_cal,(sc,sqlContext,today))

    dateArray = datetime.datetime.strptime(date,"%Y%m%d")
    tomorrow = dateArray + datetime.timedelta(days=1)

    if tomorrow.strftime("%d")=="01":
        print("========retention========")
        add_Task(retention_cal,(sc,sqlContext,date))

    for f,l in tasks.iteritems():
        while l.locked():
            pass

    sqlContext.clearCache()
    sc.stop()
