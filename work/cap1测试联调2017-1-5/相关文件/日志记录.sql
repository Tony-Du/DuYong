2017-02-06 P:1.发现rptdata.fact_kesheng_sdk_new_device_hourly表中设备数、新增设备数从17号之后突然大幅度减少
			   原因：换了BDI环境之后，kettle job运行hive SQL之前设置的优化参数，导致关联取device_key的步骤只运行了
			         第一个union，没有取到全部的device_key
             2.各表最新分区停在了1月24号11点，而此时BDI中已经有一直到了2月5号的运行记录
			   原因：分区比BDI运行记录少是因为没有原始数据,动态分区不会产生
		   M:1.修改了stg.fact_kesheng_sdk_new_device_hourly_01表关联取device_key的表和代码(增加了phone_number)
		       目的：为了提高效率，之前用了3个join，改过之后是一个join

2017-02-07 M:1.将stg.fact_kesheng_sdk_new_device_hourly_01的kettle中的优化参数删除，BDI重新从1月17号9点开始执行
               (将BDI之前的运行记录删除)
             2.开始写新需求代码：播放事件
             3.一点接入有效激活清单部分日表BDI重新从1月17号开始执行			 
	
2017-02-08 M:1.将播放器的四张表的粒度调整为小时

2017-02-09 P:1.rptdata.fact_cpa_active_device_detail_daily BDI运行从2月份开始全部报错	
               原因：没有数据造成的。该表所取的数据是7天前的数据，1月24号以后没有数据，所以2月1号取的是1月25号的数据，刚好取不到，所以报错	
             2.BDI流程文件等待一直卡，可是等待的文件确实已经产生
               原因：在BDI换过环境之后，文件等待的路径没有改
             3.BDI流程不运行
               把流程删了重新创建一遍	
             4.文件等待一直卡
               先终止运行记录 不删除记录 再重新设置调度时间运行	
           M:1.修改rptdata.fact_cpa_active_device_detail_daily月留存和7日留存部分代码，将become_new_unix_time改成了become_new_dw_day
               目的：become_new_unix_time字段数据部分有问题，become_new_dw_day是新加字段，数据没有问题
			   
2017-02-15 M:1.开始做新版的Kesheng_SDK需求:intdata.kesheng_sdk_first_play;intdata.kesheng_sdk_play_wrong;
                                           intdata.kesheng_sdk_buffer_detail;intdata.kesheng_sdk_sub_play

2017-02-16 M:1.修改新版Kesheng_SDK需求代码，添加client_IP字段，删除视图，直接lateral view插入到表中。
             2.配置新版Kesheng_SDK需求DBI，运行调度
			 3.2016-10-05至2016-11-09的源数据表分区为天，所以手动执行kettle job,将此阶段每天的数据全部写入对应天数的00小时的分区中
			   使用shell执行(脚本里面每一行都是跑一天数据的kettle job执行命令)：
			   cat kesheng_sdk_deviceinfo20161005_20161109.sh | sed "s/kesheng_sdk_deviceinfo/kesheng_sdk_sub_play/g" | sed "s/pan.sh/kitchen.sh/" | sed "s/main.ktr/main.kjb/" > kesheng_sdk_sub_play20161005_20161109.sh
               nohup sh kesheng_sdk_sub_play20161005_20161109.sh > kesheng_sdk_sub_play20161005_20161109.log &
           P:1.新版Kesheng_SDK需求DBI任务偶尔出现错误，从11月16号之后全是错误。
               原因：hiveServer挂了，没启动起来	(错误的任务直接用恢复执行解决，无需重新调度)

2017-02-17 M:1.kesheng_sdk intdata层新增3张表：intdata.kesheng_sdk_pic_load；intdata.kesheng_sdk_detail_page_load；intdata.kesheng_sdk_search_page_load
			   
2017-02-20 M:1.kesheng_sdk intdata层又添加了2张表：intdata.kesheng_sdk_login 和 intdata.kesheng_sdk_login_wrong
           P:1.kesheng_sdk intdata层之前的7张表的调度流程周末运行的部分全部报错
		       原因：BDI又出问题了唉，坑爹的，全部恢复执行，下午的时候已经没有问题了

2017-02-23 M:1.开始写CPA二期的需求(ods->app->Hbase->后台)

2017-02-24 M:1.app.cpa_settlement_detail_monthly增加产品名称product_name 
               (添加字段默认添加在分区字段上一位,移动字段位置只改变字段名顺序，不改变表内各字段数据的顺序)
             2.rptdata.fact_cpa_active_device_detail_daily增加启动时间upload_unix_time、idfa、phone_number
             3.增加一点接入话单文件(日) cpa_settlement_bill_file_daily

2017-02-28 M:1.增加一点接入产品和渠道配置文件 使用shell执行HQL 并保存到本地和上传到FTP
             2.将cpa_copy_hdfs_to_ftp中的脚本文件修改：ftp_path作为脚本的第四个变量传入,不再定死
			 3.一点接入结算各表表名中settlement改为settle
			 
2017-03-01 P:1.BDI大部分流程异常终止
               手动执行，跳过依赖
			 2.device_detail 20170116之前的运行记录删不掉
			   不知道什么鬼
			 3.表分区传值失败
			   换个kettle模板，重新做kettle文件
			   检查BDI参数配置，注意添加#
		   M:1.配置产品配置文件shell、渠道配置文件shell BDI，调试
		     2.修改device_detail表,imei、idfa分开取值
		     3.device_detail 以及后来的表 重新从20170116开始调度
			 
2017-03-02 P:1.文件等待改了一个目录，结果报错
               原因：更改的目录权限问题,修改权限(755)		
           M:1.rptdata.fact_cpa_active_device_detail_daily修改之前的数据存储于rptdata.fact_cpa_active_device_detail_daily_bak
               app.cpa_settle_detail_monthly修改之前的数据存储于app.cpa_settle_detail_monthly_bak		   
               分区表结构修改之后，之前的分区无法插入新字段的值，要重新建表(原表改名成备用表以备份数据)

2017-03-03 P:1.new_device 0301数据量突然变大，0302数据量突然变小
               原因：0301产品换线 0302从10点开始没有源数据

2017-03-07 P:1.CPA数据联调，当天数据全部为0
               原因：当天数据天维度下还没跑完
			   '注'：CPA本地共享路径：/mnt/cpa/   业务部门直接从该共享路径拿数据
 			       一点接入本地路径：/mnt/cpa/bigdata/settle/bak   之后上传到FTP 业务部门从FTP上那拿数据

2017-03-10 M:1. 10.200.65.71 在测试集群上测试hive是否支持双字符的分隔符
             2. 生产集群 10.200.60.169 hadoop hadoop123!@#
                         10.200.60.170 hadoop hadoop123!@#
			             10.200.60.172 hadoop hadoop123!@#
                测试集群 10.200.65.71 thadoop thadoop123!@#	
				
2017-03-13 P:1.版本-1的总错误数是114,但是我们自己累加所有版本的总错误数是113,影响设备数也有这个问题
               原因：版本-1总错误数多一个 因为算总错误数的时候  有一个版本是空 而错误数也有一个 也算在总数里了
			         设备数的统计有去重，所以有可能各个版本和大于-1的数据
             2.CPA部分10.11.12号的天数据今天13号早上9点多才给，所以定时器都没有跑掉
               原因:11号凌晨的时候集群运行任务报错导致延迟	
             3.retention_daily留存率超过100%	
               原因：20170207重新跑过new_device之后,new_daily没有重跑导致新增用户数较小,分母较小(retention_daily从new_daily和new_device后的一张表取数据)			 
           M:1.session_start/session_end/exception 过滤版本为''的数据
             2.session_error 当idfa/imei为-998时 不计数	
             3.本月计划的CDH版本升级，是从5.4.7升级到5.10，跨越很大，这周各自把自己开发关键代码在测试环境上测一遍			 
			   hive的版本还是1.1
				
2017-03-14 P:1.启动次数应该大于活跃用户
               原因：前端活跃用户取成累计值了
             2.平均使用时长round后仍为0
			   未解决
           M:1.session_start/end/exception join右表去重(主要是对关联的字段去重)				
 			   join时on的条件如果有重复 那么也会关联出重复的数据 无论是左表还是右表
			   
2017-03-15 P:平均使用时长直播一小时,视频和影院都是0
             原因：json解析其中一个键的值存在回车换行符 导致ods层表该字段也带回车换行符 导致intdata层表时长字段只有一小部分取到了
                   值,大部分显示null 后面sum则总时长大幅度减小

2017-03-16 P：1.rptdata.fact_kesheng_sdk_exception_daily 14/15号 分区没有 数据没有 运行记录成功 手动运行BDI也没有 手动运行代码有数据
			    明天看一下是否有16号的分区
			  3.intdata.kesheng_sdk_active_device_hourly join后没有on条件 导致iOS部分数据异常
			  4.使用时长多大 (json有回车换行符到时其他字段存在大量null值;数据源有异常过大的数据)
			  5.渠道、版本累计用户百分比累加>1(未解决)
 			  6.活跃用户数>启动次数(并没有发现存在该问题)
		   M: 1.CPA、kesheng 流程设置失败重试
				
2017-03-17 P:1.有错误数却没有影响设备数 这种情况是 产生这些错误数的设备 它们的imei或idfa号没有取到 是空值  统计的时候对这些设备进行了过滤  就出现有错误却没设备
             2.rptdata.fact_kesheng_sdk_exception_daily 仍然没有产生16号的分区
			   重新做过kettle文件 从ktr变成kjb后 运行正常
			   '注意:'记得 error和ind_daily 16号的数据没有重跑 所以错误次数/设备数是有问题的
		   M:1.ind stg_ind new_active_daily  修改。。分母  
		       修改后 版本活跃用户占比 正常  但是累计、新增、活跃用户数有一点变化 变化不大
			   
2017-03-21 P:1.int层的session_start 昨天有3个小时BDI运行报错 重试多次后仍然报错 
               原因：通过yarn监测界面查看mr任务的log 发现数据源文件该三个小时存在0字节的文件 导致一直出问题
			         删除这些0字节文件后 BDI运行正常
		   M:1.异常指标使用udid计算影响设备数；异常数据过滤掉name=null,app_channel_id=null,app_version=null,udid=null
		       intdata.kesheng_sdk_exception 添加字段 udid
			 2.累计播放次数接口
			 
2017-03-22 P:1.new_device 2017031413 deviceinfo的product_key是全的 但是inner join 之后product_key只有10
               见03-23 P：3

2017-03-23 P:1.手机型号名称出现不正常名称
               原因 历史遗留就数据 当时一开始没能过滤掉这些数据 解决见M1
			 2.playstats_d从0304开始报错  其中一张表不存在导致
			 3.3月21号提出的'咪咕影院、咪咕视频，3月14号13点的新增用户数据为0'的问题已解决 
			   应该是当时手动测试和自动调度在临时表这一块冲突了 导致数据有问题 重新跑一遍解决
           M:1.修改rptdata.fact_kesheng_sdk_phone_mode_daily 添加 add_device_num>0 
		     2.影响设备数修改 加入生产环境
			 3.新增累计播放次数 rpt层全部加入生产环境  只剩app层没有做
			 
2017-03-25/26 M:1 hadoop升级5.10 169的kitchen.sh文件被修改 导致一些表没有分区	

2017-03-27 M:1.CPA2期ods/int/rpt/app/dmt BDI流程配置  '注:'APP层的last7day/last30day 运行有错误
             2.新增累计播放次数 app层全部加入生产环境

2017-03-28 P:1.从昨天23点开始 HDFS上就不再有kesheng的数据文件	
               原因：收集数据的flume挂掉了	

2017-03-29 P:1.播放量3个产品累加跟总播放量有较大差别
               原因：有值为NULL的产品 也算到总播放量了 
           M:1.修改rptdata的first_play nvl(product_key,'-998')
               修改rptdata的new_active where product_key<>'-998'
             2.重跑CPA部分29号的数据
             3.rptdata层过滤时长大于24小时的数据 测试后使用时长减少至正常范围内			 
             4.CAP2期intdata层过滤了null值数据
               记得放到服务器上  现在git上已经更新了			 
====================================================================================================================
检查数据：1、用count()检查各分区数据数量,查看数据量趋势
          2、查看数据源(原始json表 + HDFS的/user/hadoop/ods/kesheng/文件)
          3、不同维度下查看数据量
寻找原因：1、查看git的log修改日志





  
  应用层 Telnet、FTP和e-mail等
  运输层 TCP(传输控制协议,可靠)和UDP(用户数据报协议,不可靠)
  网络层 IP、ICMP和IGMP
  链路层 设备驱动程序及接口卡
  
  FTP是一种应用层协议,
  TCP是一种运输层协议,
  IP是一种网络层协议,
  而以太网协议则应用于链路层上。