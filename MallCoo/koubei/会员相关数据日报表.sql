

--会员增长
--口碑会员新增
--微信会员新增
with member as (
select a.mallid
      ,count(distinct a.uid) as member_usr_cnt
      ,count(distinct case when a.datasource = 5 then a.uid end) as wechat_member_usr_cnt
      ,count(distinct case when a.datasource = 7 then a.uid end) as koubei_member_usr_cnt
from customer.mallcard a 
where TO_HIVE_DATE(a.createtime) = '${src_file_day}'
group by a.mallid
), 
--会员消费

--会员消费活跃数
--会员消费金额

--口碑会员消费活跃数
--口碑会员消费金额

--微信会员活跃数
--微信会员消费金额
member_consume as (
select a.mallid
      ,count(distinct a.userid) as member_consum_active_cnt
      ,sum(a.amount) as member_consum_amount
      ,count(distinct case when b.datasource = 5 then a.userid end) as wechat_member_consum_active_cnt
      ,sum(case when b.datasource = 5 then a.amount end) as wechat_member_consum_amount
      ,count(distinct case when b.datasource = 7 then a.userid end) as koubei_member_consum_active_cnt
      ,sum(case when b.datasource = 7 then a.amount end) as koubei_member_consum_amount
from crm.consbonushistory a
join customer.mallcard b
on a.mallid = b.mallid and a.userid = b.uid 
where TO_HIVE_DATE(a.createtime) = '${src_file_day}'
and b.datasource is not null
group by a.mallid 
),
  
--积分活跃会员量
member_bonus as(
select mallid
      ,count(distinct a.userid) as bonus_member_active_cnt
from crm.bonushistoryuser a 
join customer.mallcard b 
on a.mallid = b.mallid and a.userid = b.uid 
where TO_HIVE_DATE(a.CreateTime) = '${src_file_day}'
group by a.mallid
),

--微信图文阅读人数
wechat_graph as (
select mallid
      ,sum(int_page_read_user) as wechat_graph_usr_cnt
from mq.WXGraph
where date = '${src_file_day}'
and type = 1 
group by mallid
),

--微信粉丝
wechat_fans as (
select mallid
      ,new_user as wechat_fans_cnt
from mq.wx_appcount     --数据不全
where date = '${src_file_day}'
)

--insert overwrite table mongos 
select concat('${src_file_day}', '_', a.mallid) as id
      ,a.mallid 
      ,TO_MONGO_DATE_TIME('${src_file_day}') as date
      ,from_unixtime(unix_timestamp()) as createtime
      ,nvl(m.member_usr_cnt, 0) as member_usr_cnt
      ,nvl(m.wechat_member_usr_cnt, 0) as wechat_member_usr_cnt
      ,nvl(m.koubei_member_usr_cnt, 0) as koubei_member_usr_cnt
      ,nvl(c.member_consum_active_cnt, 0) as member_consum_active_cnt
      ,nvl(c.member_consum_amount, 0) as member_consum_amount
      ,nvl(c.wechat_member_consum_active_cnt, 0) as wechat_member_consum_active_cnt
      ,nvl(c.wechat_member_consum_amount, 0) as wechat_member_consum_amount
      ,nvl(c.koubei_member_consum_active_cnt, 0) as koubei_member_consum_active_cnt
      ,nvl(c.koubei_member_consum_amount, 0) as koubei_member_consum_amount
      ,nvl(b.bonus_member_active_cnt, 0) as bonus_member_active_cnt
      ,nvl(g.wechat_graph_usr_cnt, 0) as wechat_graph_usr_cnt
      ,nvl(f.wechat_fans_cnt, 0) as wechat_fans_cnt
from mongo.mall a 
left join member m
on a.mallid = m.mallid
left join member_consume c
on a.mallid = c.mallid
left join member_bonus b
on a.mallid = b.mallid 
left join wechat_graph g
on a.mallid = g.mallid
left join wechat_fans f
on a.mallid = f.mallid



--建表 hive 与 mongo 的关联表
--统计库
create external table koubei.MongodbInsightMemberDailyCount (
id string,
mallid bigint,
date timestamp,
createtime timestamp,
member_usr_cnt bigint,
wechat_member_usr_cnt bigint,
koubei_member_usr_cnt bigint,
member_consum_active_cnt bigint,
member_consum_amount double,
wechat_member_consum_active_cnt bigint,
wechat_member_consum_amount double,
koubei_member_consum_active_cnt bigint,
koubei_member_consum_amount double,
bonus_member_active_cnt bigint,
wechat_graph_usr_cnt bigint,
wechat_fans_cnt bigint
)
stored by 'com.mongodb.hadoop.hive.MongoStorageHandler'
with serdeproperties (
'mongo.columns.mapping'='{\"id\":\"_id\",\"mallid\":\"MallID\",\"date\":\"Date\",\"createtime\":\"CreateTime\",
                          \"member_usr_cnt\":\"MemberUserCount\",\"wechat_member_usr_cnt\":\"WechatMemberUserCount\",
                          \"koubei_member_usr_cnt\":\"KoubeiMemberUserCount\",\"member_consum_active_cnt\":\"MemberConsumActiveCount\",
                          \"member_consum_amount\":\"MemberConsumAmount\",\"wechat_member_consum_active_cnt\":\"WechatMemberConsumActiveCount\",
                          \"wechat_member_consum_amount\":\"WechatMemberConsumAmount\",\"koubei_member_consum_active_cnt\":\"KoubeiMemberConsumActiveCount\",
                          \"koubei_member_consum_amount\":\"KoubeiMemberConsumAmount\",\"bonus_member_active_cnt\":\"BonusMemberActiveCount\",
                          \"wechat_graph_usr_cnt\":\"WechatGraphUserCount\",\"wechat_fans_cnt\":\"WechatFansCount\",}',
'serialization.format'='1'
)
tblproperties (
'mongo.uri'='mongodb://HadoopUser:dTHK93J3cf@mongoss:27017/MallcooStatistics.InsightMemberDailyCount?authSource=admin'
)



--svn 建目录 

口碑洞察 koebeiinsight










