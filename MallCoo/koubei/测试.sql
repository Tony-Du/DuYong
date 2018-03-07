

insert into table koubei.HbaseMember
select printf('%s_%06d', TO_HIVE_DATE(a.createtime), cast(a.mallid as bigint)) as id
      ,cast(a.mallid as bigint) as mallid
      ,TO_HIVE_DATE(a.createtime) as date
      ,count(distinct a.uid) as member_usr_cnt
      ,count(distinct case when a.datasource = 5 then a.uid end) as wechat_member_usr_cnt
      ,count(distinct case when a.datasource = 7 then a.uid end) as koubei_member_usr_cnt
from customer.mallcard a 
where TO_HIVE_DATE(a.createtime) between '20171230' and '20171230' 
group by a.mallid, TO_HIVE_DATE(a.createtime);

1515046653758



insert into table koubei.HbaseMemberConsume
select printf('%s_%06d', TO_HIVE_DATE(a.createtime), a.mallid) as id
      ,a.mallid
      ,TO_HIVE_DATE(a.createtime) as date
      ,count(distinct a.userid) as member_consum_active_cnt
      ,sum(a.amount) as member_consum_amount
      ,count(distinct case when b.datasource = 5 then a.userid end) as wechat_member_consum_active_cnt
      ,sum(case when b.datasource = 5 then a.amount end) as wechat_member_consum_amount
      ,count(distinct case when b.datasource = 7 then a.userid end) as koubei_member_consum_active_cnt
      ,sum(case when b.datasource = 7 then a.amount end) as koubei_member_consum_amount
from crm.consbonushistory a
join customer.mallcard b
on a.mallid = cast(b.mallid as bigint) and a.userid = b.uid 
where TO_HIVE_DATE(a.createtime) between '20171230' and '20171230' 
and b.datasource is not null
group by a.mallid,TO_HIVE_DATE(a.createtime) ;

insert into table koubei.HbaseMemberBonus
select printf('%s_%06d', TO_HIVE_DATE(a.createtime), cast(a.mallid as bigint)) as id
      ,cast(a.mallid as bigint) as mallid 
      ,TO_HIVE_DATE(a.createtime) as date
      ,count(distinct a.userid) as bonus_member_active_cnt
from crm.bonushistoryuser a 
join customer.mallcard b 
on a.mallid = b.mallid and a.userid = b.uid 
where TO_HIVE_DATE(a.createtime) between '20171230' and '20171230' 
group by a.mallid,TO_HIVE_DATE(a.createtime);


insert into table koubei.HbaseWechatGraph
select printf('%s_%06d', a.date, a.mallid) as id
      ,a.mallid
      ,a.date
      ,sum(int_page_read_user) as wechat_graph_usr_cnt
from mq.WXGraph a
where a.date between '20171230' and '20171230' 
and a.type = 1 
group by a.mallid, a.date;

insert into table koubei.HbaseWechatFans
select printf('%s_%06d', a.date, a.mallid) as id
      ,a.mallid
      ,a.date
      ,new_user as wechat_fans_cnt
from mq.wx_appcount a    
where a.date between '20171230' and '20171230' 




insert into table koubei.MongodbInsightMemberDailyCount
select id
      ,mallid 
      ,TO_MONGO_DATE_TIME(date) as date
      ,from_unixtime(unix_timestamp()) as createtime
      ,nvl(member_usr_cnt, 0) as member_usr_cnt
      ,nvl(wechat_member_usr_cnt, 0) as wechat_member_usr_cnt  
      ,nvl(koubei_member_usr_cnt, 0) as koubei_member_usr_cnt  
      ,nvl(member_consum_active_cnt, 0) as member_consum_active_cnt  
      ,nvl(member_consum_amount, 0) as member_consum_amount  
      ,nvl(wechat_member_consum_active_cnt, 0) as wechat_member_consum_active_cnt  
      ,nvl(wechat_member_consum_amount, 0) as wechat_member_consum_amount  
      ,nvl(koubei_member_consum_active_cnt, 0) as koubei_member_consum_active_cnt 
      ,nvl(koubei_member_consum_amount, 0) as koubei_member_consum_amount 
      ,nvl(bonus_member_active_cnt, 0) as bonus_member_active_cnt
      ,nvl(wechat_graph_usr_cnt, 0) as wechat_graph_usr_cnt  
      ,nvl(wechat_fans_cnt, 0) as wechat_fans_cnt  
from koubei.HbaseInsightMemberDailyCount
where id>='20171230_000000' AND id<='20171230_999999';