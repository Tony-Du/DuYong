


select case when nvl(b.wechat_follow_cnt,0) = 0 then 0 else round(nvl(a.open_app_page_cnt,0)/wechat_follow_cnt, 4) end as active_rate
from (
    select count(distinct mallcoo_id) as open_app_page_cnt
    from medusa.kudu_event
    where id between '3-' and '3-z' 
    and projectid = '3'                               
    and (month between to_date(trunc('2016-01-01','mm')) and to_date(trunc('2017-12-30','mm')) 
        and day between '2016-01-01' and '2017-12-30')  
    and event_type = 13001   
    and event_source = '微信'
) a
cross join (
    select count(distinct mallcoo_id) as wechat_follow_cnt
    from medusa.kudu_event
    where id between '3-' and '3-z' 
    and projectid = '3'  
    and event_type = 13002 and follow_type = '关注'
) b



-- 月 --
with open_app_page as (
    select month, count(distinct mallcoo_id) as open_app_page_cnt
    from medusa.kudu_event
    where id between '69-' and '69-z' 
    and projectid = '69'                               
    and (month between to_date(trunc('2017-12-01','mm')) and to_date(trunc('2017-12-31','mm')) 
        and day between '2017-12-01' and '2017-12-31')  
    and event_type = 13001   
    and event_source = '微信'
    group by month
),
wechat_follow as (
select month, sum(wechat_follow_cnt) over(order by month) as wechat_follow_cnt
from (
    select month, count(distinct mallcoo_id) as wechat_follow_cnt
      from medusa.kudu_event
     where id between '69-' and '69-z' 
       and projectid = '69'  
       and event_type = 13002 and follow_type = '关注'
       and month <= to_date(trunc('2017-12-31','MM'))
       and day <= '2017-12-31'
     group by month
) a
)
select t1.month, 
       case when nvl(t2.wechat_follow_cnt,0) = 0 then 0 else round(nvl(t1.open_app_page_cnt,0)/t2.wechat_follow_cnt, 4) end as active_rate
from open_app_page t1
inner join wechat_follow t2
on t1.month = t2.month





