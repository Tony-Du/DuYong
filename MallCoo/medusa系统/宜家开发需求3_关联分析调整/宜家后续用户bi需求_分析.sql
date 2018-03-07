
--去过商场的总人数  
with stg_total as (    
select count(mallcoo_id) as total
  from ( 
        select mallcoo_id       
          from medusa.kudu_event_extend 
         where id between '1-' and '1-z' 
           and projectid = '1' 
           and (month between to_date(trunc('2016-01-01','mm')) and to_date(trunc('2016-05-31','mm')) 
                and day between '2016-01-01' and '2016-05-31') 
           and (event_type = 25001) 
         group by mallcoo_id 
        ) t
),
+-----------+
| total_cnt |
+-----------+
| 37715     |
+-----------+

        


+-------------------------+-------+
| shop_commercialtypename | num   |
+-------------------------+-------+
| 鞋包                    | 156   |
| 饰品                    | 1196  |
| 未知                    | 37344 |
| 服装                    | 447   |
| 餐饮                    | 1939  |
| 药品/护理/保健          | 150   |
+-------------------------+-------+


with stg_kudu_event as (
select mallcoo_id,
       nvl(shop_commercialtypename,'未知') as shop_commercialtypename
  from medusa.kudu_event_extend 
 where id between '1-' and '1-z' 
   and projectid = '1' 
   and (month between to_date(trunc('2016-01-01','mm')) and to_date(trunc('2016-05-31','mm')) 
        and day between '2016-01-01' and '2016-05-31') 
   and (event_type = 25001) 
 group by mallcoo_id,shop_commercialtypename
)
 

--去过各种品牌的人数
with stg_num as (
select nvl(shop_commercialtypename,'未知') as shop_commercialtypename,
       count(mallcoo_id) as num
  from stg_kudu_event 
 group by shop_commercialtypename
), 

--去过两个不同的品牌的人数  
stg_supportcount as (
select shop_commercialtypename_a,
       shop_commercialtypename_b,
       supportcount
  from (
        select shop_commercialtypename_a, 
               shop_commercialtypename_b,
               count(distinct mallcoo_id) as supportcount,
               rank() over (partition by shop_commercialtypename_a, shop_commercialtypename_b order by count(distinct mallcoo_id) desc) as rnk  			   
          from (
                select a.mallcoo_id, 
                       a.shop_commercialtypename as shop_commercialtypename_a,
                       b.shop_commercialtypename as shop_commercialtypename_b
                  from stg_kudu_event a
                  join stg_kudu_event b 
                    on a.mallcoo_id = b.mallcoo_id
               ) t
		 where shop_commercialtypename_a = '${常量}'
         group by shop_commercialtypename_a, shop_commercialtypename_b 
		 
       ) a
 where shop_commercialtypename_a <> shop_commercialtypename_b
   and rnk <= 10
)
,
stg_supportcount_num_supportcountb as (
select a.shop_commercialtypename_a
      ,a.shop_commercialtypename_b
      ,a.supportcount
      ,b.num
      ,c.num as supportcountb
  from stg_supportcount a 
  join stg_num b 
    on a.shop_commercialtypename_a = b.shop_commercialtypename
  join stg_num c
    on a.shop_commercialtypename_b = c.shop_commercialtypename
)
 



+---------------------------+---------------------------+--------------+
| shop_commercialtypename_a | shop_commercialtypename_b | supportcount |
+---------------------------+---------------------------+--------------+
| 餐饮                      | 饰品                      | 90           |
| 饰品                      | 餐饮                      | 90           |
| 服装                      | 餐饮                      | 35           |
| 鞋包                      | 服装                      | 10           |
| 餐饮                      | 药品/护理/保健            | 8            |
| 药品/护理/保健            | 服装                      | 5            |
| 服装                      | 饰品                      | 37           |
| 服装                      | 未知                      | 405          |
| 饰品                      | 未知                      | 994          |
| 餐饮                      | 未知                      | 1841         |
| 餐饮                      | 鞋包                      | 7            |
| 未知                      | 鞋包                      | 145          |
| 服装                      | 鞋包                      | 10           |
| 服装                      | 药品/护理/保健            | 5            |
| 鞋包                      | 药品/护理/保健            | 4            |
| 未知                      | 服装                      | 405          |
| 饰品                      | 鞋包                      | 18           |
| 药品/护理/保健            | 未知                      | 127          |
| 未知                      | 饰品                      | 994          |
| 药品/护理/保健            | 鞋包                      | 4            |
| 未知                      | 药品/护理/保健            | 127          |
| 餐饮                      | 服装                      | 35           |
| 未知                      | 餐饮                      | 1841         |
| 鞋包                      | 饰品                      | 18           |
| 药品/护理/保健            | 饰品                      | 9            |
| 饰品                      | 服装                      | 37           |
| 鞋包                      | 餐饮                      | 7            |
| 鞋包                      | 未知                      | 145          |
| 饰品                      | 药品/护理/保健            | 9            |
| 药品/护理/保健            | 餐饮                      | 8            |
+---------------------------+---------------------------+--------------+


+---------------------------+---------------------------+--------------+-------+---------------+
| shop_commercialtypename_a | shop_commercialtypename_b | supportcount | num   | supportcountb |
+---------------------------+---------------------------+--------------+-------+---------------+
| 餐饮                      | 药品/护理/保健            | 8            | 1939  | 150           |
| 服装                      | 鞋包                      | 10           | 447   | 156           |
| 服装                      | 未知                      | 405          | 447   | 37344         |
| 饰品                      | 未知                      | 994          | 1196  | 37344         |
| 餐饮                      | 未知                      | 1841         | 1939  | 37344         |
| 药品/护理/保健            | 餐饮                      | 8            | 150   | 1939          |
| 餐饮                      | 鞋包                      | 7            | 1939  | 156           |
| 药品/护理/保健            | 饰品                      | 9            | 150   | 1196          |
| 鞋包                      | 服装                      | 10           | 156   | 447           |
| 服装                      | 餐饮                      | 35           | 447   | 1939          |
| 饰品                      | 餐饮                      | 90           | 1196  | 1939          |
| 服装                      | 饰品                      | 37           | 447   | 1196          |
| 药品/护理/保健            | 鞋包                      | 4            | 150   | 156           |
| 饰品                      | 鞋包                      | 18           | 1196  | 156           |
| 鞋包                      | 未知                      | 145          | 156   | 37344         |
| 未知                      | 服装                      | 405          | 37344 | 447           |
| 服装                      | 药品/护理/保健            | 5            | 447   | 150           |
| 鞋包                      | 药品/护理/保健            | 4            | 156   | 150           |
| 药品/护理/保健            | 未知                      | 127          | 150   | 37344         |
| 未知                      | 药品/护理/保健            | 127          | 37344 | 150           |
| 未知                      | 饰品                      | 994          | 37344 | 1196          |
| 餐饮                      | 饰品                      | 90           | 1939  | 1196          |
| 饰品                      | 药品/护理/保健            | 9            | 1196  | 150           |
| 药品/护理/保健            | 服装                      | 5            | 150   | 447           |
| 未知                      | 餐饮                      | 1841         | 37344 | 1939          |
| 鞋包                      | 饰品                      | 18           | 156   | 1196          |
| 未知                      | 鞋包                      | 145          | 37344 | 156           |
| 饰品                      | 服装                      | 37           | 1196  | 447           |
| 鞋包                      | 餐饮                      | 7            | 156   | 1939          |
| 餐饮                      | 服装                      | 35           | 1939  | 447           |
+---------------------------+---------------------------+--------------+-------+---------------+





with stg_total as (    
select count(mallcoo_id) as total
  from ( 
        select mallcoo_id       
          from medusa.kudu_event_extend 
         where id between '1-' and '1-z' 
           and projectid = '1' 
           and (month between to_date(trunc('2016-01-01','mm')) and to_date(trunc('2016-05-31','mm')) 
                and day between '2016-01-01' and '2016-05-31') 
           and (event_type = 25001) 
         group by mallcoo_id 
        ) t
),
stg_num as (
select nvl(shop_commercialtypename,'未知') as shop_commercialtypename,
       count(mallcoo_id) as num
  from (
        select mallcoo_id,shop_commercialtypename
          from medusa.kudu_event_extend 
         where id between '1-' and '1-z' 
           and projectid = '1' 
           and (month between to_date(trunc('2016-01-01','mm')) and to_date(trunc('2016-05-31','mm')) 
                and day between '2016-01-01' and '2016-05-31') 
           and (event_type = 25001) 
         group by mallcoo_id,shop_commercialtypename
        ) t
 group by shop_commercialtypename
),
stg_supportcount as (
 with stg_kudu_event as (
 select mallcoo_id,
        nvl(shop_commercialtypename,'未知') as shop_commercialtypename
   from medusa.kudu_event_extend 
  where id between '1-' and '1-z' 
    and projectid = '1' 
    and (month between to_date(trunc('2016-01-01','mm')) and to_date(trunc('2016-05-31','mm')) 
         and day between '2016-01-01' and '2016-05-31') 
    and (event_type = 25001) 
  group by mallcoo_id,shop_commercialtypename
 )
 select shop_commercialtypename_a,
        shop_commercialtypename_b,
        supportcount
   from (
        select shop_commercialtypename_a, 
               shop_commercialtypename_b,
               count(distinct mallcoo_id) as supportcount 
          from (
                select a.mallcoo_id, 
                       a.shop_commercialtypename as shop_commercialtypename_a,
                       b.shop_commercialtypename as shop_commercialtypename_b
                  from stg_kudu_event a
                  join stg_kudu_event b 
                    on a.mallcoo_id = b.mallcoo_id
               ) t
         group by shop_commercialtypename_a, shop_commercialtypename_b 
        ) a
  where shop_commercialtypename_a <> shop_commercialtypename_b
),
stg_supportcount_num_supportcountb as (
select a.shop_commercialtypename_a
      ,a.shop_commercialtypename_b
      ,a.supportcount
      ,b.num
      ,c.num as supportcountb
  from stg_supportcount a 
  join stg_num b 
    on a.shop_commercialtypename_a = b.shop_commercialtypename
  join stg_num c
    on a.shop_commercialtypename_b = c.shop_commercialtypename
)
select t1.shop_commercialtypename_a
      ,t1.shop_commercialtypename_b
      ,t1.supportcount
      ,nvl(cast(t1.num - t1.supportcount as decimal(38,4)),0) as non_supportcount --非关联人数 
      ,nvl(cast(t1.supportcount/t2.total as decimal(38,4)),0) as support --支持度    
      ,nvl(cast(t1.supportcount/t1.num as decimal(38,4)),0) as confidence  --置信度            
      ,nvl(cast(t1.supportcount/t1.supportcountb as decimal(38,4)),0) as reverseconfidence  --反向置信度         
      ,nvl(cast((t1.supportcount/t1.num)/(t1.supportcountb/t2.total)as decimal(38,4)),0) as lift  --提升度
  from stg_supportcount_num_supportcountb t1
 cross join stg_total t2 
 where t1.shop_commercialtypename_a = '餐饮';

 
 
 

+---------------------------+---------------------------+--------------+------------------+---------+------------+-------------------+--------+
| shop_commercialtypename_a | shop_commercialtypename_b | supportcount | non_supportcount | support | confidence | reverseconfidence | lift   |
+---------------------------+---------------------------+--------------+------------------+---------+------------+-------------------+--------+
| 餐饮                      | 药品/护理/保健            | 8            | 1931.0000        | 0.0002  | 0.0041     | 0.0533            | 1.0373 |

| 服装                      | 鞋包                      | 10           | 437.0000         | 0.0002  | 0.0223     | 0.0641            | 5.4085 |
| 服装                      | 未知                      | 405          | 42.0000          | 0.0107  | 0.9060     | 0.0108            | 0.9150 |
| 饰品                      | 未知                      | 994          | 202.0000         | 0.0263  | 0.8311     | 0.0266            | 0.8393 |
| 餐饮                      | 未知                      | 1841         | 98.0000          | 0.0488  | 0.9494     | 0.0492            | 0.9588 |
| 药品/护理/保健            | 餐饮                      | 8            | 142.0000         | 0.0002  | 0.0533     | 0.0041            | 1.0373 |
| 餐饮                      | 鞋包                      | 7            | 1932.0000        | 0.0001  | 0.0036     | 0.0448            | 0.8727 |
| 药品/护理/保健            | 饰品                      | 9            | 141.0000         | 0.0002  | 0.0600     | 0.0075            | 1.8920 |
| 鞋包                      | 服装                      | 10           | 146.0000         | 0.0002  | 0.0641     | 0.0223            | 5.4085 |
| 服装                      | 餐饮                      | 35           | 412.0000         | 0.0009  | 0.0782     | 0.0180            | 1.5229 |
| 饰品                      | 餐饮                      | 90           | 1106.0000        | 0.0023  | 0.0752     | 0.0464            | 1.4636 |
| 服装                      | 饰品                      | 37           | 410.0000         | 0.0009  | 0.0827     | 0.0309            | 2.6102 |
| 饰品                      | 鞋包                      | 18           | 1178.0000        | 0.0004  | 0.0150     | 0.1153            | 3.6385 |
| 未知                      | 服装                      | 405          | 36939.0000       | 0.0107  | 0.0108     | 0.9060            | 0.9150 |
| 药品/护理/保健            | 鞋包                      | 4            | 146.0000         | 0.0001  | 0.0266     | 0.0256            | 6.4470 |
| 服装                      | 药品/护理/保健            | 5            | 442.0000         | 0.0001  | 0.0111     | 0.0333            | 2.8124 |
| 鞋包                      | 药品/护理/保健            | 4            | 152.0000         | 0.0001  | 0.0256     | 0.0266            | 6.4470 |
| 鞋包                      | 未知                      | 145          | 11.0000          | 0.0038  | 0.9294     | 0.0038            | 0.9387 |
| 药品/护理/保健            | 未知                      | 127          | 23.0000          | 0.0033  | 0.8466     | 0.0034            | 0.8550 |
| 未知                      | 药品/护理/保健            | 127          | 37217.0000       | 0.0033  | 0.0034     | 0.8466            | 0.8550 |
| 未知                      | 饰品                      | 994          | 36350.0000       | 0.0263  | 0.0266     | 0.8311            | 0.8393 |
| 餐饮                      | 饰品                      | 90           | 1849.0000        | 0.0023  | 0.0464     | 0.0752            | 1.4636 |
| 饰品                      | 药品/护理/保健            | 9            | 1187.0000        | 0.0002  | 0.0075     | 0.0600            | 1.8920 |
| 药品/护理/保健            | 服装                      | 5            | 145.0000         | 0.0001  | 0.0333     | 0.0111            | 2.8124 |
| 未知                      | 餐饮                      | 1841         | 35503.0000       | 0.0488  | 0.0492     | 0.9494            | 0.9588 |
| 鞋包                      | 饰品                      | 18           | 138.0000         | 0.0004  | 0.1153     | 0.0150            | 3.6385 |
| 未知                      | 鞋包                      | 145          | 37199.0000       | 0.0038  | 0.0038     | 0.9294            | 0.9387 |
| 饰品                      | 服装                      | 37           | 1159.0000        | 0.0009  | 0.0309     | 0.0827            | 2.6102 |
| 鞋包                      | 餐饮                      | 7            | 149.0000         | 0.0001  | 0.0448     | 0.0036            | 0.8727 |
| 餐饮                      | 服装                      | 35           | 1904.0000        | 0.0009  | 0.0180     | 0.0782            | 1.5229 |
+---------------------------+---------------------------+--------------+------------------+---------+------------+-------------------+--------+
 


--20171113 第一次修改：添加过滤条件，只显示每组支持数前10的内容
with stg_kudu_event as(
select mallcoo_id,if(shopname is null or trim(cast(shopname as string))='', '未知', cast(shopname as string))itemset 
from medusa.kudu_event 
where id BETWEEN '1-' AND '1-z' 
AND projectid='1' 
and(month BETWEEN to_date(trunc('2016-01-01','MM')) AND to_date(trunc('2016-05-31','MM')) 
AND day BETWEEN '2016-01-01' AND '2016-05-31')and(event_type=25001)
group by mallcoo_id,shopname
)
select itemset_a,
       itemset_b,
       supportcount,
	   rnk
  from (
        select itemset_a, 
               itemset_b,
               supportcount, 
               rank() over (partition by itemset_a order by supportcount desc) as rnk 
         from (
                 select itemset_a,
                        itemset_b,
                        count(distinct mallcoo_id)as supportcount       
                 from(
                      select a.mallcoo_id,
                             a.itemset as itemset_a,
                             b.itemset as itemset_b 
                       from stg_kudu_event a 
                       join stg_kudu_event b 
                         on a.mallcoo_id = b.mallcoo_id
                     )t 
                WHERE(itemset_a='GXG') and itemset_a <> itemset_b
                group by itemset_a,itemset_b
              )a 
        ) b
where rnk <= 10 ;   
  