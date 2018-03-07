
--关联分析

-- CRM会员消费 的 商户一级业态（扩展）== 服装

-- 2016-01-01 ~ 2016-05-31

SELECT Itemset,
       COALESCE(cast(SupportCount/total as decimal(38,4)),0) Support, --支持度 
       COALESCE(cast(SupportCount/SupportCountB as decimal(38,4)),0) ReverseConfidence,  --反向置信度
       COALESCE(cast(SupportCount/num as decimal(38,4)),0) Confidence,  --置信度
       COALESCE(SupportCount,0) SupportCount,
       COALESCE(cast((SupportCount/num)/(SupportCountB/total)as decimal(38,4)),0) Lift  --提升度
  FROM (
        SELECT count(mallcoo_id) total, --去过商场的总人数
               sum(item_sc) num         --去过服装店的总人数
          FROM (
                SELECT mallcoo_id,
                       max( if(( IF(shop_commercialtypename IS NULL OR trim(cast(shop_commercialtypename AS STRING))='','未知',
                                    cast(shop_commercialtypename AS STRING)) = '服装'), 1, 0)) item_sc
                  FROM medusa.kudu_event_extend 
                 WHERE id BETWEEN '1-' AND '1-z' 
                   AND projectid = '1' 
                   AND (month BETWEEN to_date(trunc('2016-01-01','MM')) AND to_date(trunc('2016-05-31','MM')) 
                       AND day BETWEEN '2016-01-01' AND '2016-05-31') 
                   AND (event_type = 25001) 
                 GROUP BY mallcoo_id
                ) t 
        ) a 
CROSS JOIN (
            SELECT Itemset,
                   sum(num) SupportCountB,         --去过其他店的人数
                   sum(hadBeen_count) SupportCount --既去过服装店又去过其他店的人数
              FROM (
                    SELECT Itemset,
                           COUNT(DISTINCT mallcoo_id) num,  
                           if(hadBeen=1, COUNT(DISTINCT mallcoo_id), null) hadBeen_count  --既去过服装店又去过其他店的人
                      FROM (
                            SELECT mallcoo_id,
                                   max(if((IF(shop_commercialtypename IS NULL OR trim(cast(shop_commercialtypename AS STRING)) = '', 
                                              '未知',cast(shop_commercialtypename AS STRING)) = '服装'), 1, 0)) OVER (PARTITION BY mallcoo_id) hadBeen,
                                   IF(shop_commercialtypename IS NULL OR trim(cast(shop_commercialtypename AS STRING)) = '', 
                                              '未知',cast(shop_commercialtypename AS STRING)) Itemset                              
                              FROM medusa.kudu_event_extend 
                             WHERE id BETWEEN '1-' AND '1-z' 
                               AND projectid = '1'   --原46商场
                               AND (month BETWEEN to_date(trunc('2016-01-01','MM')) AND to_date(trunc('2016-05-31','MM')) 
                                   AND day BETWEEN '2016-01-01' AND '2016-05-31') 
                               AND (event_type = 25001)                     
                            ) x 
                      GROUP BY Itemset, hadBeen
                    ) b 
              GROUP BY Itemset 
            ) c;

            
--分析       
select mallcoo_id, shop_commercialtypename 
 FROM medusa.kudu_event_extend 
WHERE id BETWEEN '1-' AND '1-z' 
  AND projectid = '1'   --原46商场
  AND (month BETWEEN to_date(trunc('2016-01-01','MM')) AND to_date(trunc('2016-05-31','MM')) 
      AND day BETWEEN '2016-01-01' AND '2016-05-31') 
  AND (event_type = 25001) 
  and mallcoo_id in ('uid_7283717','uid_7225536')
;
+-------------+-------------------------+
| mallcoo_id  | shop_commercialtypename |
+-------------+-------------------------+
| uid_7225536 | NULL                    |
| uid_7225536 | 服装                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | 服装                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | 服装                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | 服装                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |
| uid_7225536 | NULL                    |

| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | 餐饮                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
| uid_7283717 | NULL                    |
+-------------+-------------------------+



select mallcoo_id,
       IF(shop_commercialtypename IS NULL OR trim(cast(shop_commercialtypename AS STRING)) = '', 
                  '未知',cast(shop_commercialtypename AS STRING)) Itemset 
 FROM medusa.kudu_event_extend 
WHERE id BETWEEN '1-' AND '1-z' 
  AND projectid = '1'   --原46商场
  AND (month BETWEEN to_date(trunc('2016-01-01','MM')) AND to_date(trunc('2016-05-31','MM')) 
      AND day BETWEEN '2016-01-01' AND '2016-05-31') 
  AND (event_type = 25001) 
  and mallcoo_id in ('uid_7283717','uid_7225536')
;
+-------------+---------+
| mallcoo_id  | itemset |
+-------------+---------+
| uid_7283717 | 餐饮    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7283717 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 服装    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 服装    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
| uid_7225536 | 服装    |
| uid_7225536 | 服装    |
| uid_7225536 | 未知    |
| uid_7225536 | 未知    |
+-------------+---------+


select mallcoo_id,
       if((IF(shop_commercialtypename IS NULL OR trim(cast(shop_commercialtypename AS STRING)) = '','未知',cast(shop_commercialtypename AS STRING)) = '服装'), 1, 0) hadbeen,
       IF(shop_commercialtypename IS NULL OR trim(cast(shop_commercialtypename AS STRING)) = '', 
                  '未知',cast(shop_commercialtypename AS STRING)) Itemset 
 FROM medusa.kudu_event_extend 
WHERE id BETWEEN '1-' AND '1-z' 
  AND projectid = '1'   --原46商场
  AND (month BETWEEN to_date(trunc('2016-01-01','MM')) AND to_date(trunc('2016-05-31','MM')) 
      AND day BETWEEN '2016-01-01' AND '2016-05-31') 
  AND (event_type = 25001) 
  and mallcoo_id in ('uid_7283717','uid_7225536')
;
+-------------+---------+---------+
| mallcoo_id  | hadbeen | itemset |
+-------------+---------+---------+
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 餐饮    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 1       | 服装    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 1       | 服装    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 1       | 服装    |
| uid_7225536 | 1       | 服装    |
| uid_7225536 | 0       | 未知    |
| uid_7225536 | 0       | 未知    |
+-------------+---------+---------+

SELECT mallcoo_id,
       max(if((IF(shop_commercialtypename IS NULL OR trim(cast(shop_commercialtypename AS STRING)) = '', 
                  '未知',cast(shop_commercialtypename AS STRING)) = '服装'), 1, 0)) OVER (PARTITION BY mallcoo_id) hadBeen,
       IF(shop_commercialtypename IS NULL OR trim(cast(shop_commercialtypename AS STRING)) = '', 
                  '未知',cast(shop_commercialtypename AS STRING)) Itemset   
  FROM medusa.kudu_event_extend 
 WHERE id BETWEEN '1-' AND '1-z' 
   AND projectid = '1'   --原46商场
   AND (month BETWEEN to_date(trunc('2016-01-01','MM')) AND to_date(trunc('2016-05-31','MM')) 
       AND day BETWEEN '2016-01-01' AND '2016-05-31') 
   AND (event_type = 25001) 
   and mallcoo_id in ('uid_7283717','uid_7225536')
;
+-------------+---------+---------+
| mallcoo_id  | hadbeen | itemset |
+-------------+---------+---------+
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 未知    |
| uid_7283717 | 0       | 餐饮    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 服装    |
| uid_7225536 | 1       | 服装    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 服装    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 未知    |
| uid_7225536 | 1       | 服装    |
| uid_7225536 | 1       | 未知    |
+-------------+---------+---------+

SELECT Itemset,
       COUNT(DISTINCT mallcoo_id) num,  
       if(hadBeen=1, COUNT(DISTINCT mallcoo_id), null) hadBeen_count  --既去过服装店又去过其他店的人
  FROM (
        SELECT mallcoo_id,
               max(if((IF(shop_commercialtypename IS NULL OR trim(cast(shop_commercialtypename AS STRING)) = '', 
                          '未知',cast(shop_commercialtypename AS STRING)) = '服装'), 1, 0)) OVER (PARTITION BY mallcoo_id) hadBeen,
               IF(shop_commercialtypename IS NULL OR trim(cast(shop_commercialtypename AS STRING)) = '', 
                          '未知',cast(shop_commercialtypename AS STRING)) Itemset                              
          FROM medusa.kudu_event_extend 
         WHERE id BETWEEN '1-' AND '1-z' 
           AND projectid = '1'   --原46商场
           AND (month BETWEEN to_date(trunc('2016-01-01','MM')) AND to_date(trunc('2016-05-31','MM')) 
               AND day BETWEEN '2016-01-01' AND '2016-05-31') 
           AND (event_type = 25001) 
           and mallcoo_id in ('uid_7283717','uid_7225536')         
        ) x 
  GROUP BY Itemset, hadBeen;
  
+---------+-----+---------------+
| itemset | num | hadbeen_count |
+---------+-----+---------------+
| 未知    | 1   | NULL          |
| 未知    | 1   | 1             |
| 餐饮    | 1   | NULL          |
| 服装    | 1   | 1             |
+---------+-----+---------------+

SELECT Itemset,
       sum(num) SupportCountB,         --去过其他某个店的人数
       sum(hadBeen_count) SupportCount --既去过服装店又去过其他某个店的人数
  FROM (
        SELECT Itemset,
               COUNT(DISTINCT mallcoo_id) num,  
               if(hadBeen=1, COUNT(DISTINCT mallcoo_id), null) hadBeen_count  --既去过服装店又去过其他店的人
          FROM (
                SELECT mallcoo_id,
                       max(if((IF(shop_commercialtypename IS NULL OR trim(cast(shop_commercialtypename AS STRING)) = '', 
                                  '未知',cast(shop_commercialtypename AS STRING)) = '服装'), 1, 0)) OVER (PARTITION BY mallcoo_id) hadBeen,
                       IF(shop_commercialtypename IS NULL OR trim(cast(shop_commercialtypename AS STRING)) = '', 
                                  '未知',cast(shop_commercialtypename AS STRING)) Itemset                              
                  FROM medusa.kudu_event_extend 
                 WHERE id BETWEEN '1-' AND '1-z' 
                   AND projectid = '1'   --原46商场
                   AND (month BETWEEN to_date(trunc('2016-01-01','MM')) AND to_date(trunc('2016-05-31','MM')) 
                       AND day BETWEEN '2016-01-01' AND '2016-05-31') 
                   AND (event_type = 25001)  
                   and mallcoo_id in ('uid_7283717','uid_7225536')                     
                ) x 
          GROUP BY Itemset, hadBeen
        ) b 
  GROUP BY Itemset

+---------+---------------+--------------+
| itemset | supportcountb | supportcount |
+---------+---------------+--------------+
| 餐饮    | 1             | NULL         |
| 未知    | 2             | 1            |
| 服装    | 1             | 1            |
+---------+---------------+--------------+




 
  
  
SELECT mallcoo_id,
       max( if(( IF(shop_commercialtypename IS NULL OR trim(cast(shop_commercialtypename AS STRING))='','未知',
                    cast(shop_commercialtypename AS STRING)) = '服装'), 1, 0) ) item_sc    --shop_commercialtypename='服装'， 赋值为1
  FROM medusa.kudu_event_extend 
 WHERE id BETWEEN '1-' AND '1-z' 
   AND projectid = '1' 
   AND (month BETWEEN to_date(trunc('2016-01-01','MM')) AND to_date(trunc('2016-05-31','MM')) 
       AND day BETWEEN '2016-01-01' AND '2016-05-31') 
   AND (event_type = 25001)
   and mallcoo_id in ('uid_7283717','uid_7225536')   
 GROUP BY mallcoo_id

+-------------+---------+
| mallcoo_id  | item_sc |
+-------------+---------+
| uid_7283717 | 0       |
| uid_7225536 | 1       |
+-------------+---------+





       
       
       

            

CREATE VIEW medusa.kudu_event_extend AS                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| SELECT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| -- +straight_join                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|  `_membercard_id` IS NOT NULL `_is_member`, 
    t4.storecount coupon_storecount, 
    t3.shopname shop_shopname, 
    t3.area shop_area, 
    t3.commercialtypename shop_commercialtypename, 
    t3.subcommercialtypename shop_subcommercialtypename, 
    t3.floorname shop_floorname, 
    t2.*, 
    t1.mallcoo_id, 
    `_mac`, 
    `_uuid`, 
    `_uid`, 
    `_openid`, 
    `_membercard_id`, 
    `_membercard`, 
    `_membercard_bonus`, 
    `_mobile`, 
    `_name`, 
    `_id_card`, 
    `_gender`, 
    `_birthday`, 
    `_province`, 
    `_city`, 
    `_area`, 
    `_address`, 
    `_hascar`, 
    `_mobile_brand`, 
    `_mobile_os`, 
    `_residence`, 
    `_workplace`, 
    `_loc_first_time`, 
    `_portal_first_time`, 
    `_app_first_time`, 
    `_wechat_follow_time`, 
    `_register_time`, 
    `_register_source`, 
    `_constellation`, 
    idlist, 
    id, 
    `_age`, 
    tag01, 
    `_source_mallname`, 
    `_tag`, 
    `_tag_temp` 
  FROM medusa.kudu_user t1 INNER JOIN 
| -- +BROADCAST                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|  medusa.event_view_any t2 ON t1.projectid = t2.projectid AND t1.distinct_id = t2.distinct_id LEFT OUTER JOIN                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| -- +BROADCAST                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|  medusa.shop t3 ON t2.projectid = t3.projectid AND t2.shopname = t3.shopname LEFT OUTER JOIN         --小表                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| -- +BROADCAST                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|  medusa.extend_putincouponmanage t4 ON t2.projectid = t4.projectid AND t2.coupon_id = t4.cvrid       --小表


            
medusa.kudu_user --用户表 需要指定 row_key 
medusa.event_view_any --事件表视图             





CREATE VIEW medusa.event_view_any AS                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
SELECT *, 
       CASE WHEN dayofweek(day) IN (1, 7) THEN '周末' ELSE '工作日' END week_day 
  FROM medusa.event2 

 UNION ALL 

SELECT mallid, 
       mallname, 
       time, 
       ts, 
       day, 
       week, 
       hour, 
       event_scene, 
       event_source, 
       distinct_id, 
       event_name, 
       floor_id, 
       groupon_id, 
       groupon_name, 
       page_module, 
       order_id, 
       order_total_price, 
       order_product_amount, 
       payment_method, 
       payment_amount, 
       verification_amount, 
       verification_product_amount, 
       refund_amount, 
       refund_reason, 
       refund_status, 
       ip, 
       mac, 
       cookie_id, 
       user_id, 
       mobile_os, 
       app_version, 
       duration, 
       is_member, 
       lottery_type, 
       lottery_id, 
       lottery_name, 
       lottery_award_type, 
       lottery_award_id, 
       lottery_award_name, 
       mobile, 
       membercard_no, 
       auth_type, 
       openid, 
       mobile_brand, 
       mobile_version, 
       flow, 
       bonus, 
       action_type, 
       shopname, 
       product_name, 
       other, 
       remark, 
       card_source, 
       card_type, 
       park_name, 
       park_entry_time, 
       park_leave_time, 
       park_plate_no, 
       coupon_duration, 
       coupon_amount, 
       bonus_amount, 
       mall_duration, 
       mall_amount, 
       member_duration, 
       member_amount, 
       total_free_amount, 
       park_amount, 
       brand, 
       commercialtypename, --
       subcommercialtypename, 
       is_new, 
       floor_name, 
       ad_id, 
       ad_name, 
       is_register, 
       page_name, 
       page_refid, 
       movie_id, 
       movie_name, 
       activity_id, 
       activity_name, 
       gift_id, 
       gift_name, 
       pqr_type, 
       pqr_scene, 
       pqr_location, 
       follow_type, 
       cardgrade_type, 
       bonusadd_type, 
       bonususe_type, 
       bonususe_payscene, 
       bonusadd_scene, 
       coupon_id, 
       coupon_name, 
       coupon_type, 
       coupon_put_channel, 
       coupon_verificate_channel, 
       page_refname, 
       card_type_before, 
       activity_area, 
       projectid, 
       month, 
       10000 event_type, --任意事件 
       CASE WHEN dayofweek(day) IN (1, 7) THEN '周末' ELSE '工作日' END week_day 
  FROM medusa.event2 
 WHERE event_type != 10001 

 
 
 ---------------------------------------------------+
CREATE EXTERNAL TABLE medusa.kudu_user (                                       
  id STRING,                                                                   
  projectid STRING,                                                            
  distinct_id STRING,                                                          
  mallcoo_id STRING,                                                           
  _mac STRING,                                                                 
  _uuid STRING,                                                                
  _uid STRING,                                                                 
  _openid STRING,                                                              
  _membercard_id STRING,                                                       
  _membercard STRING,                                                          
  _membercard_bonus DOUBLE,                                                    
  _mobile STRING,                                                              
  _name STRING,                                                                
  _id_card STRING,                                                             
  _gender STRING,                                                              
  _birthday STRING,                                                            
  _constellation STRING,                                                       
  _province STRING,                                                            
  _city STRING,                                                                
  _area STRING,                                                                
  _address STRING,                                                             
  _hascar BOOLEAN,                                                             
  _mobile_brand STRING,                                                        
  _mobile_os STRING,                                                           
  _residence STRING,                                                           
  _workplace STRING,                                                           
  _loc_first_time STRING,                                                      
  _portal_first_time STRING,                                                   
  _app_first_time STRING,                                                      
  _wechat_follow_time STRING,                                                  
  _register_time STRING,                                                       
  _register_source STRING,                                                     
  _age INT,                                                                    
  idlist STRING,                                                               
  tag01 STRING,                                                                
  tag02 STRING,                                                                
  tag03 STRING,                                                                
  tag04 STRING,                                                                
  tag05 STRING,                                                                
  tag06 STRING,                                                                
  tag07 STRING,                                                                
  tag08 STRING,                                                                
  tag09 STRING,                                                                
  tag10 STRING,                                                                
  _tag STRING,                                                                 
  _tag_temp STRING,                                                            
  _source_mallname STRING,                                                     
  tag11 STRING,                                                                
  tag12 STRING,                                                                
  tag13 STRING,                                                                
  tag14 STRING,                                                                
  tag15 STRING,                                                                
  tag16 STRING,                                                                
  tag17 STRING,                                                                
  tag18 STRING,                                                                
  tag19 STRING,                                                                
  tag20 STRING,                                                                
  tag21 STRING,                                                                
  tag22 STRING,                                                                
  tag23 STRING,                                                                
  tag24 STRING,                                                                
  tag25 STRING,                                                                
  tag26 STRING,                                                                
  tag27 STRING,                                                                
  tag28 STRING,                                                                
  tag29 STRING,                                                                
  tag30 STRING,                                                                
  _plate_no STRING                                                             
)                                                                              
TBLPROPERTIES ('numFiles'='0', 
                 'kudu.master_addresses'='thadoop003:7051', 
                 'kudu.key_columns'='id', 
                 'kudu.table_name'='medusa_user', 
                 'transient_lastDdlTime'='1491446784', 
                 'COLUMN_STATS_ACCURATE'='false', 
                 'totalSize'='0', 
                 'numRows'='-1', 
                 'rawDataSize'='-1', 
                 'storage_handler'='com.cloudera.kudu.hive.KuduStorageHandler') 




+-------------------------------------------------------------------+
| result                                                            |
+-------------------------------------------------------------------+
| CREATE TABLE medusa.shop (                                        |
|   shopname STRING,                                                |
|   area DOUBLE,                                                    |
|   floorname STRING,                                               |
|   brand STRING,                                                   |
|   commercialtypename STRING,                                      |
|   subcommercialtypename STRING                                    |
| )                                                                 |
| PARTITIONED BY (                                                  |
|   projectid STRING                                                |
| )                                                                 |
| STORED AS PARQUET                                                 |
| LOCATION 'hdfs://thahadoop001:9000/user/root/hive/medusa.db/shop' |
| TBLPROPERTIES ('transient_lastDdlTime'='1505705526')              |
+-------------------------------------------------------------------+



+---------------------------------------------------------------------------------------+
| result                                                                                |
+---------------------------------------------------------------------------------------+
| CREATE TABLE medusa.extend_putincouponmanage (                                        |
|   mallid STRING,                                                                      |
|   cvrid STRING,                                                                       |
|   storecount INT                                                                      |
| )                                                                                     |
| PARTITIONED BY (                                                                      |
|   projectid STRING                                                                    |
| )                                                                                     |
| STORED AS PARQUET                                                                     |
| LOCATION 'hdfs://thahadoop001:9000/user/root/hive/medusa.db/extend_putincouponmanage' |
| TBLPROPERTIES ('transient_lastDdlTime'='1504260338')                                  |
+---------------------------------------------------------------------------------------+



==============================================================================================


--关联分析
-- CRM会员消费 的 商户二级业态（扩展）== 化妆品/个人护理

SELECT Itemset,
       COALESCE(cast(SupportCount/total as decimal(38,4)),0)Support,
       COALESCE(cast(SupportCount/SupportCountB as decimal(38,4)),0)ReverseConfidence,
       COALESCE(cast(SupportCount/num as decimal(38,4)),0)Confidence,
       COALESCE(SupportCount,0)SupportCount,
       COALESCE(cast((SupportCount/num)/(SupportCountB/total)as decimal(38,4)),0)Lift 
  FROM (
        SELECT count(mallcoo_id)total,
               sum(item_sc)num 
          FROM (
                SELECT mallcoo_id,
                       max(if((IF(shop_subcommercialtypename IS NULL OR trim(cast(shop_subcommercialtypename AS STRING))='', 
                                  '未知',cast(shop_subcommercialtypename AS STRING))='化妆品/个人护理'),1,0))item_sc 
                  FROM medusa.kudu_event_extend 
                 WHERE id BETWEEN '1-' AND '1-z' 
                   AND projectid='1' 
                   AND (month BETWEEN to_date(trunc('2016-01-01','MM')) AND to_date(trunc('2016-05-31','MM')) 
                        AND day BETWEEN '2016-01-01' AND '2016-05-31') 
                   AND (event_type=25001) 
                 GROUP BY mallcoo_id
                )t
        )a 
CROSS JOIN (
            SELECT Itemset,
                   sum(num) SupportCountB,
                   sum(hadBeen_count) SupportCount 
              FROM (
                    SELECT Itemset,
                           COUNT(DISTINCT mallcoo_id) num,
                           if(hadBeen=1,COUNT(DISTINCT mallcoo_id),null)hadBeen_count 
                      FROM (
                            SELECT mallcoo_id,
                                   max(if((IF(shop_subcommercialtypename IS NULL OR trim(cast(shop_subcommercialtypename AS STRING))='', 
                                              '未知',cast(shop_subcommercialtypename AS STRING))='化妆品/个人护理'),1,0))OVER(PARTITION BY mallcoo_id)hadBeen,
                                   IF(shop_subcommercialtypename IS NULL OR trim(cast(shop_subcommercialtypename AS STRING))='', 
                                      '未知',cast(shop_subcommercialtypename AS STRING))Itemset 
                              FROM medusa.kudu_event_extend 
                             WHERE id BETWEEN '1-' AND '1-z' 
                               AND projectid='1' 
                               AND (month BETWEEN to_date(trunc('2016-01-01','MM')) AND to_date(trunc('2016-05-31','MM')) 
                                    AND day BETWEEN '2016-01-01' AND '2016-05-31')
                               AND (event_type=25001)
                            )x 
                      GROUP BY Itemset,hadBeen
                    )b 
              GROUP BY Itemset
            )c;
            
            
--分析

select mallcoo_id, shop_subcommercialtypename
 FROM medusa.kudu_event_extend 
WHERE id BETWEEN '1-' AND '1-z' 
  AND projectid='1' 
  AND (month BETWEEN to_date(trunc('2016-01-01','MM')) AND to_date(trunc('2016-05-31','MM')) 
       AND day BETWEEN '2016-01-01' AND '2016-05-31')
  AND (event_type=25001)
   and mallcoo_id in ('uid_7200525','uid_7200371','uid_7200098','uid_7201678')
  
+-------------+----------------------------+
| mallcoo_id  | shop_subcommercialtypename |
+-------------+----------------------------+
| uid_7201678 | NULL                       |
| uid_7201678 | 化妆品/个人护理            |
| uid_7201678 | NULL                       |
| uid_7201678 | 化妆品/个人护理            |
| uid_7200525 | 甜品/水吧                  |
| uid_7200525 | NULL                       |
| uid_7200525 | NULL                       |
| uid_7200525 | 甜品/水吧                  |
| uid_7200371 | NULL                       |
| uid_7200371 | NULL                       |
| uid_7200371 | NULL                       |
| uid_7200371 | NULL                       |
| uid_7200371 | 甜品/水吧                  |
| uid_7200371 | NULL                       |
| uid_7200371 | NULL                       |
| uid_7200371 | NULL                       |
| uid_7200098 | 珠宝/黄金                  |
| uid_7200098 | NULL                       |
| uid_7200098 | NULL                       |
| uid_7200098 | NULL                       |
| uid_7200098 | NULL                       |
| uid_7200098 | NULL                       |
| uid_7200098 | NULL                       |
| uid_7200098 | NULL                       |
| uid_7200098 | NULL                       |
+-------------+----------------------------+


SELECT mallcoo_id,
       max(if((IF(shop_subcommercialtypename IS NULL OR trim(cast(shop_subcommercialtypename AS STRING))='', 
                  '未知',cast(shop_subcommercialtypename AS STRING))='化妆品/个人护理'),1,0))OVER(PARTITION BY mallcoo_id)hadBeen,
       IF(shop_subcommercialtypename IS NULL OR trim(cast(shop_subcommercialtypename AS STRING))='', 
          '未知',cast(shop_subcommercialtypename AS STRING))Itemset 
  FROM medusa.kudu_event_extend 
 WHERE id BETWEEN '1-' AND '1-z' 
   AND projectid='1' 
   AND (month BETWEEN to_date(trunc('2016-01-01','MM')) AND to_date(trunc('2016-05-31','MM')) 
        AND day BETWEEN '2016-01-01' AND '2016-05-31')
   AND (event_type=25001)
   and mallcoo_id in ('uid_7200525','uid_7200371','uid_7200098','uid_7201678')
+-------------+---------+-----------------+
| mallcoo_id  | hadbeen | itemset         |
+-------------+---------+-----------------+
| uid_7200371 | 0       | 未知            |
| uid_7200371 | 0       | 未知            |
| uid_7200371 | 0       | 未知            |
| uid_7200371 | 0       | 未知            |
| uid_7200371 | 0       | 甜品/水吧       |
| uid_7200371 | 0       | 未知            |
| uid_7200371 | 0       | 未知            |
| uid_7200371 | 0       | 未知            |
| uid_7200525 | 0       | 甜品/水吧       |
| uid_7200525 | 0       | 未知            |
| uid_7200525 | 0       | 未知            |
| uid_7200525 | 0       | 甜品/水吧       |
| uid_7200098 | 0       | 未知            |
| uid_7200098 | 0       | 未知            |
| uid_7200098 | 0       | 未知            |
| uid_7200098 | 0       | 未知            |
| uid_7200098 | 0       | 未知            |
| uid_7200098 | 0       | 未知            |
| uid_7200098 | 0       | 珠宝/黄金       |
| uid_7200098 | 0       | 未知            |
| uid_7200098 | 0       | 未知            |
| uid_7201678 | 1       | 未知            |
| uid_7201678 | 1       | 化妆品/个人护理 |
| uid_7201678 | 1       | 未知            |
| uid_7201678 | 1       | 化妆品/个人护理 |
+-------------+---------+-----------------+

SELECT Itemset,
       COUNT(DISTINCT mallcoo_id) num,
       if(hadBeen=1,COUNT(DISTINCT mallcoo_id),null)hadBeen_count 
  FROM (
        SELECT mallcoo_id,
               max(if((IF(shop_subcommercialtypename IS NULL OR trim(cast(shop_subcommercialtypename AS STRING))='', 
                          '未知',cast(shop_subcommercialtypename AS STRING))='化妆品/个人护理'),1,0))OVER(PARTITION BY mallcoo_id)hadBeen,
               IF(shop_subcommercialtypename IS NULL OR trim(cast(shop_subcommercialtypename AS STRING))='', 
                  '未知',cast(shop_subcommercialtypename AS STRING))Itemset 
          FROM medusa.kudu_event_extend 
         WHERE id BETWEEN '1-' AND '1-z' 
           AND projectid='1' 
           AND (month BETWEEN to_date(trunc('2016-01-01','MM')) AND to_date(trunc('2016-05-31','MM')) 
                AND day BETWEEN '2016-01-01' AND '2016-05-31')
           AND (event_type=25001)  
           and mallcoo_id in ('uid_7200525','uid_7200371','uid_7200098','uid_7201678')
        )x 
  GROUP BY Itemset,hadBeen   
+-----------------+-----+---------------+
| itemset         | num | hadbeen_count |
+-----------------+-----+---------------+
| 珠宝/黄金       | 1   | NULL          |
| 未知            | 1   | 1             |
| 未知            | 3   | NULL          |
| 甜品/水吧       | 2   | NULL          |
| 化妆品/个人护理 | 1   | 1             |
+-----------------+-----+---------------+
  
