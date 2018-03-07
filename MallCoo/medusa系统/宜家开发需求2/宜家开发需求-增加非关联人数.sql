
SELECT
    Itemset,
    COALESCE(cast(SupportCount/total as decimal(38,4)),0) Support,
    COALESCE(cast(SupportCount/SupportCountB as decimal(38,4)),0) ReverseConfidence,
    COALESCE(cast(SupportCount/num as decimal(38,4)),0) Confidence,
    COALESCE(SupportCount,0) SupportCount,
    COALESCE(num-SupportCount,0) non_SupportCount,
    COALESCE(cast((SupportCount/num)/(SupportCountB/total) as decimal(38,4)),0) Lift
FROM (
    SELECT count(mallcoo_id) total,
           sum(item_sc) num
    FROM (
        SELECT mallcoo_id,
               max(if({{ #item.rule#.where(`IF(#item.field# IS NULL OR trim(cast(#item.field# AS STRING))='', '未知',cast(#item.field# AS STRING))` }},1,0)) item_sc
        FROM {{ @table }}
        WHERE {{ @project }} AND {{ @time_range }} AND {{ @filter }}
        GROUP BY mallcoo_id
    ) t
) a CROSS JOIN (
    SELECT Itemset,
           sum(num) SupportCountB,
           sum(hadBeen_count) SupportCount
    FROM (
        SELECT Itemset,
               COUNT(DISTINCT mallcoo_id) num,
               if(hadBeen=1,COUNT(DISTINCT mallcoo_id),null) hadBeen_count
        FROM (
            SELECT
                mallcoo_id,
                max(if({{ #item.rule#.where(`IF(#item.field# IS NULL OR trim(cast(#item.field# AS STRING))='', '未知',cast(#item.field# AS STRING))`) }},1,0)) OVER(PARTITION BY mallcoo_id) hadBeen,
                {{ `IF(#item.field# IS NULL OR trim(cast(#item.field# AS STRING))='', '未知',cast(#item.field# AS STRING))` }} Itemset
            FROM {{ @table }}
            WHERE {{ @project }} AND {{ @time_range }} AND {{ @filter }}
        ) x
        GROUP BY Itemset,hadBeen
    ) b
    GROUP BY Itemset
) c
{{ #item.filter.event#.where('Itemset').prefix('WHERE') }};



parameter:
{
"parameter":{"filter":{"all":{"query":{"must":[{"code":"event_type","operator":"equals","values":[25001]}]}}},
             "item":{"field":"shop_commercialtypename",
                     "rule":{"query":{"must":[{"code":"shop_commercialtypename","operator":"equals","values":["服装"]}]}}
                    },
             "projectid":1,
             "category":"user",
             "time_range":[["2016-01-01","2016-05-31"]],
             "is_extend":true,
             "is_any":false 
            },
"model":"association"
}


SELECT Itemset,
       COALESCE(cast(SupportCount/total as decimal(38,4)),0) Support, --支持度 
       COALESCE(cast(SupportCount/SupportCountB as decimal(38,4)),0) ReverseConfidence,  --反向置信度
       COALESCE(cast(SupportCount/num as decimal(38,4)),0) Confidence,  --置信度
       COALESCE(SupportCount,0) SupportCount,
       --COALESCE(num-SupportCount,0) non_SupportCount,
       COALESCE(cast((SupportCount/num)/(SupportCountB/total)as decimal(38,4)),0) Lift  --提升度
  FROM (
        SELECT count(mallcoo_id) total, --去过商场的总人数
               sum(item_sc) num         --去过服装店的总人数
          FROM (
                SELECT mallcoo_id,
                       max( if(( IF(shop_commercialtypename IS NULL OR trim(cast(shop_commercialtypename AS STRING))='','未知',
                                    cast(shop_commercialtypename AS STRING)) = '服装'), 1, 0) ) item_sc    
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
                   sum(num) SupportCountB,         --去过其他店(包括服装店)的人数
                   sum(hadBeen_count) SupportCount --既去过服装店又去过其他店(包括服装店)的人数
              FROM (
                    SELECT Itemset,
                           COUNT(DISTINCT mallcoo_id) num,  
                           if(hadBeen=1, COUNT(DISTINCT mallcoo_id), null) hadBeen_count  --既去过服装店又去过其他店(包括服装店)的人
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