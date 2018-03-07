

--漏斗分析

--报名活动 --> crm会员积分

SELECT step,
       count(DISTINCT mallcoo_id)step_count,
       dimension 
  FROM (
        SELECT mallcoo_id,
               cast(udf.funnel(action,ts,"1,2",604800) AS INT) step,  --窗口期7天 化成秒   判断在哪一个step
               COALESCE(max(IF(event_type=15003,IF(_dim IS NULL OR trim(cast(_dim AS STRING))='',NULL,cast(_dim AS STRING)),NULL)),'未知') dimension 
          FROM (
                SELECT mallcoo_id,
                       ts,
                       event_type,
                       CASE WHEN (event_type=15003) THEN 1 
                            WHEN (event_type=25007) THEN 2 END action,
                       IF(projectID IS NULL OR trim(cast(projectID AS STRING))='', '未知',cast(projectID AS STRING)) _dim 
					   
                  FROM medusa.kudu_event 
                 WHERE id BETWEEN '1-' AND '1-z' 
                   AND projectid='1' 
                   AND (month BETWEEN to_date(trunc('2016-01-01','MM')) AND to_date(trunc('2016-05-31','MM')) 
                        AND day BETWEEN '2016-01-01' AND '2016-05-31') 
                   AND ((event_type=15003) OR (event_type=25007))
                )f 
          WHERE action>0 
          GROUP BY mallcoo_id
        )s 
  WHERE step>0 
  GROUP BY step,dimension;
 
+------+------------+-----------+
| step | step_count | dimension |
+------+------------+-----------+
| 1    | 8257       | 1         |
| 2    | 925        | 1         |
+------+------------+-----------+
