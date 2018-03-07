2017-10-26 17:34:13.485 INFO   [cn.mallcoo.medusa.data.center.web.ParserActor#akka://data-center/user/listener/parserActor]: 
parameter:
{"parameter":{"filter":{"all":{"query":{"must":[{"code":"event_type","operator":"equals","values":[25001]}]}}},"item":{"field":"shop_commercialtypename","rule":{"query":{"must":[{"code":"shop_commercialtypename","operator":"equals","values":["服装"]}]}},"filter":{"event":{"query":{"must":[{"code":"Itemset","operator":"equals","values":["餐饮"]}]}}}},"projectid":1,"category":"user","time_range":[["2016-01-01","2016-05-31"]],"is_extend":true,"is_any":false,"uniqueid":"fbd3362de8544e858606692d14807bad","model":"association"},"model":"user_list"}
mql:
UPDATE IGNORE x SET x._tag_temp=udf.append(x._tag_temp,'fbd3362de8544e858606692d14807bad')
FROM medusa.kudu_user x,
(
SELECT mallcoo_id 
  FROM medusa.kudu_event_extend 
 WHERE id BETWEEN '1-' AND '1-z' 
   AND projectid='1' 
   AND (month BETWEEN to_date(trunc('2016-01-01','MM')) AND to_date(trunc('2016-05-31','MM')) AND day BETWEEN '2016-01-01' AND '2016-05-31')
   AND (event_type=25001) 
 GROUP BY mallcoo_id 
HAVING max(if((IF(shop_commercialtypename IS NULL OR trim(cast(shop_commercialtypename AS STRING))='', NULL,cast(shop_commercialtypename AS STRING))='服装'),1,0))=1 
   AND max(if((IF(shop_commercialtypename IS NULL OR trim(cast(shop_commercialtypename AS STRING))='', '未知',cast(shop_commercialtypename AS STRING))='餐饮'),1,0))=1
)y 
WHERE x.mallcoo_id=y.mallcoo_id;


SELECT mallcoo_id 
  FROM medusa.kudu_event_extend 
 WHERE id BETWEEN '1-' AND '1-z' 
   AND projectid='1' 
   AND (month BETWEEN to_date(trunc('2016-01-01','MM')) AND to_date(trunc('2016-05-31','MM')) AND day BETWEEN '2016-01-01' AND '2016-05-31')
   AND (event_type=25001) 
 GROUP BY mallcoo_id 
HAVING max(if((IF(shop_commercialtypename IS NULL OR trim(cast(shop_commercialtypename AS STRING))='', NULL,cast(shop_commercialtypename AS STRING))='服装'),1,0))=1 
   AND max(if((IF(shop_commercialtypename IS NULL OR trim(cast(shop_commercialtypename AS STRING))='', '未知',cast(shop_commercialtypename AS STRING))='餐饮'),1,0))=0
   
   
