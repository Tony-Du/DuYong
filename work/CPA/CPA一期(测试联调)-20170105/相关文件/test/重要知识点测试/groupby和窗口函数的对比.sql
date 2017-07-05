--数据准备
use_source_ip   business_id user_cnt
'ip01','b01',10
'ip02','b01',20
'ip03','b01',30
'ip01','b02',22
'ip01','b03',25

--建表
create external table window_function_test (
use_source_ip string,
business_id string,
user_cnt bigint
) 
row format delimited 
fields terminated by ','
stored as textfile 
location'/user/thadoop/ods/migu/ott/test_dy/';


SELECT v.business_id,
       v.use_source_ip, 
       sum(v.user_cnt) as user_cnt, 
       row_number() OVER(PARTITION BY v.business_id Order by sum(v.user_cnt) DESC) nn
from window_function_test v
group by v.business_id,
         v.use_source_ip;

--order by 后面的 sum() 到底直接拿的上面的sum()? 还是重新按business_id分组计算的sum()?		
-- 答： 按运行结果来看是直接拿上面的sum() 
		 
+----------------+------------------+-----------+-----+--+
| v.business_id  | v.use_source_ip  | user_cnt  | nn  |
+----------------+------------------+-----------+-----+--+
| 'b01'          | 'ip03'           | 30        | 1   |
| 'b01'          | 'ip02'           | 20        | 2   |
| 'b01'          | 'ip01'           | 10        | 3   |
| 'b02'          | 'ip01'           | 22        | 1   |
| 'b03'          | 'ip01'           | 25        | 1   |
+----------------+------------------+-----------+-----+--+



SELECT v.business_id,
       v.use_source_ip, 
       sum(v.user_cnt) as user_cnt, 
       rank() OVER(PARTITION BY v.business_id Order by sum(v.user_cnt) DESC) nn
from window_function_test v
group by v.business_id,
         v.use_source_ip;
		 
+----------------+------------------+-----------+-----+--+
| v.business_id  | v.use_source_ip  | user_cnt  | nn  |
+----------------+------------------+-----------+-----+--+
| 'b01'          | 'ip03'           | 30        | 1   |
| 'b01'          | 'ip02'           | 20        | 2   |
| 'b01'          | 'ip01'           | 10        | 3   |
| 'b02'          | 'ip01'           | 22        | 1   |
| 'b03'          | 'ip01'           | 25        | 1   |
+----------------+------------------+-----------+-----+--+		 
		 
		 
SELECT v.business_id,
       v.use_source_ip, 
       sum(v.user_cnt) as user_cnt, 
       dense_rank() OVER(PARTITION BY v.business_id Order by sum(v.user_cnt) DESC) nn
from window_function_test v
group by v.business_id,
         v.use_source_ip;