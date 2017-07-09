+----------+------------+--------+
| user_id  | user_type  | sales  |
+----------+------------+--------+
| zhangsa  | new        | 2      |
| lisi     | old        | 1      |
| wanger   | new        | 3      |
| liiu     | new        | 1      |
| qibaqiu  | new        | 1      |
| wangshi  | old        | 2      |
| liwei    | old        | 3      |
| wutong   | new        | 6      |
| lilisi   | new        | 5      |
| qishili  | new        | 5      |
+----------+------------+--------+

create external table window_function_test2 (
user_id string,
user_type string,
sales bigint
) 
row format delimited 
fields terminated by ','
stored as textfile 
location'/user/thadoop/ods/migu/ott/test_dy/t2/';

--10.200.65.71


## COUNT、SUM、MIN、MAX、AVG
select 
    user_id,
    user_type,
    sales,
    --默认为从起点到当前行（同一个组，排序一样的进行累加，还要加上 自己排序前面的）
    sum(sales) OVER(PARTITION BY user_type ORDER BY sales asc) AS sales_1,
    --从起点到当前行，结果与sales_1不同。
    sum(sales) OVER(PARTITION BY user_type ORDER BY sales asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sales_2,
    --当前行+往前3行
    sum(sales) OVER(PARTITION BY user_type ORDER BY sales asc ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS sales_3,
    --当前行+往前3行+往后1行
    sum(sales) OVER(PARTITION BY user_type ORDER BY sales asc ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING) AS sales_4,
    --当前行+往后所有行  
    sum(sales) OVER(PARTITION BY user_type ORDER BY sales asc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS sales_5,
    --分组内所有行 
    SUM(sales) OVER(PARTITION BY user_type) AS sales_6                          
from 
    window_function_test2
order by 
    user_type,
    sales,
    user_id

+----------+------------+--------+----------+----------+----------+----------+----------+----------+--+
| user_id  | user_type  | sales  | sales_1  | sales_2  | sales_3  | sales_4  | sales_5  | sales_6  |
+----------+------------+--------+----------+----------+----------+----------+----------+----------+--+
| liiu     | new        | 1      | 2        | 2        | 2        | 4        | 22       | 23       |
| qibaqiu  | new        | 1      | 2        | 1        | 1        | 2        | 23       | 23       |
| zhangsa  | new        | 2      | 4        | 4        | 4        | 7        | 21       | 23       |
| wanger   | new        | 3      | 7        | 7        | 7        | 12       | 19       | 23       |
| lilisi   | new        | 5      | 17       | 17       | 15       | 21       | 11       | 23       |
| qishili  | new        | 5      | 17       | 12       | 11       | 16       | 16       | 23       |
| wutong   | new        | 6      | 23       | 23       | 19       | 19       | 6        | 23       |
| lisi     | old        | 1      | 1        | 1        | 1        | 3        | 6        | 6        |
| wangshi  | old        | 2      | 3        | 3        | 3        | 6        | 5        | 6        |
| liwei    | old        | 3      | 6        | 6        | 6        | 6        | 3        | 6        |
+----------+------------+--------+----------+----------+----------+----------+----------+----------+--+


注意:
结果和ORDER BY相关,默认为升序
如果不指定ROWS BETWEEN,默认为从起点到当前行;
如果不指定ORDER BY，则将分组内所有值累加;

关键是理解ROWS BETWEEN含义,也叫做WINDOW子句：
PRECEDING：往前
FOLLOWING：往后
CURRENT ROW：当前行
UNBOUNDED：无界限（起点或终点）
UNBOUNDED PRECEDING：表示从前面的起点 
UNBOUNDED FOLLOWING：表示到后面的终点
其他COUNT、AVG，MIN，MAX，和SUM用法一样。



## first_value与last_value
select 
    user_id,
    user_type,
    ROW_NUMBER() OVER(PARTITION BY user_type ORDER BY sales) AS row_num,  
    first_value(user_id) over (partition by user_type order by sales desc) as max_sales_user,
    first_value(user_id) over (partition by user_type order by sales asc) as min_sales_user,
    last_value(user_id) over (partition by user_type order by sales desc) as curr_last_min_user,
    last_value(user_id) over (partition by user_type order by sales asc) as curr_last_max_user
from 
    order_detail;



-- 验证下分组	
select a.user_id,
	   a.user_type,
	   a.sales,
	   round(a.sales/sum(a.sales) over(partition by a.user_type), 2)         
  from (	
		select a.user_id,
			   a.user_type,
			   sum(a.sales) sales	
		  from window_function_test2 a 
		 group by a.user_id,
				  a.user_type
       ) t ;