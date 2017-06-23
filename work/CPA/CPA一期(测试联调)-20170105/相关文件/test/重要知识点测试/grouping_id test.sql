-- 数据准备
month	day			cookieid
2015-03,2015-03-10,cookie1
2015-03,2015-03-10,cookie5
2015-03,2015-03-12,cookie7
2015-04,2015-04-12,cookie3
2015-04,2015-04-13,cookie2
2015-04,2015-04-13,cookie4
2015-04,2015-04-16,cookie4
2015-03,2015-03-10,cookie2
2015-03,2015-03-10,cookie3
2015-04,2015-04-12,cookie5
2015-04,2015-04-13,cookie6
2015-04,2015-04-15,cookie3
2015-04,2015-04-15,cookie2
2015-04,2015-04-16,cookie1

CREATE EXTERNAL TABLE dy1234 (
 month    STRING,
 day      STRING, 
 cookieid STRING 
) ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
stored as textfile location '/tmp/tony11/';
-- =========================================================================================== --

select month, count(day), count(cookieid) as uv from dy1234 group by month;
2015-03 5       5
2015-04 9       9
select month, count(distinct day), count(cookieid) as uv from dy1234 group by month;
2015-03 2       5
2015-04 4       9
select month, count(day), count(distinct cookieid) as uv from dy1234 group by month;
2015-03 5       5
2015-04 9       6
select month, count( distinct day), count(distinct cookieid) as uv from dy1234 group by month;
2015-03 2       5
2015-04 4       6

select month, count( distinct day,cookieid) as aa from dy1234 group by month;
OK
2015-03 5
2015-04 9
-- 只有当day,cookieid都相同时才视为相同记录


select month,day,COUNT(DISTINCT cookieid) AS uv
FROM dy1234 
GROUP BY month,day ;
2015-03 2015-03-10      4
2015-03 2015-03-12      1
2015-04 2015-04-12      2
2015-04 2015-04-13      3
2015-04 2015-04-15      2
2015-04 2015-04-16      2
-- 最终的分组其实是按day分组的

select month,day,COUNT(DISTINCT cookieid) AS uv
FROM dy1234 
GROUP BY day, month ;
2015-03 2015-03-10      4
2015-03 2015-03-12      1
2015-04 2015-04-12      2
2015-04 2015-04-13      3
2015-04 2015-04-15      2
2015-04 2015-04-16      2
-- group by 多字段时，与字段的顺序没有关系


select month,day,COUNT(DISTINCT cookieid) AS uv,GROUPING__ID 
FROM dy1234 
GROUP BY month,day 
GROUPING SETS (month,day);

NULL    2015-03-10      4       2
NULL    2015-03-12      1       2
NULL    2015-04-12      2       2
NULL    2015-04-13      3       2
NULL    2015-04-15      2       2
NULL    2015-04-16      2       2
2015-03 NULL    		5       1
2015-04 NULL    		6       1
-- 注：grouping__id中 month对应1，day对应2

select month,day,COUNT(DISTINCT cookieid) AS uv,GROUPING__ID 
FROM dy1234 
GROUP BY month,day 
GROUPING SETS (month,day) 
ORDER BY GROUPING__ID;

2015-04 NULL    		6       1
2015-03 NULL   			5       1
NULL    2015-04-16      2       2
NULL    2015-04-15      2       2
NULL    2015-04-13      3       2
NULL    2015-04-12      2       2
NULL    2015-03-12      1       2

select month,day,COUNT(DISTINCT cookieid) AS uv,GROUPING__ID 
FROM dy1234 
GROUP BY month,day 
GROUPING SETS ((),month,day,(month,day)) 
ORDER BY GROUPING__ID;

NULL    NULL    		7       0
2015-03 NULL    		5       1
2015-04 NULL    		6       1
NULL    2015-04-16      2       2
NULL    2015-04-15      2       2
NULL    2015-04-13      3       2
NULL    2015-04-12      2       2
NULL    2015-03-12      1       2
NULL    2015-03-10      4       2
2015-04 2015-04-12      2       3
2015-04 2015-04-16      2       3
2015-03 2015-03-12      1       3
2015-03 2015-03-10      4       3
2015-04 2015-04-15      2       3
2015-04 2015-04-13      3       3

select month,day,COUNT(DISTINCT cookieid) AS uv,GROUPING__ID 
FROM dy1234 
GROUP BY month,day 
GROUPING SETS ((),month,day,(month,day));

NULL    NULL    		7       0
NULL    2015-03-10      4       2
NULL    2015-03-12      1       2
NULL    2015-04-12      2       2
NULL    2015-04-13      3       2
NULL    2015-04-15      2       2
NULL    2015-04-16      2       2
2015-03 NULL    		5       1
2015-03 2015-03-10      4       3
2015-03 2015-03-12      1       3
2015-04 NULL    		6       1
2015-04 2015-04-12      2       3
2015-04 2015-04-13      3       3
2015-04 2015-04-15      2       3
2015-04 2015-04-16      2       3

-- 注：()对应0，month对应1，day对应2，(month,day)对应3

select month,day,COUNT(DISTINCT cookieid) AS uv,GROUPING__ID 
FROM dy1234 
GROUP BY month,day 
GROUPING SETS ((),day,month,(month,day));

NULL    NULL    		7       0
NULL    2015-03-10      4       2
NULL    2015-03-12      1       2
NULL    2015-04-12      2       2
NULL    2015-04-13      3       2
NULL    2015-04-15      2       2
NULL    2015-04-16      2       2
2015-03 NULL    		5       1
2015-03 2015-03-10      4       3
2015-03 2015-03-12      1       3
2015-04 NULL    		6       1
2015-04 2015-04-12      2       3
2015-04 2015-04-13      3       3
2015-04 2015-04-15      2       3
2015-04 2015-04-16      2       3

select month,day,COUNT(DISTINCT cookieid) AS uv,GROUPING__ID 
FROM dy1234 
GROUP BY day, month
GROUPING SETS ((),month,day,(month,day));

NULL    NULL    		7       0
2015-03 NULL    		5       2
2015-04 NULL    		6       2
NULL    2015-03-10      4       1
2015-03 2015-03-10      4       3
NULL    2015-03-12      1       1
2015-03 2015-03-12      1       3
NULL    2015-04-12      2       1
2015-04 2015-04-12      2       3
NULL    2015-04-13      3       1
2015-04 2015-04-13      3       3
NULL    2015-04-15      2       1
2015-04 2015-04-15      2       3
NULL    2015-04-16      2       1
2015-04 2015-04-16      2       3