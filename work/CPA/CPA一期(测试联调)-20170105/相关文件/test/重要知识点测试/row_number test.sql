-- 数据准备
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
month STRING,
day STRING, 
cookieid STRING 
) ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
stored as textfile location '/tmp/tony11/';
=================================================================
select month, day, cookieid, 
row_number() over(partition by month order by day) row_num,
'000' grain_ind
from dy1234;
2015-03	2015-03-10	cookie5	1	000
2015-03	2015-03-10	cookie3	2	000
2015-03	2015-03-10	cookie2	3	000
2015-03	2015-03-10	cookie1	4	000
2015-03	2015-03-12	cookie7	5	000
2015-04	2015-04-12	cookie5	1	000
2015-04	2015-04-12	cookie3	2	000
2015-04	2015-04-13	cookie6	3	000
2015-04	2015-04-13	cookie4	4	000
2015-04	2015-04-13	cookie2	5	000
2015-04	2015-04-15	cookie2	6	000
2015-04	2015-04-15	cookie3	7	000
2015-04	2015-04-16	cookie1	8	000
2015-04	2015-04-16	cookie4	9	000
=========================================================================
select month, day, cookieid, 
row_number() over(partition by month,day order by day) row_num,
'010' grain_ind
from dy1234;
2015-03 2015-03-10      cookie5 1       010
2015-03 2015-03-10      cookie3 2       010
2015-03 2015-03-10      cookie2 3       010
2015-03 2015-03-10      cookie1 4       010
2015-03 2015-03-12      cookie7 1       010
2015-04 2015-04-12      cookie5 1       010
2015-04 2015-04-12      cookie3 2       010
2015-04 2015-04-13      cookie6 1       010
2015-04 2015-04-13      cookie4 2       010
2015-04 2015-04-13      cookie2 3       010
2015-04 2015-04-15      cookie2 1       010
2015-04 2015-04-15      cookie3 2       010
2015-04 2015-04-16      cookie1 1       010
2015-04 2015-04-16      cookie4 2       010