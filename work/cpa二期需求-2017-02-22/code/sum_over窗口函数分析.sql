
drop table if exists temp.over_partition_test_dy;

create table temp.over_partition_test_dy (
a int,
b int,
c int
) row format delimited
fields terminated by ',';


a b	c
1,1,3
2,2,3
3,3,3
4,4,3
5,5,3
6,5,3
7,2,3
8,2,8
9,3,3


load data local inpath '/home/hadoop/test_data/over_partition_test_dy.txt' overwrite into table temp.over_partition_test_dy;


select a, b, c, sum(c) over(order by b ) as sum1, sum(c) over() as sum2 from temp.over_partition_test_dy t;
+----+----+----+-------+-------+--+
| a  | b  | c  | sum1  | sum2  |
+----+----+----+-------+-------+--+
| 5  | 5  | 3  | 32    | 32    |3 +8+3+3 +3+3 +3 +3+3
| 6  | 5  | 3  | 32    | 32    |3 +8+3+3 +3+3 +3 +3+3 =3+14+6+3+6
| 4  | 4  | 3  | 26    | 32    |3 +8+3+3 +3+3 +3      =3+14+6+3
| 3  | 3  | 3  | 23    | 32    |3 +8+3+3 +3+3 
| 9  | 3  | 3  | 23    | 32    |3 +8+3+3 +3+3         =3+14+6
| 2  | 2  | 3  | 17    | 32    |3 +8+3+3
| 7  | 2  | 3  | 17    | 32    |3 +8+3+3
| 8  | 2  | 8  | 17    | 32    |3 +8+3+3              =3+14 
| 1  | 1  | 3  | 3     | 32    |3                     =3
+----+----+----+-------+-------+--+
-- 1、按b进行升序排列
-- 2、序号n的值 = 序号1的和 + 序号2的和 +...+ 序号n-1的和 + 序号n的和
-- 3、不排序，求c列所有值的和
select a, b, c, sum(c) over(order by b desc) as sum1, sum(c) over() as sum2 from temp.over_partition_test_dy t;
+----+----+----+-------+-------+--+
| a  | b  | c  | sum1  | sum2  |
+----+----+----+-------+-------+--+
| 1  | 1  | 3  | 32    | 32    |3+3 +3 +3+3 ++8+3+3 +3
| 2  | 2  | 3  | 29    | 32    |3+3 +3 +3+3 ++8+3+3
| 7  | 2  | 3  | 29    | 32    |3+3 +3 +3+3 ++8+3+3
| 8  | 2  | 8  | 29    | 32    |3+3 +3 +3+3 ++8+3+3
| 3  | 3  | 3  | 15    | 32    |3+3 +3 +3+3
| 9  | 3  | 3  | 15    | 32    |3+3 +3 +3+3
| 4  | 4  | 3  | 9     | 32    |3+3 +3
| 5  | 5  | 3  | 6     | 32    |3+3
| 6  | 5  | 3  | 6     | 32    |3+3
+----+----+----+-------+-------+--+


select a, b, c, sum(c) over(partition by b) as sum1, sum(c) over(partition by b order by a) as sum2 from temp.over_partition_test_dy t;
+----+----+----+-------+-------+--+
| a  | b  | c  | sum1  | sum2  |
+----+----+----+-------+-------+--+
| 1  | 1  | 3  | 3     | 3     |
| 2  | 2  | 3  | 14    | 3     |
| 7  | 2  | 3  | 14    | 6     |
| 8  | 2  | 8  | 14    | 14    |
| 3  | 3  | 3  | 6     | 3     |
| 9  | 3  | 3  | 6     | 6     |
| 4  | 4  | 3  | 3     | 3     |
| 5  | 5  | 3  | 6     | 3     |
| 6  | 5  | 3  | 6     | 6     |
+----+----+----+-------+-------+--+
-- sum1: 按b分组，组内进行求和
-- sum2: 按b分组; 组内进行排序，再求值（序号n的值 = 序号1的和 + 序号2的和 +...+ 序号n-1的和 + 序号n的和 ）


