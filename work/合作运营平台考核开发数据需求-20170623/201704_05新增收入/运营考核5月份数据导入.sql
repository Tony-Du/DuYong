--1.把数据放到hdfs上的某个路径里

hadoop dfs -put '/.../data_accu_add_revenue_May.txt' 'hdfs上的目录' --hdfs目录麻烦你填一下

--2.创建一个临时外部表指向 步骤1的数据

create external table accu_add_revenue_May (
statis_month string,
business_id string,
accu_add_revenue bigint
)
row format delimited
fields terminated by '\t'
location 'hdfs上的目录';   --同上

--3.插入数据到 目标表分区

--alter table cdmpview.tmp_dy_05_assess_1_add_revenue add partition (src_file_month = '201705');

insert overwrite table cdmpview.tmp_dy_05_assess_1_add_revenue partition (src_file_month = '201705')
select * from accu_add_revenue_May;