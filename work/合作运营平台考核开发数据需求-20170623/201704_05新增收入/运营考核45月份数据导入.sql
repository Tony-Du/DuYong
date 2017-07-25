create table qushupingtai.qspt_hzyykh_201704_05_add_revenue(
statis_month     string
business_id      string
add_revenue      bigint
);


--1.把数据放到hdfs上的某个路径里

hadoop dfs -put '/home/oru/dy/data_accu_add_revenue_May.txt' 'hdfs上的目录' --hdfs目录麻烦你填一下

--2.创建一个临时外部表指向 步骤1的数据

create external table add_revenue_April_May (
statis_month string,
business_id string,
add_revenue bigint
)
row format delimited
fields terminated by '\t'
location 'hdfs上的目录';   --同上

--3.插入数据到 目标表分区


insert overwrite table qushupingtai.qspt_hzyykh_201704_05_add_revenue
select * from add_revenue_April_May;


