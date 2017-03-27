sqoop export
1、关系型数据库中的表必须已经存在
2、定义分割符来解析文件
3、默认操作实质是一些数据插入语言，也有更新模式

注意：oracle JDBC的jar包，安装到$Sqoop_HOME/lib目录下

/***********************************************************/
11g之后使用interval来实现自动扩展分区，简化了维护。
根据年: INTERVAL(NUMTOYMINTERVAL(1,'YEAR'))

根据月: INTERVAL(NUMTOYMINTERVAL(1,'MONTH'))
根据天: INTERVAL(NUMTODSINTERVAL(1,'DAY'))

根据时分秒: NUMTODSINTERVAL( n, { 'DAY'|'HOUR'|'MINUTE'|'SECOND'})

下面用按月自动扩展来做个试验：

SQL> create table t_range (id number not null PRIMARY KEY, test_date date)
partition by range (test_date) interval (numtoyMinterval (1,'MONTH'))
(
partition p_2014_01_01 values less than (to_date('2014-01-01', 'yyyy-mm-dd'))
);
--看到只有一个分区
SQL> select partition_name from user_tab_partitions where table_name='T_RANGE';
PARTITION_NAME
------------------------------
P_2014_01_01


SQL> insert /*+append */ into t_range select rownum,
to_date(to_char(sysdate - 140, 'J') + trunc(dbms_random.value(0, 80)), 'J')
from dual
connect by rownum <= 100000;
SQL> commit;

--可以看到SYS开头的分区是自动扩展的
SQL> select partition_name from user_tab_partitions where table_name='T_RANGE';
PARTITION_NAME
------------------------------
P_2014_01_01
SYS_P21
SYS_P22
SYS_P23

--如果对分区名不太爽，则可以自己修改一下：

alter table t_range rename partition SYS_P21 to p_2014_02_01;
alter table t_range rename partition SYS_P22 to p_2014_03_01;
alter table t_range rename partition SYS_P23 to p_2014_04_01;
/***********************************************************/


-- 再帖一个：按天间隔分区、按小时子分区的例子给你：
-- Step 1: 创建表：(按小时子分区)
CREATE TABLE BIEE.DW_ADS_ADI_VADS_HOUR2
(
DATE_ID NUMBER(8),
HOUR_ID NUMBER(2),
SITE_ID number(1),
SUB_CHANNEL_ID number(7),
CITY_ID number(7),
FADI number(38,0),
RADI number(38,0),
date_time as (to_date(date_id,'YYYYMMDD'))
)
PARTITION BY RANGE (date_time) INTERVAL(NUMTODSINTERVAL(1,'day'))
STORE IN (adv_m01_tbs,adv_m02_tbs,adv_m03_tbs,adv_m04_tbs,adv_m05_tbs,adv_m06_tbs,adv_m07_tbs,adv_m08_tbs,adv_m09_tbs,adv_m10_tbs,adv_m11_tbs,adv_m12_tbs) 
SUBPARTITION BY LIST (HOUR_ID)
  SUBPARTITION TEMPLATE
  ( SUBPARTITION p_0 VALUES(0),
    SUBPARTITION p_1 VALUES(1),
    SUBPARTITION p_2 VALUES(2),
    SUBPARTITION p_3 VALUES(3),
    SUBPARTITION p_4 VALUES(4),
    SUBPARTITION p_5 VALUES(5),
    SUBPARTITION p_6 VALUES(6),
    SUBPARTITION p_7 VALUES(7),
    SUBPARTITION p_8 VALUES(8),
    SUBPARTITION p_9 VALUES(9),
    SUBPARTITION p_10 VALUES(10),
    SUBPARTITION p_11 VALUES(11),
    SUBPARTITION p_12 VALUES(12),
    SUBPARTITION p_13 VALUES(13),
    SUBPARTITION p_14 VALUES(14),
    SUBPARTITION p_15 VALUES(15),
    SUBPARTITION p_16 VALUES(16),
    SUBPARTITION p_17 VALUES(17),
    SUBPARTITION p_18 VALUES(18),
    SUBPARTITION p_19 VALUES(19),
    SUBPARTITION p_20 VALUES(20),
    SUBPARTITION p_21 VALUES(21),
    SUBPARTITION p_22 VALUES(22),
    SUBPARTITION p_23 VALUES(23)
  )
(PARTITION P20121201_LS VALUES LESS THAN (TO_DATE('20121201','YYYYMMDD')) TABLESPACE adv_m11_tbs)
PARALLEL;


CREATE TABLE BIEE.DW_ADS_ADI_VADS_HOUR 
(
DATE_ID NUMBER(8),
HOUR_ID NUMBER(2),
SITE_ID number(1),
SUB_CHANNEL_ID number(7),
CITY_ID number(7),
FADI number(38,0),
RADI number(38,0),
date_time as (to_date(date_id,'YYYYMMDD'))
)
PARTITION BY RANGE (date_time)
INTERVAL(NUMTODSINTERVAL(1,'day')) 
STORE IN (adv_m01_tbs,adv_m02_tbs,adv_m03_tbs,adv_m04_tbs,adv_m05_tbs,adv_m06_tbs,adv_m07_tbs,adv_m08_tbs,adv_m09_tbs,adv_m10_tbs,adv_m11_tbs,adv_m12_tbs) 
(PARTITION P20121201_LS VALUES LESS THAN (TO_DATE('20121201','YYYYMMDD')) TABLESPACE adv_m11_tbs);

-- 上面的 date_time字段是基于date_id虚拟的一个日期类型字段，然后表根据这个虚拟字段“按天”分区，并且　存放的表空间是：adv_m01_tbs、adv_m02_tbs、...、adv_m12_tbs
-- 起始分区为 P20121201_LS ，2012年12月1号之前的数据将存放在这个分区中。




sqoop import -D oracle.sessionTimeZone=Asia/Shanghai \ #不多说，一看便知，格式化时区
--connect 
jdbc:oracle:thin:@127.0.0.1:1521:ss \ #数据的基本链接
--username 
root \
--password 
root \
--table 
t_table \ #oracle中需要被同步的表格
--columns 
KEY,KPV,KTIME \ #假设oracle表的字段很多很多。此处是需要同步的字段。注意是大写的！！！！
--where 
"TRUNC(KTIME)>=TRUNC(ADD_MONTHS(SYSDATE,-1),'month') AND TRUNC(KTIME)<=TRUNC(LAST_DAY(ADD_MONTHS(SYSDATE,-1)))" \ #假设很多很多数据，我只要上个月的数据。
--warehouse-dir 
/hadoop/zc/t_table \ #sqoop同步到hive中中间数据存储的位置。 
--hive-import \ #同步到hive里面
-m 
1 \ #map的个数
--split-by 
KEY \ #根据KEY字段分区
--hive-table 
hive.table \ #同步到hive库的table表中
--hive-overwrite #如果有数据的话，全部覆盖

其中oracle的同步需要注意时区的格式化。

oracle同步如果不指定split-by会同步失败


sqoop --options-file /users/homer/work/import.txt 


import.txt的文件内容如下：
import
--connect
jdbc:mysql://localhost/db
--username
foo
--table 
TEST


--首先必须要在oracle中创建一个空表empty()

sqoop export --connect jdbc:oracle:thin:@//192.168.27.235:1521/ORCL --username scott --password liujiyu --table empty --export-dir '/liujiyu/nihao/part-m-00000' --fields-terminated-by ':' -m  1


export 
--connect 
jdbc:mysql://192.168.1.110:3306/school 
--username 
root 
--password 
123456
 

--table 
student1 
--columns
""
--export-dir 
/tem/stu2/part-m-00000



export 
 
--connect 
jdbc:oracle:thin:@localhost:port:test1   
--username 
test 
--password 
test

-m 
1
--null-string
'' 
    
--export-dir 
/user/hive/warehouse/data_w.db/seq_fdc_jplp 
--table 
FDC_JPLP    
--columns  
"goal_ocityid,goal_issueid,compete_issueid,ncompete_rank"  
--input-fields-terminated-by 
'\001'     
--input-lines-terminated-by 
'\n' 