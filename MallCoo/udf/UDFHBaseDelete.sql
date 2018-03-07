
1.更新文档

GET_MAP_DISTANCE(mall_longitude,mall_latitude,customer_longitude,customer_latitude)

CREATE FUNCTION GET_MAP_DISTANCE AS 'cn.mallcoo.udf.MapDistance' USING JAR 'hdfs:///udf/MapDistance-0.0.1-SNAPSHOT.jar';


Mark Down


2.测试hbase_delete

(1)创建hive-hbase表：

create table test.hive_hbase_test(
id string,
name string,
age int)
stored by'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping" = ":key,cf:name,cf:age")
tblproperties("hbase.table.name" = "hive_hbase_test");

(2)插入几条数据

hbase(main):011:0> put 'hive_hbase_test', 'second', 'cf:name', 'tony'
hbase(main):013:0> put 'hive_hbase_test', 'second', 'cf:age', '26'
hbase(main):015:0> put 'hive_hbase_test', 'third', 'cf:name', 'sisi'
hbase(main):016:0> put 'hive_hbase_test', 'third', 'cf:age', '25'


CREATE FUNCTION DELETE_HBASE_ROW AS 'cn.mallcoo.udf.UDFHBaseDelete' USING JAR 'hdfs:///udf/HBaseUtil-0.0.1-SNAPSHOT.jar';

select DELETE_HBASE_ROW("hadoop000,hadoop004,hadoop005","hive_hbase_test",a.id)
from test.hive_hbase_test a
where id = "third"





log 只是输出， exception 直接停掉程序


== 基础知识 ============
使用HiveQL创建一个指向HBase表(HBase 表不存在，但会被创建)：
create table hbase_stocks (
key int,
name string,
price float
)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties ("hbase.columns.mapping" = ":key,stock:val")
tblproperties ("hbase.table.name" = "stocks")

如果想创建一个指向一个已经存在的 HBase 表的 Hive 表的话，那么必须使用外部表：
create external table hbase_stocks(
key int,
name string,
price float
)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties ("hbase.columns.mapping" = "stock:val")
tblproperties ("hbase.table.name" = "stocks")