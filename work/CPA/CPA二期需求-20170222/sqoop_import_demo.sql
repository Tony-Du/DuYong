--shell文件所在路径
/mnt/bigdata-bdi/bigdata/etl/common/sqoopshell/sqoopfromcdmp.sh

--shell文件内容如下
#!/bin/bash
/usr/bin/sqoop import
--connect
jdbc:oracle:thin:@172.16.14.201:1521:cdmpdb1
--username
SQOOP
--password
POOQS123
--table
$1
--hive-import
--hive-overwrite
--hive-table
$2
--fields-terminated-by
"\0037"
--null-string
'\\N'
--null-non-string
'\\N'
--hive-partition-key
src_file_day
--hive-partition-value
$3
-m
$4

--BDI调度配置如下
sudo su - hadoop -c "bash #system.bigdataconfig#/bigdata/etl/common/sqoopshell/sqoopfromcdmp.sh CDMP_DW.TD_ICMS_DRAMA_SIN_REL cdmp_dw.td_icms_drama_sin_rel #flow.startDataTime# 1"



--连接数据库测试
sqoop list-tables --connect jdbc:oracle:thin:@172.16.14.201:1521:cdmpdb1 --username cdmp_dmt --password \!2012cdmp_dmt\!



