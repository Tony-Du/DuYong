sqoop export
1、关系型数据库中的表必须已经存在
2、定义分割符来解析文件
3、默认操作实质是一些数据插入语言，也有更新模式

注意：oracle JDBC的jar包，安装到$Sqoop_HOME/lib目录下



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