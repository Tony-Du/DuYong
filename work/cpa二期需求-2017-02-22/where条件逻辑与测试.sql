
create table andtest (
channel_id string,
page_id string,
os string 
)
row format delimited fields terminated by ','
location '/tmp/yony11/';

load data local inpath '/home/hadoop/temp/test_data_dy/andtest_dat.txt' overwrite into table andtest;	


数据准备：
channel_id, page_id, os

1234567,yx01,ios	-- 都不为 -998

1234567,'-998',ios	--以下至少有一个为 -998
1234567,yx01,'-998'
'-998',yx01,ios
'-998','-998',ios
'-998',yx01,'-998'
1234567,'-998','-998'
'-998','-998','-998'


hive> select * from andtest where channel_id <> '\'-998\'' and page_id <> '\'-998\'' and os<>'\'-998\'';
OK
1234567	yx01	ios