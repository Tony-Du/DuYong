show create table mallcoo_sp.QrMap;
OK
CREATE EXTERNAL TABLE `mallcoo_sp.QrMap`(
  `id` string COMMENT 'from deserializer', 
  `qrid` string COMMENT 'from deserializer', 
  `mallid` bigint COMMENT 'from deserializer', 
  `qrstring` string COMMENT 'from deserializer', 
  `qrparam` string COMMENT 'from deserializer', 
  `iswx` boolean COMMENT 'from deserializer', 
  `wxqr_id` bigint COMMENT 'from deserializer', 
  `wxqr_id_per` string COMMENT 'from deserializer', 
  `istemp` boolean COMMENT 'from deserializer', 
  `qrbusinesstype` int COMMENT 'from deserializer', 
  `createtime` timestamp COMMENT 'from deserializer')
COMMENT ''
ROW FORMAT SERDE 
  'com.mongodb.hadoop.hive.BSONSerDe' 
STORED BY 
  'com.mongodb.hadoop.hive.MongoStorageHandler' 
WITH SERDEPROPERTIES ( 
  'mongo.columns.mapping'='{\"id\":\"_id\",\"QrID\":\"QrID\",\"MallID\":\"MallID\",\"QrString\":\"QrString\",\"QrParam\":\"QrParam\",\"IsWx\":\"IsWx\",\"WXQR_ID\":\"WXQR_ID\",\"WXQR_ID_Per\":\"WXQR_ID_Per\",\"IsTemp\":\"IsTemp\",\"QrBusinessType\":\"QrBusinessType\",\"CreateTime\":\"CreateTime\"}', 
  'serialization.format'='1')
LOCATION
  'hdfs://hadoop000:9000/user/root/hive/mallcoo_sp.db/qrmap'

  

show create table pqr.QrMap;              
OK                                        
CREATE TABLE `pqr.QrMap`(                 
  `id` string COMMENT '',                 
  `qrid` string COMMENT '',               
  `mallid` bigint COMMENT '',             
  `qrstring` string COMMENT '',           
  `qrparam` string COMMENT '',            
  `iswx` boolean COMMENT '',              
  `wxqr_id` bigint COMMENT '',            
  `wxqr_id_per` string COMMENT '',        
  `istemp` boolean COMMENT '',            
  `qrbusinesstype` int COMMENT '',        
  `createtime` timestamp COMMENT '')      
COMMENT ''                                
PARTITIONED BY (                          
  `date` string COMMENT '')               
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://hadoop000:9000/user/root/hive/pqr.db/qrmap'
  
  
--数据备份  
create table pqr.QrMap2(
`id` string,
`qrid` string,
`mallid` bigint,
`qrstring` string,
`qrparam` string,
`iswx` boolean,
`wxqr_id` bigint,
`wxqr_id_per` string,
`istemp` boolean,
`qrbusinesstype` int,
`createtime` timestamp
)
stored as orc;


insert into table pqr.QrMap2  --已删除
select id,
       qrid,
       mallid,
       qrstring,
       qrparam,
       iswx,
       wxqr_id,
       wxqr_id_per,
       istemp,
       qrbusinesstype,
       createtime
  from pqr.QrMap; 
       
select count(*) from pqr.QrMap;
1267169

select count(*) from pqr.QrMap2;
1267169



source $dir/conf/maxid  --需要找到pqr.QrMap中的最大的id
select id from pqr.QrMap where date = 20171123 order by id desc limit 10;
1267169


max_qr_map=`echo "db.QrMap.find({},{_id:1}).sort({_id:-1}).limit(1)" | /apps/mongodb/bin/mongo --quiet sp:28001/SP_OPService | JSON.sh -b |sed 's/NumberLong(//g' | tr -d ')' | awk '{print $2}'| tr -d '"'`

echo "
  wx_cb_mess start: $wx_cb_mess    end: $max_wx_cb_mess
      qr_map start：$qr_map        end: $max_qr_map
          filename: $filename
"

echo "SP_OPService.QrMap --> pqr.QrMap"
/apps/datax/bin/datax.py -p "-Dstart=\'${qr_map}\' -Dend=\'${max_qr_map}\' -Dfilename=$filename" /apps/datax/job/qr_map.json && qr_map=$max_qr_map


echo "sns_event=$sns_event
sns_event_sp=$sns_event_sp
cons=$cons
bonus=$bonus
grade=$grade
wx_cb_mess=$wx_cb_mess
qr_map=$qr_map" > $dir/conf/maxid



    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

