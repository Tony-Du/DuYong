
CREATE TABLE `pqr.WeixinCallbackMessage`(
  `id` string COMMENT '', 
  `mallid` bigint COMMENT '', 
  `event` int COMMENT '', 
  `eventkey` string COMMENT '', 
  `ticket` string COMMENT '', 
  `msgtype` int COMMENT '', 
  `msgid` int COMMENT '', 
  `tousername` string COMMENT '', 
  `fromusername` string COMMENT '', 
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
  'hdfs://hadoop000:9000/user/root/hive/pqr.db/weixincallbackmessage'
TBLPROPERTIES (
  'transient_lastDdlTime'='1472622335')
  

  
create table pqr.WeixinCallbackMessage2 (
id string , 
mallid bigint, 
event string, 
eventkey string,
ticket string,
msgtype string,
msgid string,
tousername string,
fromusername string,
createtime timestamp
)  
stored as orc;




insert into table pqr.WeixinCallbackMessage2  
select id,
       mallid,
       event,
       eventkey,
       ticket,
       msgtype,
       msgid,
       tousername,
       fromusername,
       createtime
  from pqr.WeixinCallbackMessage
 where date = 20171120 
 limit 5;      

drop table pqr.WeixinCallbackMessage;

create table pqr.WeixinCallbackMessage (
id string , 
mallid bigint, 
event string, 
eventkey string,
ticket string,
msgtype string,
msgid string,
tousername string,
fromusername string,
createtime timestamp
)  
stored as orc; 




insert into table pqr.WeixinCallbackMessage  
select id,
       mallid,
       event,
       eventkey,
       ticket,
       msgtype,
       msgid,
       tousername,
       fromusername,
       createtime
  from pqr.WeixinCallbackMessage2 

20171122开始同步的
  
maxid  
wx_cb_mess='1511279997_o-q3gjnYwgjmLSlnQNd5sV3EBtDg_7' 
 
 
 
 
 
48201292 
 
 

max_sns_event_sp=`echo "db.UserSNSEvent.find({},{_id:1}).sort({_id:-1}).limit(1)" | /apps/mongodb/bin/mongo --quiet sp:28001/SP_UserCenter | sed 's/NumberLong(//g'| tr -d ')'| JSON.sh -b | grep '_id' | awk '{print $2}'`

echo "db.UserSNSEvent.find({},{_id:1}).sort({_id:-1}).limit(1)" | /apps/mongodb/bin/mongo --quiet sp:28001/SP_UserCenter 
{ "_id" : NumberLong(7441789) }
echo "db.UserSNSEvent.find({},{_id:1}).sort({_id:-1}).limit(1)" | /apps/mongodb/bin/mongo --quiet sp:28001/SP_UserCenter | sed 's/NumberLong(//g'| tr -d ')'| JSON.sh -b | grep '_id' | awk '{print $2}'

echo "SP_UserCenter.UserSNSEvent --> pqr.UserSNSEvent"
/apps/datax/bin/datax.py -p "-Dstart=${sns_event_sp} -Dend=${max_sns_event_sp} -Dfilename=$filename" /apps/datax/job/user_sns_event_sp.json && sns_event_sp=$max_sns_event_sp
  



select mallid,newmallid from mongo.mall where mallid <> newmallid;



11969





{Date:{$lt:new Date("2017-09-24")}}


