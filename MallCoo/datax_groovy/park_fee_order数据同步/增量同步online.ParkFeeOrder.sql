CREATE TABLE `online.ParkFeeOrder`(                                   
  `orderno` string,                                                   
  `uid` string,                                                       
  `parkname` string,                                                  
  `plateno` string,                                                   
  `entrytime` string,                                                 
  `leavetime` timestamp,                                              
  `parkingminutes` int,                                               
  `paytype` int,                                                      
  `bonus` int,                                                        
  `bonusamountoffset` double,                                         
  `parkingtotalfee` double,                                           
  `mallid` string,                                                    
  `parkingfee` double,                                                
  `createtime` timestamp,                                             
  `status` int,                                                       
  `malltimeoffset` double,                                            
  `mallamountoffset` double,                                          
  `memberrightamountoffset` double,                                   
  `memberrighttimeoffset` double,                                     
  `coupontimeoffset` double,                                          
  `couponamountoffset` double,                                      	
  `flag` string,                                                      
  `orderprice` double)                                                
ROW FORMAT SERDE                                                      
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'                       
STORED AS INPUTFORMAT                                               
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'                   
OUTPUTFORMAT                                                          
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'                  
LOCATION                                                              
  'hdfs://hadoop000:9000/user/root/hive/online.db/parkfeeorder'       
TBLPROPERTIES (                                                       
  'COLUMN_STATS_ACCURATE'='true',                                     
  'last_modified_by'='root',                                          
  'last_modified_time'='1495093399',                                  
  'numFiles'='1',                                                     
  'numRows'='15776622',                                               
  'rawDataSize'='12521464264',                                        
  'totalSize'='359442612', 
  'transient_lastDdlTime'='1511369008')
  
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
   
 
 
 
 
 			

 
 
 
  
  
  
  
  








































  
  



