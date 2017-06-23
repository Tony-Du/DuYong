create table intdata.kesheng_sdk_play_buffer_detail(
    clientId                String,
    imei                    String,
    udid                    String,
    idfa                    String,
    idfv                    String,
    appVersion              String,
    apppkg                  String,
    networkType             String,
    os                      String,
    appchannel              String,
    installationID          String,
    MG_MSG_TIME             String,
    Session                 String,
    Subsession              String,
    MG_MSG_STUCK_START      String,
    networkType2            String,
    MG_MSG_STUCK_END        String,
    MG_MSG_STUCK_DURATION   String,
    SubsessionServiceURL    String,
    SubsessionServiceIP     String
)
partitioned by(src_file_day String,src_file_hour String)
stored as parquet;

=================================================================================================================

create or replace view int.kesheng_sdk_play_buffer_detail_v
as select 
   a.clientid, 
   a.imei, 
   a.udid, 
   a.idfa, 
   a.idfv, 
   a.appversion, 
   a.apppkg, 
   a.networktype, 
   a.os, 
   a.appchannel, 
   a.installationid,
   a.src_file_day,
   a.src_file_hour,   
   b.MG_MSG_TIME,
   b.Session,
   b.Subsession,
   b.MG_MSG_STUCK_START,
   b.MG_MSG_STUCK_END,
   b.MG_MSG_STUCK_DURATION,
   b.SubsessionServiceURL,
   b.SubsessionServiceIP
from ods.kesheng_sdk_json_sessioninfo_v a
lateral view json_tuple(a.custominfo_json,
                        'MG_MSG_TIME',
                        'Session',
                        'Subsession',
                        'MG_MSG_STUCK_START',
                        'MG_MSG_STUCK_END',
                        'MG_MSG_STUCK_DURATION',
                        'SubsessionServiceURL',
                        'SubsessionServiceIP') b 
as MG_MSG_TIME,Session,Subsession,MG_MSG_STUCK_START,MG_MSG_STUCK_END,MG_MSG_STUCK_DURATION,SubsessionServiceURL,SubsessionServiceIP
where a.custominfo_type = '56000015';

===================================================================================================================

set mapreduce.job.name=intdata.kesheng_sdk_play_buffer_detail_${SRC_FILE_DAY}__${SRC_FILE_HOUR};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

add jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

insert overwrite table intdata.kesheng_sdk_play_buffer_detail partition(src_file_day,src_file_hour)
select
   clientid, 
   imei, 
   udid, 
   idfa, 
   idfv, 
   appversion, 
   apppkg, 
   networktype, 
   os, 
   appchannel, 
   installationid,      
   MG_MSG_TIME,
   Session,
   Subsession,
   MG_MSG_STUCK_START,
   '' as networkType2,
   MG_MSG_STUCK_END,
   MG_MSG_STUCK_DURATION,
   SubsessionServiceURL,
   SubsessionServiceIP,
   src_file_day,
   src_file_hour
from int.kesheng_sdk_play_buffer_detail_v
where src_file_day='${SRC_FILE_DAY}' and src_file_hour='${SRC_FILE_HOUR}';



select * from intdata.kesheng_sdk_play_buffer_detail where src_file_day='20170124' and src_file_hour='00' limit 1;



















































