
-- create table
 create table intdata.kesheng_sdk_sub_play(
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
        client_ip               String,
        MG_MSG_TIME             String,
        Session                 String,
        Subsession              String,
        MG_MSG_PLAYER_VERSION   String,
        DataUsage               String,
        BeginTime               String,
        EndTime                 String,
        NetType                 String,
        PlayDuration            String,
        HostIP                  String,
        sub_networkType         String, 
        SubsessionServiceURL    String,
        SubsessionServiceIP     String,
        province_name           String,
        city_name               String,
        provider_name           String
)
partitioned by (src_file_day String,src_file_hour String)
stored as parquet;


-- 第一步：
 set mapreduce.job.name=intdata.kesheng_sdk_sub_play_${SRC_FILE_DAY}_${SRC_FILE_HOUR};
 
 insert overwrite table intdata.kesheng_sdk_sub_play partition (src_file_day='${SRC_FILE_DAY}',src_file_hour='${SRC_FILE_HOUR}')
 select s.clientId,
        s.imei,
        s.udid,
        s.idfa,
        s.idfv,
        s.appVersion,
        s.apppkg,
        s.networkType,
        s.os,
        s.appchannel,
        s.installationid,
        s.client_ip,        
        c.MG_MSG_TIME,
        c.Session,
        c.Subsession,
        c.MG_MSG_PLAYER_VERSION, 
        c.DataUsage, 
        c.BeginTime, 
        c.EndTime, 
        c.NetType, 
        c.PlayDuration, 
        c.HostIP, 
        c.sub_networkType,
        c.SubsessionServiceURL, 
        c.SubsessionServiceIP,
        s.province_name,            
        s.city_name,
        s.provider_name            
   from ods.kesheng_sdk_custominfo_json_v s
lateral view json_tuple(s.custominfo_json,
                        'MG_MSG_TIME',
                        'Session',
                        'Subsession',
                        'MG_MSG_PLAYER_VERSION',
                        'DataUsage',
                        'BeginTime',
                        'EndTime',
                        'NetType',
                        'PlayDuration',
                        'HostIP',
                        'networkType',
                        'SubsessionServiceURL',
                        'SubsessionServiceIP') c
     as MG_MSG_TIME,Session,Subsession,MG_MSG_PLAYER_VERSION,DataUsage,BeginTime,EndTime,NetType,PlayDuration,HostIP,sub_networkType,SubsessionServiceURL,SubsessionServiceIP
  where s.custominfo_type = '57000000' and src_file_day='${SRC_FILE_DAY}' and src_file_hour='${SRC_FILE_HOUR}';


