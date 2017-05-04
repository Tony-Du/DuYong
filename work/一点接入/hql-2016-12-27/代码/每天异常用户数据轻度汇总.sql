
INSERT OVERWRITE TABLE odsdata.kesheng_sdk_exception PARTITION(src_file_day)
  SELECT 
        v.rowkey,
        v.clientId,
        v.imei,
        v.udid,
        v.idfa,
        v.idfv,
        v.appVersion,
        v.apppkg,
        v.networktype, 
        v.os,
        v.appchannel,
        v.installationID,
        v.client_ip,
        e.province_name,
        e.city_name,
        e.provider_name,
        v.exception_pos,
        v.name,
        v.reason,
        v.ts,
        v.src_file_day
FROM ods.kesheng_sdk_exception_v v LEFT JOIN mscdata.ip_city_provider_map e ON (regexp_extract(v.client_ip, '(.*)(\\..*)', 1) = e.ip_segment)
WHERE src_file_day = '${EXTRACT_DATE}';

-- ======================================================================================================================================= --

INSERT OVERWRITE TABLE intdata.kesheng_sdk_exception PARTITION(extract_date_label)
  SELECT 
        os,
        imei,
        null as imsi,
        idfa,
        idfv,
        null as sim,
        null as macId,
        appVersion,
        null as phoneMode,
        null as phoneBrand,
        null as phoneNumber,
        null as sreenWidth,
        null as sreenHeight,
        null as sreenDensity,
        null as appkey,
        apppkg,
        appchannel,		-- 
        null as userid,
        null as language,
        null as isBTB,
        null as backCount,
        null as uploadTs,
        clientId,
        null as sessionId,
        null as osversion,
        null as sdkversion,
        null as ints,
        networktype,
        null as networktype1,
        null as networktype2,
        client_ip,
        province_name,
        city_name,
        provider_name,
       c.name,
       c.reason,
       c.ts,
         installationID,
         src_file_day
FROM odsdata.kesheng_sdk_exception c
WHERE src_file_day = '${EXTRACT_DATE}';

-- ======================================================================================================================================= --

-- 每天异常用户数据轻度汇总
insert overwrite table rptdata.fact_kesheng_sdk_exception_daily partition(src_file_day='${EXTRACT_DATE}')
select nvl(t2.device_key,-998), t1.imei, t1.user_id, t1.idfa, t1.imsi, t1.app_ver_code
      ,t1.app_pkg_name, t1.src_app_channel_id, t1.phone_number, t1.app_os_type
      ,t1.first_launch_channel_id, t1.app_channel_id, t1.reason
      ,t1.product_key, count(1) as exception_cnt
  from (select if(nvl(trim(a1.imei),'') = '', '-998', trim(a1.imei)) imei
              ,if(nvl(trim(a1.userid),'') = '', '-998', trim(a1.userid)) user_id
              ,if(nvl(trim(a1.idfa),'') = '', '-998', trim(a1.idfa)) idfa
              ,if(nvl(trim(a1.imsi),'') = '', '-998', trim(a1.imsi)) imsi
              ,a1.appversion app_ver_code ,a1.apppkg app_pkg_name, a1.appchannel src_app_channel_id
              ,if(nvl(trim(a1.phonenumber),'') = '', '-998', trim(a1.phonenumber)) phone_number 
              ,a1.os as app_os_type
              --,a1.first_launch_channel_id
              --,nvl(trim(regexp_extract(a1.first_launch_channel_id,'([^-]+$)',1)),'-998') app_channel_id
              ,'' first_launch_channel_id
              ,a1.appchannel app_channel_id
              ,nvl(d1.product_key,-998) product_key, a1.reason
          from intdata.kesheng_sdk_exception a1
		  left join mscdata.dim_kesheng_sdk_app_pkg d1
             on d1.app_os_type = a1.os and d1.app_pkg_name = a1.apppkg
         where a1.extract_date_label = '${EXTRACT_DATE}'
       ) t1
  left join
       (select * from intdata.kesheng_sdk_active_device_hourly b2
         where b2.src_file_day = '${EXTRACT_DATE}'
       ) t2
    on (t1.imei = t2.imei and t1.user_id = t2.user_id and t1.idfa = t2.idfa 
        and t1.imsi = t2.imsi and t1.phone_number = t2.phone_number)
 group by nvl(t2.device_key,-998), t1.imei, t1.user_id, t1.idfa, t1.imsi, t1.app_ver_code
             ,t1.app_pkg_name, t1.src_app_channel_id, t1.phone_number, t1.app_os_type
             ,t1.first_launch_channel_id, t1.app_channel_id
             ,t1.product_key, t1.reason;

select appchannel,split(appchannel, '-')[length(appchannel) - length(translate(appchannel, '-', ''))] from intdata.kesheng_sdk_exception limit 100;