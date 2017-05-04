
-- create table
create table  rptdata.fact_cpa_active_device_detail_daily(
device_key                  bigint,
app_channel_id              string,
become_new_unix_time        bigint,
app_os_type                 string,
imei                        string,
imsi                        string,
user_id                     string,
day7_keep_device_flag      tinyint,
month1_keep_device_flag    tinyint,
new_device_flag            tinyint,
abnormal_device_flag       tinyint,
play_device_flag           tinyint
)
partitioned by (src_file_day string)
stored as parquet;


set mapreduce.job.name=rptdata.fact_cpa_active_device_detail_daily_${SRC_FILE_DAY}_${SRC_FILE_PAST7DAY};

insert overwrite table rptdata.fact_cpa_active_device_detail_daily partition(src_file_day='${SRC_FILE_PAST7DAY}')  -- 为什么传的是7天前的数据？？？
select t1.device_key
      ,t1.app_channel_id
      ,t1.become_new_unix_time  
      ,t2.app_os_type
      ,t2.imei
      ,t2.imsi
      ,t2.user_id
      ,if(t4.device_key is null, 0, 1) day7_keep_device_flag                 --- 是否7日留存用户
      ,case when substr(add_months(concat_ws('-',substr('${SRC_FILE_PAST7DAY}',1,4),substr('${SRC_FILE_PAST7DAY}',5,2),'01')  -- YYYY-MM-01				
                                      , -1) ,1 ,7) = from_unixtime(bigint(t1.become_new_unix_time/1000),'yyyy-MM')
            then 1 else 0 end month1_keep_device_flag                       --- 是否上月留存用户
      ,t1.new_cnt new_device_flag                                                   --- 是否当前周期新增用户
      ,case when from_unixtime(bigint(t1.become_new_unix_time/1000),'HH') between '02' and '05'
            then 1 else 0 end abnormal_device_flag                             --- 是否异常用户
      ,if(t3.device_key is null, 0, 1) play_device_flag                           --- 是否使用用户
  from 
		(select a.device_key, a.app_channel_id
              ,max(a.new_cnt) new_cnt
              ,min(nvl(a.become_new_unix_time, a.upload_unix_time)) become_new_unix_time     -- become_new_unix_time这个字段存在为空的情况，当其为空时用最小的upload_unix_time代替
         from rptdata.fact_kesheng_sdk_new_device_hourly a
         where a.src_file_day = '${SRC_FILE_PAST7DAY}'
           and a.grain_ind = '100'
         group by a.device_key, a.app_channel_id
       ) t1   --- 七天前活跃的用户的基本数据（去重后）
 inner join 
       (select b.device_key, b.imsi, b.user_id, b.app_os_type
              ,if(b.app_os_type = 'AD', b.imei, b.idfa) imei
              ,row_number()over(partition by b.device_key order by b.dw_upd_hour desc) rnk  -- 去重&选出最近时间的记录
         from intdata.kesheng_sdk_active_device_hourly b
         where b.src_file_day = '${SRC_FILE_PAST7DAY}'           
       ) t2   --- 七天前最近时间活跃的用户的设备、imsi、账号、系统
    on (t1.device_key = t2.device_key)
  left join
       (select c.device_key
         from rptdata.fact_kesheng_sdk_playurl_detail_daily c
         where c.src_file_day = '${SRC_FILE_PAST7DAY}'
         group by c.device_key
       ) t3   --- 七天前使用用户的设备
    on (t1.device_key = t3.device_key)
  left join  
       (select d.device_key         
         from rptdata.fact_kesheng_sdk_new_device_hourly d
         where d.src_file_day = '${SRC_FILE_DAY}'
           and d.grain_ind = '100'         -- ？？？？？
           and from_unixtime(bigint(d.become_new_unix_time/1000),'yyyyMMdd') = '${SRC_FILE_PAST7DAY}'  -- become_new_unix_time 是unix时间戳毫秒值 /1000 转化为秒值,
		               -- from_unixtime(bigint unixtime[,string pattern])函数的参数unixtime需要秒值
         group by d.device_key
       ) t4   --- 当天活跃且七天前注册的设备
    on (t1.device_key = t4.device_key)
 where t2.rnk = 1;
 
 
-- become_new_unix_time -> 第一个upload_unix_time
-- 若new_cnt=1,则become_new_unix_time为空
-- 所以统计7日留存用户的时候用become_new_unix_time，
   --因为become_new_unix_time为空，那么一定不是留存的 
   --而upload_unix_time为空则不一定