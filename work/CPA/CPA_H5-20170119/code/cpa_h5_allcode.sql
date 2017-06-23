
CREATE EXTERNAL TABLE `ods.kesheng_h5_json_ex`(
  `json` string)
PARTITIONED BY ( 
  `src_file_day` string, 
  `src_file_hour` string)
 
LOCATION
 'hdfs://ns1/user/hadoop/ods/migu/kesheng/kesheng_h5'
 
alter table ods.kesheng_h5_json_ex add if not exists partition(src_file_day='20170320', src_file_hour='13');

 --  /user/hadoop/ods/migu/kesheng/kesheng_h5/20170216/11/
 
-- == ods.kesheng_h5_json_ex_v ==========================================

CREATE VIEW `ods.kesheng_h5_json_ex_v` AS select 
		`h5`.`url`
		,`h5`.`array_path`[size(`h5`.`array_path`)-2] `product_id`
		,translate(`h5`.`array_path`[size(`h5`.`array_path`)-1], '.jsp', '') `page_id`
		,`h5`.`channel_id`
		,`h5`.`os` 
		,`h5`.`time_stamp` 
		,`h5`.`data_src_type` 
		,`h5`.`param_json`
		,`h5`.`src_file_day`
		,`h5`.`src_file_hour`
   from (select `m1`.`url`
               ,`m1`.`os` 
			   ,`m1`.`time_stamp` 
			   ,`pt`.`data_src_type` 
			   ,`a1`.`param_json`
               ,split(parse_url(`m1`.`url`,'PATH'), '/') `array_path`	-- /mgsp/yx01.jsp
               ,parse_url(`m1`.`url`,'QUERY','channelId') `channel_id`	-- 12345678901
               ,`a0`.`src_file_day`
			   ,`a0`.`src_file_hour`
           from `ods`.`kesheng_h5_json_ex` `a0`
                lateral view json_tuple(`a0`.`json`, 'measure', 'param') `a1` as `measure_json`, `param_json`
                lateral view json_tuple(`a1`.`measure_json`, 'URL', 'OS', 'timeStamp') `m1` as `url`, `os`, `time_stamp`
                lateral view json_tuple(`a1`.`param_json`, 'type') `pt` as `data_src_type`
       ) `h5`

-- == 测试：
select * from ods.kesheng_h5_json_ex limit 3;
{"measure":{"URL":"http://h5.miguvideo.com/mgsp/yx01.jsp?channelId=12345678901"
			,"OS":"iOS"
			,"timeStamp":"1480521763049"}
,"param": {"type":"common","IP":"127.0.0.1","cookieId":"asdf435234fqw2323vwe34","title":"migu movie","domain":"222.66.199.177","referrer":"222.66.199.177","width":"360","height":"640","lang":"zh-CN"}
}	
20170208	
15

{"measure":{"URL":"h5.miguvideo.com/mgzb/yx02.jsp?channelId=22345678901"
			,"OS":"iOS"
			,"timeStamp":"1480521763049"}
,"param": {"type":"pageVisit", "cookieId":"asdf435234fqw2323vwe34", "IP":"127.0.0.1","startTime":"1480521763049","endTime":"1480521763049", "visitTime": 234}
}	
20170208	
15

{"measure":{"URL":"http://127.0.0.1/migu/aa.jsp"
			,"OS":"iOS"
			,"timeStamp":"1480521763049"}
,"param": {"type":"downloadClick","description":"xxxxxx","aaa":"bbb"}
}	
20170208	
15

-- == intdata.kesheng_h5_common_event =======================================
	   
set mapreduce.job.name=intdata.kesheng_h5_common_event_${SRC_FILE_DAY}_${SRC_FILE_HOUR};

insert overwrite table intdata.kesheng_h5_common_event partition(src_file_day='${SRC_FILE_DAY}',src_file_hour='${SRC_FILE_HOUR}')
select a.url,
       nvl(a.os,'-998'),
       a.time_stamp,
       a.ip_addr,
       a.cookie_id,
       a.title,
       a.ip_domain,
       a.referrer,
       a.height,
       a.lang,
       a.width,
       nvl(a.product_id,'-998'),
       nvl(a.page_id,'-998'),
       nvl(a.channel_id,'-998')
  from (select b.url,
               b.os,
               b.time_stamp,
               c.ip_addr,
               c.cookie_id,
               c.title,
               c.ip_domain,
               c.referrer,
               c.height,
               c.lang,
               c.width,
               b.product_id,
               b.page_id,
               b.channel_id,
               b.data_src_type
          from ods.kesheng_h5_json_ex_v b
       lateral view json_tuple(b.param_json,'IP','cookieId','title','domain','referrer','height','lang','width') c 
            as ip_addr,cookie_id,title,ip_domain,referrer,height,lang,width
         where b.data_src_type = 'common' and b.src_file_day = '${SRC_FILE_DAY}' and b.src_file_hour = '${SRC_FILE_HOUR}' 
	  )a;

-- == intdata.kesheng_h5_page_visit_event =======================================

set mapreduce.job.name=intdata.kesheng_h5_page_visit_event_${SRC_FILE_DAY}_${SRC_FILE_HOUR};

insert overwrite table intdata.kesheng_h5_page_visit_event partition(src_file_day='${SRC_FILE_DAY}',src_file_hour='${SRC_FILE_HOUR}')
select a.url,
       nvl(a.os,'-998'),
	   a.time_stamp,
	   a.ip_addr,
	   a.cookie_id,
	   a.start_time,
	   a.end_time,
	   a.visit_duration_ms,
       nvl(a.product_id,'-998'),
       nvl(a.page_id,'-998'),
       nvl(a.channel_id,'-998')
 from (select b.url,
              b.os,
			  b.time_stamp,
			  c.ip_addr,
			  c.cookie_id,
			  c.start_time,
			  c.end_time,
			  c.visit_duration_ms,
			  b.product_id,
			  b.page_id,
			  b.channel_id,
			  b.data_src_type
         from ods.kesheng_h5_json_ex_v b
	  lateral view json_tuple(b.param_json,'IP','cookieId','startTime','endTime','visitTime')c 
           as ip_addr,cookie_id,start_time,end_time,visit_duration_ms
        where b.data_src_type='pageVisit' and c.visit_duration_ms > 0 and b.src_file_day='${SRC_FILE_DAY}' and b.src_file_hour='${SRC_FILE_HOUR}' 
	  )a;	-- 剔除访问时长小于0的记录

	  
-- == intdata.kesheng_h5_download_event =======================================

set mapreduce.job.name=intdata.kesheng_h5_download_event_${SRC_FILE_DAY}_$(SRC_FILE_HOUR);
insert overwrite table intdata.kesheng_h5_download_event partition(src_file_day='${SRC_FILE_DAY}',src_file_hour='${SRC_FILE_HOUR}')
select a.url,
       nvl(a.os,'-998'),
	   a.time_stamp,
	   a.description,
       nvl(a.product_id,'-998'),
       nvl(a.page_id,'-998'),
       nvl(a.channel_id,'-998')
  from (select b.url,
               b.os,
			   b.time_stamp,
			   c.description,
			   b.product_id,
			   b.page_id,
			   b.channel_id,
			   b.data_src_type
		  from ods.kesheng_h5_json_ex_v b
	   lateral view json_tuple(b.param_json,'description')c as description
         where b.data_src_type='downloadClick' and b.src_file_day='${SRC_FILE_DAY}' and b.src_file_hour='${SRC_FILE_HOUR}'	  
       )a;

	   
-- == rptdata.fact_kesheng_h5_common_event_daily =======================================

 set mapreduce.job.name=rptdata.fact_kesheng_h5_common_event_daily_${SRC_FILE_DAY};
 insert overwrite table rptdata.fact_kesheng_h5_common_event_daily  partition(src_file_day='${SRC_FILE_DAY}')
 select c.product_key,
        c.channel_id,
	    c.page_id,
	    c.os,
	    count(distinct(concat_ws('-',c.cookie_id,c.ip_addr))) uv_num,       --页面访问用户数  cookie和IP合并组合判定，完全一样识别为一个用户
	    count(1) pv_cnt,                                                           --页面访问次数   带页面ID的访问请求次数,不踢重
        rpad(reverse(bin(cast(grouping__id as int))),4,'0') grain_ind
   from (select nvl(b.product_key,-998) product_key,    
                a.channel_id channel_id,
                a.page_id page_id,
                a.os os,
                a.cookie_id,           
                a.ip_addr                                                  
		   from intdata.kesheng_h5_common_event a
           left join mscdata.dim_kesheng_h5_product b on a.product_id = b.product_id
          where a.src_file_day='${SRC_FILE_DAY}' and a.channel_id<>'-998'	--剔除渠道id,页面id和操作系统至少一者为-998的情况
            and a.page_id<>'-998' and a.os<>'-998'
         )c
   group by product_key,channel_id,page_id,os
grouping sets( product_key,
              (product_key,channel_id),
			  (product_key,page_id),
			  (product_key,os),
              (product_key,channel_id,page_id),
			  (product_key,channel_id,os),
			  (product_key,page_id,os),
			  (product_key,channel_id,page_id,os)
			 );
	 
			 
-- == rptdata.fact_kesheng_h5_common_event_hourly =======================================

set mapreduce.job.name=rptdata.fact_kesheng_h5_common_event_hourly_${SRC_FILE_DAY}_$(SRC_FILE_HOUR);
 insert overwrite table rptdata.fact_kesheng_h5_common_event_hourly partition(src_file_day='${SRC_FILE_DAY}',src_file_hour='${SRC_FILE_HOUR}')
 select c.product_key,
	    c.channel_id,
	    c.page_id,
	    c.os,
	    count(distinct(concat_ws('-',c.cookie_id,c.ip_addr))) uv_num,       --页面访问用户数  cookie和IP合并组合判定，完全一样识别为一个用户
	    count(1) pv_cnt,                                                    --页面访问次数   带页面ID的访问请求次数,不踢重
        rpad(reverse(bin(cast(grouping__id as int))),4,'0') grain_ind
   from(select  nvl(b.product_key,-998) product_key,    
                a.channel_id channel_id,
                a.page_id page_id,
                a.os os,
                a.cookie_id,           
                a.ip_addr                                                  
		  from intdata.kesheng_h5_common_event a
          left join mscdata.dim_kesheng_h5_product b on a.product_id = b.product_id
         where a.src_file_day='${SRC_FILE_DAY}' and a.src_file_hour='${SRC_FILE_HOUR}' 
           and a.channel_id<>'-998' and a.page_id<>'-998' and a.os<>'-998'
       )c
   group by product_key,channel_id,page_id,os
grouping sets( product_key,
              (product_key,channel_id),
			  (product_key,page_id),
			  (product_key,os),
              (product_key,channel_id,page_id),
			  (product_key,channel_id,os),
			  (product_key,page_id,os),
			  (product_key,channel_id,page_id,os)
			 );


-- == rptdata.fact_kesheng_h5_download_event_hourly =======================================

set mapreduce.job.name=rptdata.fact_kesheng_h5_download_event_hourly_${SRC_FILE_DAY}_$(SRC_FILE_HOUR);
 insert overwrite table rptdata.fact_kesheng_h5_download_event_hourly  partition(src_file_day='${SRC_FILE_DAY}',src_file_hour='${SRC_FILE_HOUR}')
 select c.product_key,
	    c.channel_id,
	    c.page_id,
	    c.os,
        count(1),                 --点击下载次数	
        rpad(reverse(bin(cast(grouping__id as int))),4,'0') grain_ind   
   from(select nvl(b.product_key,-998) product_key, 
               a.channel_id channel_id,
		       a.page_id page_id,
		       a.os os
          from intdata.kesheng_h5_download_event a
          left join mscdata.dim_kesheng_h5_product b on a.product_id = b.product_id
         where a.src_file_day='${SRC_FILE_DAY}' and a.src_file_hour='${SRC_FILE_HOUR}' 
           and a.channel_id<>'-998' and a.page_id<>'-998' and a.os<>'-998'
        )c
   group by product_key,channel_id,page_id,os
grouping sets( product_key,
              (product_key,channel_id),
			  (product_key,page_id),
			  (product_key,os),
              (product_key,channel_id,page_id),
			  (product_key,channel_id,os),
			  (product_key,page_id,os),
			  (product_key,channel_id,page_id,os)
			 );
			 

-- == rptdata.fact_kesheng_h5_page_visit_event_hourly =======================================

set mapreduce.job.name=rptdata.fact_kesheng_h5_page_visit_event_hourly_${SRC_FILE_DAY}_$(SRC_FILE_HOUR);
 insert overwrite table rptdata.fact_kesheng_h5_page_visit_event_hourly  partition(src_file_day='${SRC_FILE_DAY}',src_file_hour='${SRC_FILE_HOUR}')
 select c.product_key,
        c.channel_id,
	    c.page_id,
	    c.os,
	    sum(c.visit_duration_ms),
        rpad(reverse(bin(cast(grouping__id as int))),4,'0') grain_ind	
   from(select nvl(b.product_key,-998) product_key,  
               a.channel_id channel_id,
		       a.page_id page_id,
		       a.os os,
		       a.visit_duration_ms visit_duration_ms
          from intdata.kesheng_h5_page_visit_event a
          left join mscdata.dim_kesheng_h5_product b on a.product_id = b.product_id
         where a.src_file_day='${SRC_FILE_DAY}' and a.src_file_hour='${SRC_FILE_HOUR}' 
           and a.channel_id<>'-998' and a.page_id<>'-998' and a.os<>'-998'
        )c
   group by product_key,channel_id,page_id,os
grouping sets( product_key,
              (product_key,channel_id),
			  (product_key,page_id),
			  (product_key,os),
              (product_key,channel_id,page_id),
			  (product_key,channel_id,os),
			  (product_key,page_id,os),
			  (product_key,channel_id,page_id,os)
			);
			
			
-- == app.cpa_h5_event_hourly =======================================			
			
set mapreduce.job.name=app.cpa_h5_event_hourly_${SRC_FILE_DAY}__${SRC_FILE_HOUR}; 

insert overwrite table app.cpa_h5_event_hourly partition (src_file_day='${SRC_FILE_DAY}', src_file_hour='${SRC_FILE_HOUR}')
select concat('${SRC_FILE_DAY}','${SRC_FILE_HOUR}') as stat_time
	,if(t1.product_key=-1, '-1', d.product_name) as product_name
	,t1.product_key
	,if(t1.channel_id='-1', '-1', e.chn_name) as channel_name
	,t1.channel_id
	,t1.page_id
	,t1.os
	,t1.uv_num
	,t1.pv_cnt
	,case when t1.uv_num = 0 then 0 else round(t1.visit_duration_ms/(t1.uv_num*1000)) end as avg_visit_duration_sec 
	,t1.download_click_cnt
from( 
	  select if(substr(a1.grain_ind,1,1) = '0', -1, a1.product_key) as product_key
			,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id) as channel_id
			,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.page_id) as page_id 
			,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.os) as os
			,sum(a1.uv_num) as uv_num 
			,sum(a1.pv_cnt) as pv_cnt 
			,sum(a1.visit_duration_ms) as visit_duration_ms 
			,sum(a1.download_click_cnt) as download_click_cnt 
		from( 
			select a.product_key
				  ,a.channel_id
				  ,a.page_id
				  ,a.os
				  ,a.uv_num
				  ,a.pv_cnt
				  ,0 visit_duration_ms
				  ,0 download_click_cnt
				  ,a.grain_ind 
			  from rptdata.fact_kesheng_h5_common_event_hourly a 
			 where a.src_file_day = '${SRC_FILE_DAY}' and a.src_file_hour = '${SRC_FILE_HOUR}' 
			 union all 
			select b.product_key
			      ,b.channel_id
				  ,b.page_id
				  ,b.os
				  ,0 uv_num
				  ,0 pv_cnt
				  ,b.visit_duration_ms
				  ,0 download_click_cnt
				  ,b.grain_ind
			  from rptdata.fact_kesheng_h5_page_visit_event_hourly b 
			 where b.src_file_day = '${SRC_FILE_DAY}' and b.src_file_hour = '${SRC_FILE_HOUR}'
			 union all 
			select c.product_key
				  ,c.channel_id
				  ,c.page_id
				  ,c.os
				  ,0 uv_num
				  ,0 pv_cnt
				  ,0 visit_duration_ms
				  ,c.download_click_cnt
				  ,c.grain_ind 
			  from rptdata.fact_kesheng_h5_download_event_hourly c 
			 where c.src_file_day = '${SRC_FILE_DAY}' and c.src_file_hour = '${SRC_FILE_HOUR}'
			) a1  
		group by if(substr(a1.grain_ind,1,1) = '0', -1, a1.product_key)	
				,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id)
				,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.page_id)
				,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.os)
	)t1
inner join mscdata.dim_kesheng_sdk_product d on t1.product_key = d.product_key
left join rptdata.dim_chn e on t1.channel_id = e.chn_id 
where e.chn_id is not null or t1.channel_id='-1';


-- == app.cpa_h5_event_daily =======================================			
			
set mapreduce.job.name=app.cpa_h5_event_daily_${SRC_FILE_DAY}; 

insert overwrite table app.cpa_h5_event_daily partition (src_file_day='${SRC_FILE_DAY}')
select '${SRC_FILE_DAY}' as stat_time
	,if(t1.product_key=-1, '-1', d.product_name) as product_name 
	,t1.product_key
	,if(t1.channel_id='-1', '-1', e.chn_name) as channel_name 
	,t1.channel_id
	,t1.page_id
	,t1.os
	,t1.uv_num
	,t1.pv_cnt
	,case when t1.uv_num = 0 then 0 else round(t1.visit_duration_ms/(t1.uv_num*1000)) end as avg_visit_duration_sec 
	,t1.download_click_cnt
from(
	select if(substr(a1.grain_ind,1,1) = '0', -1, a1.product_key) as product_key 
		  ,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id) as channel_id
		  ,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.page_id) as page_id
		  ,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.os) as os
	      ,sum(a1.uv_num) as uv_num
	      ,sum(a1.pv_cnt) as pv_cnt
	      ,sum(a1.visit_duration_ms) as visit_duration_ms
	      ,sum(a1.download_click_cnt) as download_click_cnt
	  from(
			select a.product_key --第一层子查询
				  ,a.channel_id
				  ,a.page_id
				  ,a.os
				  ,a.uv_num
				  ,a.pv_cnt
				  ,0 visit_duration_ms
				  ,0 download_click_cnt
				  ,a.grain_ind 
			  from rptdata.fact_kesheng_h5_common_event_daily a 
			 where a.src_file_day = '${SRC_FILE_DAY}' 
			 union all 
			select b.product_key
				  ,b.channel_id
				  ,b.page_id
				  ,b.os
				  ,0 uv_num
				  ,0 pv_cnt
				  ,b.visit_duration_ms
				  ,0 download_click_cnt
				  ,b.grain_ind 
			  from rptdata.fact_kesheng_h5_page_visit_event_hourly b 
			 where b.src_file_day = '${SRC_FILE_DAY}' 
			 union all 
			select c.product_key
				  ,c.channel_id
				  ,c.page_id
				  ,c.os
				  ,0 uv_num
				  ,0 pv_cnt
				  ,0 visit_duration_ms
				  ,c.download_click_cnt
				  ,c.grain_ind  
			  from rptdata.fact_kesheng_h5_download_event_hourly c 
			 where c.src_file_day = '${SRC_FILE_DAY}' 
		  ) a1 
	  group by if(substr(a1.grain_ind,1,1) = '0', -1, a1.product_key)
			  ,if(substr(a1.grain_ind,2,1) = '0', '-1', a1.channel_id)
			  ,if(substr(a1.grain_ind,3,1) = '0', '-1', a1.page_id)
			  ,if(substr(a1.grain_ind,4,1) = '0', '-1', a1.os)
	)t1 
inner join mscdata.dim_kesheng_sdk_product d on t1.product_key = d.product_key 
 left join rptdata.dim_chn e on t1.channel_id = e.chn_id 
where e.chn_id is not null or t1.channel_id='-1';			

