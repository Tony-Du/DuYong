


### 项目版  1.购买电影票 2.报名活动 3.活动参与 4.领取餐饮排号 5.丢弃餐饮排号
select mallid,
       mallname,
	   result,
	   event_name,
	   count(*) 
  from (
        select t1.mallid,
		       t1.mallname,
			   'MovieOrder' event_name,        --购买电影票
               kudu_upsert('hadoop002','medusa_event_warehouse',
                           'id',concat(unix_timestamp(createtime),'-',t1.mallid,'-',concat('uid_', uid),'-',17001),
                           'distinct_id',concat('uid_', uid),
                           'ts',unix_timestamp(createtime),
                           'time',from_unixtime(unix_timestamp(createtime),'yyyy-MM-dd HH:mm:ss'),
                           'hour',concat(from_unixtime(unix_timestamp(createtime),'HH'),':00:00'),
                           'event_name','MovieOrder',
                           'event_type',17001,
                           'event_source',Case OrderSource When 1 then '安卓' when 2 then 'IOS' when 3 then '轻应用' when 5 then '微信' when 6 then 'Portal' else '未知' End,
                           'event_scene','线上',
                           'user_id',uid,
                           'movie_id',MovieID,
                           'movie_name',MovieName,
                           'page_refid',MovieID,
                           'page_refname',MovieName,
                           'order_id',OrderNo,
                           'order_total_price',COALESCE(Price,0.0),
                           'mobile',Mobile,
                           'mallid',t1.mallid,
                           'mallname',t1.mallname,
                           'day',date_parse(createtime,2)) result
            from medusa.ProjectToMall t1 
			join (select *
                    from online.MovieOrder
                   where status =1 
				     and createtime is not null  
					 and date_parse(createtime,0) between ${insert_condition}
				 ) t2 
			  on (t1.mallid=t2.mallid)
        ) m 
  group by mallid,mallname,result,event_name
		
union all

select mallid,
       mallname,
	   result,
	   event_name,
	   count(*) 
  from (
        select
            t1.mallid,
			t1.mallname,
			'ActivitySign' event_name,     --报名活动
            kudu_upsert('hadoop002','medusa_event_warehouse',
                'id',concat(unix_timestamp(createtime),'-',t1.mallid,'-',concat('uid_', uid),'-',15003),
                'distinct_id',concat('uid_', uid),
                'ts',unix_timestamp(createtime),
                'time',from_unixtime(unix_timestamp(createtime),'yyyy-MM-dd HH:mm:ss'),
                'hour',concat(from_unixtime(unix_timestamp(createtime),'HH'),':00:00'),
                'event_name','ActivitySign',
                'event_type',15003,
                'event_source','未知',
                'event_scene','线上',
                'activity_id',promotionid,
                'activity_name',promotionname,
                'page_refid',promotionid,
                'page_refname',promotionname,
                'user_id',uid,
                'mobile',Mobile,
                'mallid',t1.mallid,
                'mallname',t1.mallname,
                'day',date_parse(createtime,2)) result
            from medusa.ProjectToMall t1 
			join (select *
                    from online.PPInfo
                   where ispromotion =0 
				     and ismallactivity =true  
					 and date_parse(createtime,0) between ${insert_condition}
			     ) t2 
			  on (t1.mallid=t2.mallid)
       ) m group by mallid,mallname,result,event_name
	   
union all

select mallid,
       mallname,
	   result,
	   event_name,
	   count(*) 
  from (
        select
            t1.mallid,
			t1.mallname,
			'ActivityParticipate' event_name,  --活动参与
            kudu_upsert('hadoop002','medusa_event_warehouse',
                'id',concat(unix_timestamp(usedtime),'-',t1.mallid,'-',concat('uid_', uid),'-',15004),
                'distinct_id',concat('uid_', uid),
                'ts',unix_timestamp(usedtime),
                'time',from_unixtime(unix_timestamp(usedtime),'yyyy-MM-dd HH:mm:ss'),
                'hour',concat(from_unixtime(unix_timestamp(usedtime),'HH'),':00:00'),
                'event_name','ActivityParticipate',
                'event_type',15004,
                'event_source','未知',
                'event_scene','线上',
                'activity_id',promotionid,
                'activity_name',promotionname,
                'page_refid',promotionid,
                'page_refname',promotionname,
                'user_id',uid,
                'mobile',Mobile,
                'mallid',t1.mallid,
                'mallname',t1.mallname,
                'day',date_parse(usedtime,2)) result
            from medusa.ProjectToMall t1 
			join (select *
                  from online.PPInfo
                  where ispromotion =0 
				    and ismallactivity =true  
					and date_parse(usedtime,0) between ${insert_condition}
			     ) t2 
			  on (t1.mallid=t2.mallid)
       ) m 
 group by mallid,mallname,result,event_name

union all

select mallid,
       mallname,
	   result,
	   event_name,
	   count(*) 
  from (
        select
            t1.mallid,
			t1.mallname,
			'TicketGet' event_name,   --领取餐饮排号
            kudu_upsert('hadoop002','medusa_event_warehouse',
                'id',concat(unix_timestamp(TicketDate),'-',t1.mallid,'-',concat('uid_', uid),'-',19001),
                'distinct_id',concat('uid_', uid),
                'ts',unix_timestamp(TicketDate),
                'time',from_unixtime(unix_timestamp(TicketDate),'yyyy-MM-dd HH:mm:ss'),
                'hour',concat(from_unixtime(unix_timestamp(TicketDate),'HH'),':00:00'),
                'event_name','TicketGet',
                'event_type',19001,
                'event_source','未知',
                'event_scene','线上',
                'shopname',shopname,
                'brand',brand,
                'duration',if(unix_timestamp(callingtime)-unix_timestamp(ticketdate)>0 ,round((unix_timestamp(callingtime)-unix_timestamp(ticketdate))/60,4),0),
                'user_id',uid,
                'mobile',Mobile,
                'mallid',t1.mallid,
                'mallname',t1.mallname,
                'day',date_parse(TicketDate,2)) result
            from medusa.ProjectToMall t1 
			join (select *
                    from online.ticket
                   where ticketstatus !=3  
				     and date_parse(TicketDate,0) between ${insert_condition}
				 ) t2 
			 on (t1.mallid=t2.mallid)
       ) m 
 group by mallid,mallname,result,event_name

union all

select mallid,
       mallname,
	   result,
	   event_name,
	   count(*) 
  from (
        select
            t1.mallid,
			t1.mallname,
			'TicketLost' event_name,    --丢弃餐饮排号
            kudu_upsert('hadoop002','medusa_event_warehouse',
                'id',concat(unix_timestamp(TicketDate),'-',t1.mallid,'-',concat('uid_', uid),'-',19002),
                'distinct_id',concat('uid_', uid),
                'ts',unix_timestamp(TicketDate),
                'time',from_unixtime(unix_timestamp(TicketDate),'yyyy-MM-dd HH:mm:ss'),
                'hour',concat(from_unixtime(unix_timestamp(TicketDate),'HH'),':00:00'),
                'event_name','TicketLost',
                'event_type',19002,
                'event_source','未知',
                'event_scene','线上',
                'shopname',shopname,
                'brand',brand,
                'user_id',uid,
                'mobile',Mobile,
                'mallid',t1.mallid,
                'mallname',t1.mallname,
                'day',date_parse(TicketDate,2)) result
            from medusa.ProjectToMall t1 
			join (select *
                    from online.ticket
                   where ticketstatus not in (2,3)  
				     and date_parse(TicketDate,0) between ${insert_condition}
				 ) t2 
			  on (t1.mallid=t2.mallid)
       ) m 
 group by mallid,mallname,result,event_name;





#### 6.兑换礼品 7.领取礼品 8. 扫描固定带参二维码
select mallid,
       mallname,
	   result,
	   event_name,
	   count(*) 
  from (
        select
            t1.mallid,
			t1.mallname,
			'GiftExchange' event_name,    --兑换礼品
            kudu_upsert('hadoop002','medusa_event_warehouse',
                'id',concat(unix_timestamp(ReceivedTime),'-',t1.mallid,'-',concat('uid_', uid),'-',15005),
                'distinct_id',concat('uid_', uid),
                'ts',unix_timestamp(ReceivedTime),
                'time',from_unixtime(unix_timestamp(ReceivedTime),'yyyy-MM-dd HH:mm:ss'),
                'hour',concat(from_unixtime(unix_timestamp(ReceivedTime),'HH'),':00:00'),
                'event_name','GiftExchange',
                'event_type',15005,
                'event_source',Case DataSource When 1 then '安卓' when 2 then 'IOS' when 3 then '轻应用' when 5 then '微信' when 6 then 'Portal' else '未知' End,
                'event_scene','线上',
                'user_id',uid,
                'gift_id',GiftID,
                'gift_name',GiftTitle,
                'bonus',point,
                'mobile',Mobile,
                'mallid',t1.mallid,
                'mallname',t1.mallname,
                'day',date_parse(ReceivedTime,2)) result
            from medusa.ProjectToMall t1 
			join (select *
                    from online.GiftCoupon
                   where ReceivedTime is not null  
				     and date_parse(ReceivedTime,0) between ${insert_condition}
			     ) t2 
			  on (t1.mallid=t2.mallid)
       ) m 
 group by mallid,mallname,result,event_name

union all

select mallid,
       mallname,
	   result,
	   event_name,
	   count(*) 
  from (
        select
            t1.mallid,
			t1.mallname,
			'GiftReceive' event_name,   --领取礼品
            kudu_upsert('hadoop002','medusa_event_warehouse',
                'id',concat(unix_timestamp(ReceivedTime),'-',t1.mallid,'-',concat('uid_', uid),'-',15006),
                'distinct_id',concat('uid_', uid),
                'ts',unix_timestamp(ReceivedTime),
                'time',from_unixtime(unix_timestamp(ReceivedTime),'yyyy-MM-dd HH:mm:ss'),
                'hour',concat(from_unixtime(unix_timestamp(ReceivedTime),'HH'),':00:00'),
                'event_name','GiftReceive',
                'event_type',15006,
                'event_source',Case DataSource When 1 then '安卓' 
				                               when 2 then 'IOS' 
											   when 3 then '轻应用' 
											   when 5 then '微信' 
											   when 6 then 'Portal' 
											   else '未知' End,
                'event_scene','线上',
                'user_id',uid,
                'gift_id',GiftID,
                'gift_name',GiftTitle,
                'bonus',point,
                'mobile',Mobile,
                'mallid',t1.mallid,
                'mallname',t1.mallname,
                'day',date_parse(ReceivedTime,2)) result
            from medusa.ProjectToMall t1 
			join (select *
                    from online.GiftCoupon
                   where ReceivedTime is not null  
				     and date_parse(ReceivedTime,0) between ${insert_condition}
				 ) t2 
			  on (t1.mallid=t2.mallid)
       ) m 
 group by mallid,mallname,result,event_name
 
union all

select mallid,
       mallname,
	   result,
	   event_name,
	   count(*) 
  from (
        select
            t1.mallid,
			t1.mallname,
			'ScanPQR' event_name,  --扫描固定带参二维码
            kudu_upsert('hadoop002','medusa_event_warehouse',
                'id',concat(unix_timestamp(createtime),'-',t1.mallid,'-',concat('openid_',openid),'-',24001),
                'distinct_id',concat('openid_', openid),
                'ts',unix_timestamp(createtime),
                'time',from_unixtime(unix_timestamp(createtime),'yyyy-MM-dd HH:mm:ss'),
                'hour',concat(from_unixtime(unix_timestamp(createtime),'HH'),':00:00'),
                'event_name','ScanPQR',
                'event_type',24001,
                'event_source','微信',
                'event_scene','线上',
                'user_id',uid,
                'openid',openid,
                'is_new',isfirsttime,
                'pqr_scene',scene,
                'pqr_type',Case businesstype When 1 then '停车场' when 2 then '场内WiFi' when 3 then '场内海报' when 4 then '商户店铺' when 5 then '其他' when 6 then '地推人员' else '未知' End,
                'pqr_location',location,
                'mallid',t1.mallid,
                'mallname',t1.mallname,
                'day',date_parse(createtime,2)) result
            from medusa.ProjectToMall t1 
			join (select *
                    from pqr.scanrecord
                   where createtime is not null  
				     and date between ${insert_condition}
				 ) t2 
			  on (t1.mallid=t2.mallid)
        ) m 
  group by mallid,mallname,result,event_name;

