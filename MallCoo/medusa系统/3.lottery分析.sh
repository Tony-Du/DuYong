$1='into'
$2="2017-10-10 and 2017-10-10"


if  [[ -n "$1" ]] && [[ -n "$2" ]]  ;then
    insert_action=$1
    insert_condition=${2//-/}       

-- shell 变量替换，使用方法如下
--${value//pattern/string}
--进行变量内容的替换,把与pattern匹配的部分替换为string的内容


##### 1.参与游戏 ,2.游戏中奖######
select mallid,
       mallname,
	   result,
	   event_name,
	   count(*) 
  from (
        select t1.mallid,
        	   t1.mallname,
        	   'GameLottery' event_name,
               kudu_upsert('hadoop002','medusa_event_warehouse',
                           'id',concat(unix_timestamp(createtime),'-',t1.mallid,'-',concat('uid_', userid),'-',15001),
                           'distinct_id',concat('uid_', userid),
                           'ts',unix_timestamp(createtime),
                           'time',from_unixtime(unix_timestamp(createtime),'yyyy-MM-dd HH:mm:ss'),
                           'hour',concat(from_unixtime(unix_timestamp(createtime),'HH'),':00:00'),
                           'event_name','GameLottery',
                           'event_type',15001,
                           'event_source',Case DataSource When 1 then '安卓' 
						                                  when 2 then 'IOS' 
														  when 3 then '轻应用' 
														  when 5 then '微信' 
														  when 6 then 'Portal' 
														  else '未知' End,
                           'event_scene','线上',
                           'lottery_type',Case LotteryType When 4 then '大转盘' 
						                                   When 5 then '刮刮乐' 
														   When 6 then '老虎机' 
														   when 9 then '摇一摇' 
														   else '未知' End,
                           'lottery_id',BIID,
                           'lottery_name',BIName,
                           'lottery_award_type',Case AwardType When 0 then '未中奖' 
						                                       when 2 then '奖品券' 
															   when 3 then '积分' 
															   else '未知' End,
                           'lottery_award_id',ASID,
                           'lottery_award_name',ASName,
                           'mobile',Mobile,
                           'mallid',t1.mallid,
                           'mallname',t1.mallname,
                           'day',date_parse(createtime,2)) result
            from medusa.ProjectToMall t1 
			join (select *
                    from online.LotteryRecord
                   where createtime is not null  
				     and date_parse(createtime,0) between $insert_condition
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
        select t1.mallid,t1.mallname,'GameLotteryAward' event_name,
               kudu_upsert('hadoop002','medusa_event_warehouse',
                           'id',concat(unix_timestamp(createtime),'-',t1.mallid,'-',concat('uid_', userid),'-',15002),
                           'distinct_id',concat('uid_', userid),
                           'ts',unix_timestamp(createtime),
                           'time',from_unixtime(unix_timestamp(createtime),'yyyy-MM-dd HH:mm:ss'),
                           'hour',concat(from_unixtime(unix_timestamp(createtime),'HH'),':00:00'),
                           'event_name','GameLotteryAward',
                           'event_type',15002,
                           'event_source','未知',
                           'event_scene','线上',
                           'lottery_id',BIID,
                           'lottery_name',BIName,
                           'lottery_award_id',ASID,
                           'lottery_award_name',ASName,
                           'mobile',Mobile,
                           'membercard_no',CardNo,
                           'mallid',t1.mallid,
                           'mallname',t1.mallname,
                           'day',date_parse(createtime,2)) result
            from medusa.ProjectToMall t1 
			join (select *
                    from online.AwardCoupon
                   where createtime is not null 
				     and date_parse(createtime,0) between $insert_condition
				 ) t2 
			  on (t1.mallid=t2.mallid)
       ) m 
 group by mallid,mallname,result,event_name;
 
===================================================== 
 
 medusa.ProjectToMall  | ---> kudu
 online.AwardCoupon    |
                       |
 medusa.ProjectToMall  |
 online.LotteryRecord  |