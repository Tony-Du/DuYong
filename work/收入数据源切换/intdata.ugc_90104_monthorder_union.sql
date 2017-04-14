

-- intdata.ugc_90104_monthorder_union -------------------------------------------------------------------------

add jar hdfs:/user/hadoop/ods/udfjar/json-serde.jar;
set hive.exec.parallel=true; 
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
alter table intdata.ugc_90104_MonthOrder_union add if not exists partition (src_file_day='${date}');
insert overwrite table intdata.ugc_90104_MonthOrder_union  partition (src_file_day='${date}')
select
   serv_number,
   product_id,
   first_order_time,
   last_order_time,
   last_cancel_time,
   order_status,
   last_bill_time,
   next_bill_time,
   chn_id_source,
   substatus,
   oprlogin,
   order_expier_date,
   user_id,
   usenum_type,
   imei 
from ods.aaa_10104_order_ex where src_file_day='${date}' and product_id not in ('2028599910','2028599925')
  -- 2028599925	MIGU_PMS	咪咕视频-阿里联合会员30元包
  -- 2028599910	MIGU_PMS	咪咕视频-阿里联合会员15元包
  -- ods.aaa_10104_order_ex	：AAA-10104 用户订购信息：提供用户在AAA平台的全量订购关系信息
union all

select 
	userNum as serv_number,
	PRODUCT_ID,
	FIRST_ORDER_TIME,
	LAST_ORDER_TIME,
	LAST_CANCEL_TIME,
	ORDER_STATUS,
	LAST_BILL_TIME,
	NEXT_BILL_TIME,
	CHN_ID_SOURCE,
	SUBSTATUS,
	OPRLOGIN,
	order_expire_date as order_expier_date,
	userid as user_id,
	user_type as usenum_type,
	imei
from intdata.ugc_90104_MonthOrder 
where src_file_day='${date}';

--------------------------------------------------------------------------------------

add jar hdfs:/user/hadoop/ods/udfjar/json-serde.jar;
set hive.exec.parallel=true; 
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.exec.reducers.max=10;
set hive.auto.convert.join=false;

alter table intdata.ugc_90104_MonthOrder add if not exists partition (src_file_day='${tomorrow}');

insert overwrite table intdata.ugc_90104_MonthOrder  partition (src_file_day='${tomorrow}')
select
	if(d.usernum is not null,d.usernum,h.accountname),
	if(d.product_id is not null,d.product_id,h.productid),
	if(d.first_order_time is not null,d.first_order_time,h.createtime),
	if(h.orderstatus='5',h.lastupdatetime,d.last_order_time),
	if(h.orderstatus in ('14','16'),h.lastupdatetime,d.last_cancel_time),
	case h.orderstatus
		when '5' then '1'
		when '16' then '4'	--4：已支付
		when '14' then '4'
		else d.order_status end,
	'',
	'',
	if(d.chn_id_source is not null,d.chn_id_source,h.channelid),
	'',
	if(h.operatoruserid is not null,h.operatoruserid,d.oprlogin),
	case when d.order_expire_date = '20381231235959' then d.order_expire_date 
		 when d.order_expire_date!='20381231235959' and h.endtime is not null then h.endtime 
		 else d.order_expire_date end,
	nvl(d.userid,h.orderuserid),
	'02',
	''
from (
	  select * 
		from intdata.ugc_90104_MonthOrder  
	   WHERE src_file_day='${today}'
	  ) d
	  
full join 

	 (select * from 
	 			(select
	 				  c.accountname,
	 				  c.productid,
	 				  c.createtime,
	 				  c.orderstatus,
	 				  c.channelid,
	 				  c.operatoruserid,
	 				  c.orderuserid,
	 				  c.endtime,
	 				  c.lastupdatetime,
	 				  row_number() over(partition by productid,accountname order by createtime desc) rn2
	 			  from(
	 					select 
	 						 nvl(a.orderitems[0].data.usernum,b.user_num) as accountname,
	 						 f.productcode as productid,
	 						 a.createtime as createtime,
	 						 a.orderstatus as orderstatus,
	 						 a.channelid as channelid,
	 						 a.extinfo.operatoruserid as operatoruserid,
	 						 a.orderuserid as orderuserid,
	 						 a.lastupdatetime as lastupdatetime,
	 						 if(a.deliveryresults[0].resultdata is not null, get_json_object(deliveryresults[0].resultdata,'$.endTime'), '') as endTime
	 					from (select * 
	 							from ods.ugc_50201_order_ex 
	 						   where orderstatus in ('5','14','16') and src_file_day='${tomorrow}'	-- 5：订单完成状态 14：已退款，退履约成功  16：退履约成功
	 						 ) a
	 					left outer join (select * 
	 									   from ods.aaa_10112_userid_usernum_ex t1 
	 									  where t1.src_file_day= '${tomorrow}'
	 									 ) b 
	 								 on a.orderuserid = b.user_id
	 					left outer join (select 
	 										   t2.service.code as program,
	 										   t2.service.payment[0].charge.cpcode,
	 										   t2.service.payment[0].charge.opercode,
	 										   t2.service.payment[0].charge.productcode
	 									  from (select * 
	 									  		from ods.ugc_10106_goodsinfo_raw_ex t 
	 									  	   where t.src_file_day='${tomorrow}'
	 									  	   ) t1
	 								      LATERAL VIEW explode(service) t2 as service
	 									 ) f
	 								 on a.serviceCode = f.program
	 							   join (select * 
	 							  		   from rptdata.dim_productid_productname 
	 							  	      where chrg_type_id = '0' and chrgprod_id in ('2028595246','2028599910','2028599925','2028600184','2028597820','2028595240','2028598635')
	 								      -- 2028599910	 MIGU_PMS	咪咕视频-阿里联合会员15元包
	 								      -- 2028599925	 MIGU_PMS	咪咕视频-阿里联合会员30元包
	 							  	    ) e 
	 							     on f.productcode = e.chrgprod_id
	 					) c
	 				) t 
	 where t.rn2='1'
	 ) h 
on d.product_id = h.productid and d.usernum = h.accountname;

