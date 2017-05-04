set mapreduce.job.name=rptdata.fact_ugc_order_relation_amount_daily${SRC_FILE_DAY};

set hive.groupby.skewindata=true;
set hive.optimize.skewjoin=true;
set hive.merge.mapredfiles = true;
insert overwrite table rptdata.fact_ugc_order_relation_amount_daily partition (src_file_day='${SRC_FILE_DAY}')
select  
    user_type_id,
   	net_type_id,
   	cp_id,
   	broadcast_type_id,
   	term_prod_id,
   	term_version_id,
   	chn_id,
   	city_id,
   	sub_busi_id,
   	order_status,
   	sub_status,
   	last_order_time,
   	company_id,
   	SUM(chrgprod_price)		--计费产品价格
from rptdata.fact_ugc_order_relation_detail_daily
where src_file_day='${SRC_FILE_DAY}'
group by user_type_id, net_type_id, cp_id, broadcast_type_id, term_prod_id, term_version_id, chn_id, city_id, sub_busi_id,
		order_status, sub_status, last_order_time, company_id;

-------------------------------------------------------------------------------

set mapreduce.job.name=rptdata.fact_ugc_order_relation_detail_daily${SRC_FILE_DAY};

set hive.groupby.skewindata=true;
set hive.optimize.skewjoin=true;
set hive.merge.mapredfiles = true;
insert overwrite table rptdata.fact_ugc_order_relation_detail_daily partition (src_file_day='${SRC_FILE_DAY}')
select 
	aa.user_type_id,
   	aa.net_type_id,
   	aa.cp_id,
   	aa.broadcast_type_id,
   	aa.term_prod_id,
   	aa.term_version_id,
   	aa.chn_id,
   	aa.city_id,
   	aa.sub_busi_id,
   	aa.user_id,
   	aa.order_status,
   	aa.sub_status,
   	aa.last_order_time,
   	aa.company_id,
   	SUM(aa.chrgprod_price)
from (
	select
		bb.user_type_id,
		bb.net_type_id,
		bb.cp_id,
		bb.broadcast_type_id,
		bb.term_prod_id,
		bb.term_version_id,
		bb.chn_id,
		bb.city_id,
		bb.sub_busi_id,
		bb.user_id,
		bb.order_status,
		bb.sub_status,
		bb.last_order_time,
		bb.company_id,
		bb.chrgprod_price
	from (
		 select 
		 	  '1' as user_type_id,	--默认1，为注册用户
		 	  '-998' as net_type_id,
		 	  '-998' as cp_id,
		 	  '-998' as broadcast_type_id,
		 	  nvl(nvl(if((i.chn_id2 is not null and i.is_wap=1), i.term_prod_id, NULL), f.term_prod_id), '-998') as term_prod_id,
		 	  a.new_version_id as term_version_id,
		 	  nvl(a.chn_id, '-998') as chn_id,
		 	  nvl(g.region_id, '-998') as city_id,
		 	  nvl(nvl(c.sub_busi_id, d.sub_busi_id), '-998') as sub_busi_id,--nvl(b.sub_busi_bdid, '-998') as sub_busi_id,
		 	  if(a.user_num is not null, if(length(a.user_num) = 13 and substr(a.user_num, 1, 2) = '86', substr(a.user_num, 3, 11), a.user_num), '-998') as user_id,--nvl(a.user_id, '-998') as user_id,
		 	  a.order_status as order_status,
		 	  a.sub_status as sub_status,
		 	  if(a.last_order_time is not null, substr(a.last_order_time, 1, 4), '-998') as last_order_time,
		 	  nvl(h.comp_id, '-998') as company_id,
		 	  nvl(cast(b.chrgprod_price as double), 0) as chrgprod_price	--计费产品价格
		 from intdata.ugc_order_relation a
		 left join rptdata.dim_charge_product b		-- 收费产品维表，收费产品id关联收费产品价格
		 on (case when nvl(a.product_id, '-998') = '-998' then concat('',rand()) else a.product_id end) = b.chrgprod_id
		 left join rptdata.dim_product_busi c		-- 产品业务维表，产品id关联子业务id 
		 on (case when nvl(a.product_id, '-998') = '-998' then concat('',rand()) else a.product_id end) = c.product_id --and c.dw_delete_flag = 'N'
		 left join rptdata.dim_product_busi_new d	-- 新产品业务维表，产品id关联子业务id
		 on (case when nvl(a.product_id, '-998') = '-998' then concat('',rand()) else a.product_id end) = d.new_product_id
		 left outer join rptdata.dim_term_prod f	-- 终端产品维表，终端产品版本id关联终端产品id 
		 on a.new_version_id = f.term_version_id and a.new_version_id <> '-998'
		 left join rptdata.dim_phone_belong g		-- 移动手机号段表，不同的手机段号代表不同的城市
		 on (if(a.user_num is not null, if(substr(a.user_num, 1, 2)='86', substr(a.user_num, 3, 7), substr(a.user_num, 1, 7)), concat('', rand()))) = g.phone_range
		 left outer join rptdata.dim_comp_busi h	-- 合作公司业务维表，子业务id关联公司id
		 on b.sub_busi_bdid = h.sub_busi_id
		 left outer join rptdata.dim_chn_id i		-- ，渠道id关联终端产品id 
		 on a.chn_busi_type	= i.chn_id2   ---临时chn_busi_type为platID
		 where a.src_file_day='${SRC_FILE_DAY}'
		) bb
		group by bb.user_type_id, bb.net_type_id, bb.cp_id, bb.broadcast_type_id, bb.term_prod_id, bb.term_version_id, 
			bb.chn_id, bb.city_id, bb.sub_busi_id, bb.user_id, bb.order_status, bb.sub_status, bb.last_order_time, bb.company_id, bb.chrgprod_price
) aa
group by aa.user_type_id, aa.net_type_id, aa.cp_id, aa.broadcast_type_id, aa.term_prod_id, aa.term_version_id, aa.chn_id, aa.city_id, aa.sub_busi_id,
		aa.user_id, aa.order_status, aa.sub_status, aa.last_order_time, aa.company_id;

-------------------------------------------------------------------------------------------

set mapreduce.job.name=intdata.ugc_order_relation${SRC_FILE_DAY};

set hive.groupby.skewindata=true;
set hive.optimize.skewjoin=true;
set hive.merge.mapredfiles = true;

insert overwrite table intdata.ugc_order_relation partition (src_file_day='${SRC_FILE_DAY}')
select
	bb.user_num,			  --
	bb.product_id,			  --产品ID
    bb.first_order_time,      --第一次订购时间
    bb.last_order_time,       --最近一次订购时间
    bb.last_cancel_time,      --最近一次取消时间
    bb.order_status,          --订购状态
    bb.last_bill_time,        --最近一次收费时间
    bb.next_bill_time,        --下一次收费时间
    bb.chn_id_source,         --访问渠道ID
    bb.sub_status,            --订购子状态
    bb.opr_login,             --操作员
    bb.order_expire_date,     --包月订购授权到期时间
    bb.user_id,               --用户ID
    bb.user_type,             --UserNum类型
    bb.imei,                  --加密IMEI
	nvl(bb.chn_values[0], '-998') as version_id,
	nvl(bb.chn_values[2], '-998') as chn_id,
	nvl(bb.chn_values[3], '-998') as chn_busi_type, ---临时填入platID
	substr(bb.chn_values[0], 1, 6) as new_version_id
from (
	select
		a.user_num as user_num,
		a.product_id as product_id,
	    a.first_order_time as first_order_time,
	    a.last_order_time as last_order_time,
	    a.last_cancel_time as last_cancel_time,
	    a.order_status as order_status,
	    a.last_bill_time as last_bill_time,
	    a.next_bill_time as next_bill_time,
	    a.chn_id_source chn_id_source,
	    a.sub_status as sub_status,
	    a.opr_login as opr_login,
	    a.order_expire_date as order_expire_date,
	    a.user_id as user_id,
	    a.user_type as user_type,
	    a.imei as imei,
		split(--version-chn99000-chn_id
			case when chn_source_mask = '0-00000000000' then concat_ws('_', '-998', '-998', substr(a.chn_id_source, 3), concat('000', substr(a.chn_id_source, 1, 1)))
			     when chn_source_mask = '0000-00000000000' then concat_ws('_', '-998', '-998', substr(a.chn_id_source, 6), substr(a.chn_id_source, 1, 4))
			     when chn_source_mask = '0000-000000000000000' then concat_ws('_', '-998', '-998', substr(a.chn_id_source, 6), substr(a.chn_id_source, 1, 4))
			     when chn_source_mask = '0000-00000000-00000-000000000000000' then concat_ws('_', substr(a.chn_id_source, 6, 8), substr(a.chn_id_source, 15, 5), substr(a.chn_id_source, 21), substr(a.chn_id_source, 1, 4))
			     when chn_source_mask in ('00000000000', '000000000000000') then concat_ws('_', '-998', '-998', a.chn_id_source, '-998')
			     when chn_source_mask = '00000000-00000-000000000000000' then concat_ws('_', substr(a.chn_id_source, 1, 8), substr(a.chn_id_source, 10, 5), substr(a.chn_id_source, 16), '-998')
			     when chn_source_mask like '0-00000000000-%' then concat_ws('_', '-998', '-998', substr(a.chn_id_source, 3, 11), concat('000', substr(a.chn_id_source, 1, 1)))
			     when chn_source_mask like '0000-00000000000-%' then concat_ws('_', '-998', '-998', substr(a.chn_id_source, 6, 11), substr(a.chn_id_source, 1, 4))    
			      else concat_ws('_', '-998', '-998', '-998', '-998')
			end, '_') as chn_values
	from (
		select 
		   t1.usernum as user_num,
		   t1.product_id,
		   t1.first_order_time,
		   t1.last_order_time,
		   t1.last_cancel_time,
		   t1.order_status,
		   t1.last_bill_time,
		   t1.next_bill_time,
		   nvl(t2.dest_chn_id, t1.chn_id_source) as chn_id_source,
		   t1.substatus as sub_status,
		   t1.oprlogin as opr_login,
		   t1.order_expire_date,
		   t1.userid as user_id,
		   t1.user_type,
		   t1.imei,
		   regexp_replace(translate(nvl(t2.dest_chn_id, t1.chn_id_source), '1234567899', '000000000'), '_', '-') as chn_source_mask
		from intdata.ugc_90104_monthorder_union t1
		left join rptdata.dim_chn_adjust_cfg t2
		on t1.chn_id_source = t2.src_chn_id
		where src_file_day='${SRC_FILE_DAY}'
	) a
) bb;

-- intdata.ugc_90104_monthorder_union -------------------------------------------------------------------------

add jar hdfs:/user/hadoop/ods/udfjar/json-serde.jar;
set hive.exec.parallel=true; 
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
alter table intdata.ugc_90104_MonthOrder_union add if not exists partition (src_file_day='${date}');
insert overwrite table intdata.ugc_90104_MonthOrder_union  partition (src_file_day='${date}')
select
   serv_number,        --手机号码或非移动用户的userNum
   product_id,         --产品ID
   first_order_time,   --第一次订购时间
   last_order_time,    --最近一次订购时间
   last_cancel_time,   --最近一次取消时间
   order_status,       --订购状态
   last_bill_time,     --最近一次收费时间
   next_bill_time,     --下一次收费时间
   chn_id_source,      --访问渠道ID
   substatus,          --订购子状态
   oprlogin,           --操作员
   order_expier_date,  --包月订购授权到期时间
   user_id,            --用户ID
   usenum_type,        --UserNum类型
   imei                --加密IMEI
from ods.aaa_10104_order_ex where src_file_day='${date}' and product_id not in ('2028599910','2028599925')	--3A和用户中心都出了这2个产品的数据,3A出的不准
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
	 							  	    ) e 		-- chrg_type_id = '0' 包月
	 							     on f.productcode = e.chrgprod_id
	 					) c
	 				) t 
	 where t.rn2='1'
	 ) h 
on d.product_id = h.productid and d.usernum = h.accountname;

--明天的存量数据 = 今天的存量数据 +(叠加) 明天的新增数据
--使用join的目的：同一个用户明天的订单状态(如:退订) 覆盖 明天以前的订单状态(如:订购) 
-------------------------------------------------------------------

desc rptdata.dim_productid_productname;
OK
chrgprod_id         	string          --计费产品ID  	                    
chrgprod_name       	string          --计费产品名称	                    
chrg_type_id        	string   		--计费产品类型 ：0包月,1按次,7免费,13包年,14包多天,15大包月,16平台包月产品

--------------------------------------------------------------------------------------------------------------------------------

ods.ugc_50201_order_ex			-- 订单组件在订单状态为最终状态时，订单组件出话单 	hdfs://ns1/user/hadoop/ods/huawei/ugc/50201
ods.aaa_10112_userid_usernum_ex	-- UserID-UserNum对应表		hdfs://ns1/user/hadoop/ods/huawei/aaa/aaa-10112
ods.ugc_10106_goodsinfo_raw_ex	-- 销售系统全量商品信息		hdfs://ns1/user/hadoop/ods/huawei/ugc/ugc-10106

ods.aaa_10104_order_ex			-- AAA-10104 用户订购信息：提供用户在AAA平台的全量订购关系信息		hdfs://ns1/user/hadoop/ods/huawei/aaa/aaa-10104

ods.ugc_50201_order_ex			|-> 该两表是自己创建的jsonserde：ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' 
ods.ugc_10106_goodsinfo_raw_ex	|


ods.ugc_50201_order_json_ex   |-> 两个不同的外部表使用不同的jsonserde指向同一个数据源: hdfs://ns1/user/hadoop/ods/huawei/ugc/50201
ods.ugc_50201_order_ex	      |


