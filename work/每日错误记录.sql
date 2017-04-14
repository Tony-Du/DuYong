2017-04-10	Q(1):select 
                     a.phone_number as serv_number,
                     a.create_time as bill_time,
                     '-998' as broadcast_type,
                     '-998' as bill_fee,
                     '-998' as use_dur,
                     a.cp_id as cp_id,
                     a.goods_id as product_id,
                     '-998' as program_id,
                     '-998' as program_father_id,
                     '-998' as content_id,
                     a.sub_busi_id as sub_busi_id,
                     '-998' as net_type_id,
                     a.user_type as user_type_id,
                     a.chn_id as chn_id,
                     a.new_version_id as term_version_id,
                     '-998' as plat_id,
                     a.term_prod_id as term_prod_id,
                     a.company_id as company_id,
                     a.city_id as city_id
                  from intdata.ugc_order_daily a 
                  join intdata.ugc_order_item_daily b
                    on a.order_id = b.order_id and b.src_file_day='20170301'
                  join intdata.ugc_order_paychannel_daily c
                    on a.order_id = c.order_id and c.src_file_day='20170301'
                  join intdata.ugc_payment_daily d
                    on a.payment_id = d.payment_id and d.src_file_day='20170301'
                  join (
                        select 
                        t4.service.code as program,
                        t4.service.payment[0].charge.cpcode,
                        t4.service.payment[0].charge.opercode,
                        t4.service.payment[0].charge.productcode
                        from (select * from ods.ugc_10106_goodsinfo_raw_ex t1 where t1.src_file_day ='20170301') t3
                        LATERAL VIEW explode(service) t4 as service
                       ) j
                     on a.service_code = j.program
                   join rptdata.dim_productid_productname k
                     on k.chrgprod_id = j.productcode and k.chrg_type_id='1'
                  where a.src_file_day='20170301' 
                    and a.resource_type='POMS_PROGRAM_ID'
                    and b.period_unit = 'HOUR';
			    
              FAILED: RuntimeException MetaException(message:java.lang.ClassNotFoundException Class org.openx.data.jsonserde.JsonSerDe not found)
			  
			S: 代码前先运行 add jar hdfs:/user/hadoop/ods/udfjar/json-serde.jar;
			
			Q(2):   Task failed!
					Task ID:
					  Stage-16
					Logs:
					/tmp/hadoop/hive.log
					FAILED: Execution Error, return code 2 from org.apache.hadoop.hive.ql.exec.mr.MapredLocalTask
					
			S: set hive.auto.convert.join=false;
			M: hive.auto.convert.join 
				是否根据输入小表的大小，自动将 Reduce 端的 Common Join 转化为 Map Join，从而加快大表关联小表的 Join 速度。 
				默认值：false
			
			
			
			
			