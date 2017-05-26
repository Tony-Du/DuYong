2017-04-10	Q(1): select 
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
				（原因未知）
				
			
2017-04-19	Q: BDI:外部命令参数执行异常（删除旧记录，重新启用调度时间，流程无法被调起来）
			S: 关闭调度时间，导出流程，删除旧流程，再导入流程（被导出的那个），启用调度时间即可
			
			
2017-04-20	Q(1): CPA2期，app层，分组排序出现错误
			   row_number()over(partition by a.event_name, a.param_name, a.param_val order by a.val_cnt desc) param_val_rank
			S: row_number()over(partition by a.event_name, a.param_name order by a.val_cnt desc) param_val_rank
			   (test sql)select t.event_name, t.param_name, count(distinct t.param_val) 
						   from app.cpa_event_params_last30_daily t 
						  where t.src_file_day = '20170419' and t.event_name='tokenvalidate'
						  group by t.event_name, t.param_name;
			
			Q(2): CPA2期，app层，如果出现 event_name 和 param_name 相同的情况，用java提取出来的数据，event那部分输出会被param部分覆盖
			S：原因是java是通过key-value对放到以个总的map集合中，所以当key出现相同时，value会被覆盖
			   重新定义接口
			   

2017-04-21	Q: param_name=timestamp时，param_val=2017-04-19 09， 而源数据中 timestamp的值为 2017-04-19 09:59:49:767 这种形式
			   解析出现错误
			S: 参考最新的 intdata.kesheng_event_params 的sql			   

			
2017-05-04	Q(1): 编写shell程序，实现从hdfs merge文件,放到本地,再上传至ftp上。
			   上传到ftp时，使用mput命令，传了两个文件，就发生问题：EOF received; mput aborted
			S: 关掉 交互功能：prompt off	
			
			Q(2): n=1
				  seqnum=`echo ${subfile} | awk -F "-" -v n=$n '{printf("%06d",$NF+n)}'`
				  按习惯用法,使用 '{print("%06d",$NF+n)}',不能实现
		    S：print输出指定内容后换行，printf只输出指定内容后不换行
			   print($NF+n),不支持"%06d"格式输出
			
			
									 