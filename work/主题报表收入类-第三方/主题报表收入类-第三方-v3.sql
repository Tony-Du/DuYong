
select bb.dept_id as dept_id
       ,bb.term_prod_id as term_prod_id
       ,bb.term_video_type_id as term_video_type_id
       ,bb.term_video_soft_id as term_video_soft_id
       ,bb.term_prod_type_id as term_prod_type_id
       ,bb.term_version_id as term_version_id
       ,bb.term_os_type_id as term_os_type_id
       ,bb.busi_id as busi_id
       ,bb.sub_busi_id as sub_busi_id
       ,bb.net_type_id as net_type_id
       ,bb.cp_id as cp_id
       ,bb.product_id as product_id
       ,bb.content_id as content_id
       ,bb.content_type as content_type
       ,bb.broadcast_type_id as broadcast_type_id
       ,bb.user_type_id as user_type_id
       ,bb.province_id as province_id
       ,bb.city_id as city_id
       ,bb.phone_province_id as phone_province_id
       ,bb.phone_city_id as phone_city_id
       ,bb.company_id as company_id
       ,bb.chn_id as chn_id
       ,bb.chn_attr_1_id as chn_attr_1_id
       ,bb.chn_attr_2_id as chn_attr_2_id
       ,bb.chn_attr_3_id as chn_attr_3_id
       ,bb.chn_attr_4_id as chn_attr_4_id
       ,bb.chn_type as chn_type
       ,bb.con_class_1_name as con_class_1_name
       ,bb.copyright_type as copyright_type
       ,bb.busi_type_id as busi_type_id
       ,bb.program_id as program_id
       ,sum(bb.third_add_period_revenue) as third_add_period_revenue                                                                     	--天。第三方新增包周期收入
       ,sum(bb.mon_third_add_period_revenue) as mon_third_add_period_revenue                                                             	--月。第三方新增包周期收入
       ,sum(bb.mon_third_actual_revenue) as mon_third_actual_revenue                                                                     	--月。第三方实际收入(包月分摊+按次）之按次 
       ,sum(case when (bb.third_add_period_order_user_flag = 'Y') then 1 else 0 end) as third_add_period_order_user_num                  	--天。第三方新增包周期订购用户
       ,sum(case when (bb.mon_third_add_period_order_user_flag = 'Y') then 1 else 0 end) as mon_third_add_period_order_user_num          	--月。第三方新增包周期订购用户
       ,sum(case when (bb.third_add_time_order_user_flag = 'Y') then 1 else 0 end) as third_add_time_order_user_num                      	--天。第三方新增按次订购用户
       ,sum(case when (bb.mon_third_add_time_order_user_flag = 'Y') then 1 else 0 end) as mon_third_add_time_order_user_num              	--月。第三方新增按次订购用户
       ,sum(bb.third_add_time_revenue) as third_add_time_revenue                                                                         	--天。第三方新增按次收入
       ,sum(bb.mon_third_add_time_revenue) as mon_third_add_time_revenue                                                                 	--月。第三方新增按次收入
       ,sum(case when (bb.mon_third_month_share_pay_user_flag = 'Y') then 1 else 0 end) as mon_third_month_share_pay_user_num            	--月。第三方包月分摊付费用户
       ,sum(case when (bb.third_pay_user_flag = 'Y') then 1 else 0 end) as third_pay_user_num                                            	--天。第三方付费用户（包月分摊+按次）之按次
       ,sum(case when (bb.mon_third_pay_user_flag = 'Y') then 1 else 0 end) as mon_third_pay_user_num                                    	--月。第三方付费用户（包月分摊+按次）之按次
       ,sum(case when (bb.boss_third_add_period_order_user_flag = 'Y') then 1 else 0 end) as boss_third_add_period_order_user_num        	--天。boss+第三方新增包周期订购用户
       ,sum(case when (bb.mon_boss_third_add_period_order_user_flag = 'Y') then 1 else 0 end) as mon_boss_third_add_period_order_user_num	--月。boss+第三方新增包周期订购用户
       ,sum(case when (bb.boss_third_add_time_order_user_flag = 'Y') then 1 else 0 end) as boss_third_add_time_order_user_num            	--天。boss+第三方新增按次订购用户
       ,sum(case when (bb.mon_boss_third_add_time_order_user_flag = 'Y') then 1 else 0 end) as mon_boss_third_add_time_order_user_num    	--月。boss+第三方新增按次订购用户
       ,sum(case when (bb.boss_third_month_in_order_user_flag = 'Y') then 1 else 0 end ) as boss_third_month_in_order_user_num           	--天。boss+第三方包月在订用户
       ,sum(bb.sale_revenue) as sale_revenue                                                                                             	--销售收入
       ,cast(bb.dim_group_id as int) & 2147483647 as dim_group_id      
  from (
        select tt.dept_id
              ,tt.term_prod_id
              ,tt.term_video_type_id
              ,tt.term_video_soft_id
              ,tt.term_prod_type_id
              ,tt.term_version_id
              ,tt.term_os_type_id
              ,tt.busi_id
              ,tt.sub_busi_id
              ,tt.net_type_id
              ,tt.cp_id
              ,tt.product_id
              ,tt.content_id
              ,tt.content_type
              ,tt.broadcast_type_id
              ,tt.user_type_id
              ,tt.province_id
              ,tt.city_id
              ,tt.phone_province_id
              ,tt.phone_city_id
              ,tt.company_id
              ,tt.chn_id
              ,tt.chn_attr_1_id
              ,tt.chn_attr_2_id
              ,tt.chn_attr_3_id
              ,tt.chn_attr_4_id
              ,tt.chn_type
              ,tt.con_class_1_name
              ,tt.copyright_type
              ,tt.busi_type_id
              ,tt.program_id
              ,tt.user_id
              ,sum(tt.third_add_period_revenue) as third_add_period_revenue										--天。第三方新增包周期收入
              ,sum(tt.mon_third_add_period_revenue) as mon_third_add_period_revenue								--月。第三方新增包周期收入
              ,sum(tt.mon_third_actual_revenue) as mon_third_actual_revenue										--月。第三方实际收入(包月分摊+按次）之按次  
              ,max(tt.third_add_period_order_user_flag) as third_add_period_order_user_flag						--天。第三方新增包周期订购用户
              ,max(tt.mon_third_add_period_order_user_flag) as mon_third_add_period_order_user_flag				--月。第三方新增包周期订购用户
              ,max(tt.third_add_time_order_user_flag) as third_add_time_order_user_flag							--天。第三方新增按次订购用户
              ,max(tt.mon_third_add_time_order_user_flag) as mon_third_add_time_order_user_flag					--月。第三方新增按次订购用户
              ,sum(tt.third_add_time_revenue) as third_add_time_revenue											--天。第三方新增按次收入
              ,sum(tt.mon_third_add_time_revenue) as mon_third_add_time_revenue									--月。第三方新增按次收入
              ,max(tt.mon_third_month_share_pay_user_flag) as mon_third_month_share_pay_user_flag				--月。第三方包月分摊付费用户
              ,max(tt.third_pay_user_flag) as third_pay_user_flag												--天。第三方付费用户（包月分摊+按次）之按次
              ,max(tt.mon_third_pay_user_flag) as mon_third_pay_user_flag										--月。第三方付费用户（包月分摊+按次）之按次
              ,max(tt.boss_third_add_period_order_user_flag) as boss_third_add_period_order_user_flag			--天。boss+第三方新增包周期订购用户
              ,max(tt.mon_boss_third_add_period_order_user_flag) as mon_boss_third_add_period_order_user_flag	--月。boss+第三方新增包周期订购用户
              ,max(tt.boss_third_add_time_order_user_flag) as boss_third_add_time_order_user_flag				--天。boss+第三方新增按次订购用户
              ,max(tt.mon_boss_third_add_time_order_user_flag) as mon_boss_third_add_time_order_user_flag		--月。boss+第三方新增按次订购用户
              ,max(tt.boss_third_month_in_order_user_flag) as boss_third_month_in_order_user_flag				--天。boss+第三方包月在订用户
              ,sum(tt.sale_revenue) as sale_revenue																--销售收入
              ,grouping__id as dim_group_id
          from ( 
                select case when e.term_prod_name in ('和视频OPENAPI','和视频SDK') then nvl(d.dept_id,'-998') else nvl(b.dept_id,nvl(c.dept_id,nvl(d.dept_id,'-998'))) end as dept_id
                       ,a.term_prod_id as term_prod_id
                       ,nvl(e.term_video_type_id, '-998') as term_video_type_id
                       ,nvl(e.term_video_soft_id, '-998') as term_video_soft_id
                       ,nvl(e.term_prod_type_id, '-998') as term_prod_type_id
                       ,a.term_version_id as term_version_id
                       ,nvl(e.term_os_type_id, '-998') as term_os_type_id
                       ,nvl(h.business_id, '-998') as busi_id
                       ,a.sub_busi_id as sub_busi_id
                       ,a.network_type as net_type_id
                       ,a.cp_id as cp_id
                       ,a.product_id as product_id
                       ,'-999' as content_id
                       ,'-999' as content_type
                       ,a.use_type as broadcast_type_id
                       ,a.user_type as user_type_id
                       ,nvl(f.prov_id, '-998') as province_id
                       ,a.city_id as city_id
                       ,nvl(f.prov_id, '-998') as phone_province_id
                       ,a.city_id as phone_city_id
                       ,a.company_id as company_id
                       ,nvl(g.chn_id, '-998') as chn_id
                       ,nvl(g.chn_attr_1_id, '-998') as chn_attr_1_id
                       ,nvl(g.chn_attr_2_id, '-998') as chn_attr_2_id
                       ,nvl(g.chn_attr_3_id, '-998') as chn_attr_3_id
                       ,nvl(g.chn_attr_4_id, '-998') as chn_attr_4_id
                       ,nvl(g.chn_type, '-998') as chn_type
                       ,'-998' as con_class_1_name
                       ,'-998' as copyright_type
                       ,nvl(h.b_type_id, '-998') as busi_type_id
                       ,a.program_id as program_id
                       ,a.serv_number as user_id
                       ,case when a.src_file_day = '${SRC_FILE_DAY}' and a.main = 0 and a.period_unit <> 'hour' and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE') 
                             and a.pay_chn_type not in (50,58,59,302,304,306) and a.order_status = '5' then a.amount else 0 end as third_add_period_revenue       --天。第三方新增包周期收入
                       ,case when a.main = 0 and a.period_unit <> 'hour' and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE') 
                             and a.pay_chn_type not in (50,58,59,302,304,306) and a.order_status = '5' then a.amount else 0 end as mon_third_add_period_revenue   --月。第三方新增包周期收入           
                       ,case when a.main = 0 and a.period_unit = 'hour' and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE') 
                             and a.pay_chn_type not in (50,58,59,302,304,306) and a.order_status = '5' then a.amount else 0 end as mon_third_actual_revenue       --月。第三方实际收入(包月分摊+按次）之按次          
                       ,case when a.src_file_day = '${SRC_FILE_DAY}' and a.period_unit <> 'hour' and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE') 
                             and a.pay_chn_type not in (50,58,59,302,304,306) and a.order_status = '5' then 'Y' else 'N' end as third_add_period_order_user_flag       --天。第三方新增包周期订购用户
                       ,case when a.period_unit <> 'hour' and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE') 
                             and a.pay_chn_type not in (50,58,59,302,304,306) and a.order_status = '5' then 'Y' else 'N' end as mon_third_add_period_order_user_flag   --月。第三方新增包周期订购用户             
                       ,case when a.src_file_day = '${SRC_FILE_DAY}' and a.period_unit = 'hour' and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')
                             and a.pay_chn_type not in (50,58,59,302,304,306) and a.order_status = '5' then 'Y' else 'N' end as third_add_time_order_user_flag     --天。第三方新增按次订购用户
                       ,case when a.period_unit = 'hour' and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')
                             and a.pay_chn_type not in (50,58,59,302,304,306) and a.order_status = '5' then 'Y' else 'N' end as mon_third_add_time_order_user_flag --月。第三方新增按次订购用户          
                       ,case when a.src_file_day = '${SRC_FILE_DAY}' and a.main = 0 and a.period_unit = 'hour' and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')
                             and a.pay_chn_type not in (50,58,59,302,304,306) and a.order_status = '5' then a.amount else 0 end as third_add_time_revenue         --天。第三方新增按次收入            
                       ,case when a.main = 0 and a.period_unit = 'hour' and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')
                             and a.pay_chn_type not in (50,58,59,302,304,306) and a.order_status = '5' then a.amount else 0 end as mon_third_add_time_revenue     --月。第三方新增按次收入       
                       ,'N' as mon_third_month_share_pay_user_flag              --月。第三方包月分摊付费用户      
                       ,case when a.src_file_day = '${SRC_FILE_DAY}' and a.main = 0 and a.period_unit = 'hour' and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')    
                             and a.pay_chn_type not in (50,58,59,302,304,306) and a.order_status = '5' then 'Y' else 'N' end as third_pay_user_flag        --天。第三方付费用户（包月分摊+按次）之按次      
                       ,case when a.main = 0 and a.period_unit = 'hour' and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')    
                             and a.pay_chn_type not in (50,58,59,302,304,306) and a.order_status = '5' then 'Y' else 'N' end as mon_third_pay_user_flag    --月。第三方付费用户（包月分摊+按次）之按次          
                       ,case when a.src_file_day = '${SRC_FILE_DAY}' and a.period_unit <> 'hour' and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')
                             and a.order_status = '5' then a.user_id else '' end as boss_third_add_period_order_user_flag      --天。boss+第三方新增包周期订购用户            
                       ,case when a.period_unit <> 'hour' and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')
                             and a.order_status = '5' then 'Y' else 'N' end as mon_boss_third_add_period_order_user_flag       --月。boss+第三方新增包周期订购用户            
                       ,case when a.src_file_day = '${SRC_FILE_DAY}' and a.period_unit = 'hour' and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE') 
                             and a.order_status = '5' then 'Y' else 'N' end as boss_third_add_time_order_user_flag     --天。boss+第三方新增按次订购用户                 
                       ,case when a.period_unit = 'hour' and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE') 
                             and a.order_status = '5' then 'Y' else 'N' end as mon_boss_third_add_time_order_user_flag --月。boss+第三方新增按次订购用户
                       ,'N' as boss_third_month_in_order_user_flag          --天。boss+第三方包月在订用户  
                       ,case when a.main = 0 and a.goods_type in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')
                             and order_status = '5' then a.amount else 0 end as sale_revenue --销售收入            
                  from rptdata.fact_ugc_order_detail_daily a
                  left join rptdata.dim_server h
                    on a.sub_busi_id = h.sub_busi_id --and h.dw_delete_flag = 'N'
                  left join rptdata.dim_dept_term_prod b --<部门-终端产品维表> b
                    on a.term_prod_id = b.term_prod_id  --and b.dw_delete_flag = 'N'
                  left join rptdata.dim_dept_busi c --<部门-业务维表> c
                    on h.business_id = c.business_id --and c.dw_delete_flag = 'N'
                  left join rptdata.dim_dept_chn d --<部门-渠道维表> d
                    on a.chn_id = d.chn_id --and d.dw_delete_flag = 'N'
                  left join rpt.dim_term_prod_v e
                    on a.term_prod_id = e.term_prod_id --and e.dw_delete_flag = 'N'
                  left join rptdata.dim_region f
                    on a.city_id = f.region_id --and f.dw_delete_flag = 'N'
                  left join rptdata.dim_chn g
                    on a.chn_id = g.chn_id --and g.dw_delete_flag = 'N' 
                 where a.src_file_day >= '${SRC_FILE_MONTH}01' and a.src_file_day <= '${SRC_FILE_DAY}' 
                 
                 union all 
                 
                select case when e.term_prod_name in ('和视频OPENAPI','和视频SDK') then nvl(d.dept_id,'-998') else nvl(b.dept_id,nvl(c.dept_id,nvl(d.dept_id,'-998'))) end as dept_id
                       ,t.term_prod_id as term_prod_id
                       ,nvl(e.term_video_type_id, '-998') as term_video_type_id
                       ,nvl(e.term_video_soft_id, '-998') as term_video_soft_id
                       ,nvl(e.term_prod_type_id, '-998') as term_prod_type_id
                       ,t.term_version_id as term_version_id
                       ,nvl(e.term_os_type_id, '-998') as term_os_type_id
                       ,nvl(h.business_id, '-998') as busi_id
                       ,t.sub_busi_id as sub_busi_id
                       ,t.net_type_id as net_type_id
                       ,t.cp_id as cp_id
                       ,'-999' as product_id
                       ,'-999' as content_id
                       ,'-999' as content_type
                       ,t.broadcast_type_id as broadcast_type_id
                       ,t.user_type_id as user_type_id
                       ,nvl(f.prov_id, '-998') as province_id
                       ,t.city_id as city_id
                       ,nvl(f.prov_id, '-998') as phone_province_id
                       ,t.city_id as phone_city_id
                       ,t.company_id as company_id
                       ,nvl(g.chn_id, '-998') as chn_id
                       ,nvl(g.chn_attr_1_id, '-998') as chn_attr_1_id
                       ,nvl(g.chn_attr_2_id, '-998') as chn_attr_2_id
                       ,nvl(g.chn_attr_3_id, '-998') as chn_attr_3_id
                       ,nvl(g.chn_attr_4_id, '-998') as chn_attr_4_id
                       ,nvl(g.chn_type, '-998') as chn_type
                       ,'-998' as con_class_1_name
                       ,'-998' as copyright_type
                       ,nvl(h.b_type_id, '-998') as busi_type_id
                       ,'-998' as program_id
                       ,t.user_id as user_id       
                       ,0 as third_add_period_revenue       --天。第三方新增包周期收入
                       ,0 as mon_third_add_period_revenue   --月。第三方新增包周期收入     
                       ,case when t.expire_time >= '${SRC_FILE_DAY}' and t.start_time <= '${SRC_FILE_DAY}' 
                             then t.period_amount else 0 end as mon_third_actual_revenue --月。第三方实际收入(包月分摊+按次）之包月分摊 
                       ,'N' as third_add_period_order_user_flag       --天。第三方新增包周期订购用户
                       ,'N' as mon_third_add_period_order_user_flag   --月。第三方新增包周期订购用户       
                       ,'N' as third_add_time_order_user_flag         --天。第三方新增按次订购用户
                       ,'N' as mon_third_add_time_order_user_flag     --月。第三方新增按次订购用户    
                       ,0 as third_add_time_revenue              --天。第三方新增按次收入
                       ,0 as mon_third_add_time_revenue          --月。第三方新增按次收入      
                       ,case when t.expire_time >= '${SRC_FILE_DAY}' and t.start_time <= '${SRC_FILE_DAY}' 
                             then 'Y' else 'N' end as mon_third_month_share_pay_user_flag    --月。第三方包月分摊付费用户     
                       ,case when t.src_file_day = '${SRC_FILE_DAY}' and t.expire_time >= '${SRC_FILE_DAY}' and t.start_time <= '${SRC_FILE_DAY}' 
                             then 'Y' else 'N' end as third_pay_user_flag      --天。第三方付费用户（包月分摊+按次）之包月分摊
                       ,case when t.expire_time >= '${SRC_FILE_DAY}' and t.start_time <= '${SRC_FILE_DAY}'
                             then 'Y' else 'N' end as mon_third_pay_user_flag  --月。第三方付费用户（包月分摊+按次）之包月分摊     
                       ,'N' as boss_third_add_period_order_user_flag      --天。boss+第三方新增包周期订购用户
                       ,'N' as mon_boss_third_add_period_order_user_flag  --月。boss+第三方新增包周期订购用户      
                       ,'N' as boss_third_add_time_order_user_flag        --天。boss+第三方新增按次订购用户
                       ,'N' as mon_boss_third_add_time_order_user_flag    --月。boss+第三方新增按次订购用户       
                       ,case when t.src_file_day = '${SRC_FILE_DAY}' and t.expire_time >= '${SRC_FILE_DAY}' and t.start_time <= '${SRC_FILE_DAY}'
                             then 'Y' else 'N' end as boss_third_month_in_order_user_flag  --天。boss+第三方包月在订用户(其一)    
                       ,0 as sale_revenue --销售收入
                  from intdata.ugc_tpp_contract t  
                  left join rptdata.dim_server h
                    on t.sub_busi_id = h.sub_busi_id --and h.dw_delete_flag = 'N'
                  left join rptdata.dim_dept_term_prod b --<部门-终端产品维表> b
                    on t.term_prod_id = b.term_prod_id  --and b.dw_delete_flag = 'N'
                  left join rptdata.dim_dept_busi c --<部门-业务维表> c
                    on h.business_id = c.business_id --and c.dw_delete_flag = 'N'
                  left join rptdata.dim_dept_chn d --<部门-渠道维表> d
                    on t.chn_id = d.chn_id --and d.dw_delete_flag = 'N'
                  left join rpt.dim_term_prod_v e
                    on t.term_prod_id = e.term_prod_id --and e.dw_delete_flag = 'N'
                  left join rptdata.dim_region f
                    on t.city_id = f.region_id --and f.dw_delete_flag = 'N'
                  left join rptdata.dim_chn g
                    on t.chn_id = g.chn_id --and g.dw_delete_flag = 'N'         
                 where t.src_file_day >= '${SRC_FILE_MONTH}01' and t.src_file_day <= '${SRC_FILE_DAY}'
                                 
                 union all
                 
                select case when e.term_prod_name in ('和视频OPENAPI','和视频SDK') then nvl(d.dept_id,'-998') else nvl(b.dept_id,nvl(c.dept_id,nvl(d.dept_id,'-998'))) end as dept_id
                       ,a.term_prod_id as term_prod_id
                       ,nvl(e.term_video_type_id, '-998') as term_video_type_id
                       ,nvl(e.term_video_soft_id, '-998') as term_video_soft_id
                       ,nvl(e.term_prod_type_id, '-998') as term_prod_type_id
                       ,a.term_version_id as term_version_id
                       ,nvl(e.term_os_type_id, '-998') as term_os_type_id
                       ,nvl(h.business_id, '-998') as busi_id
                       ,a.sub_busi_id as sub_busi_id
                       ,a.net_type_id as net_type_id
                       ,a.cp_id as cp_id
                       ,a.product_id as product_id
                       ,'-999' as content_id
                       ,'-999' as content_type
                       ,a.broadcast_type_id as broadcast_type_id
                       ,a.user_type_id as user_type_id
                       ,nvl(f.prov_id, '-998') as province_id
                       ,a.city_id as city_id
                       ,nvl(f.prov_id, '-998') as phone_province_id
                       ,a.city_id as phone_city_id
                       ,a.company_id as company_id
                       ,nvl(g.chn_id, '-998') as chn_id
                       ,nvl(g.chn_attr_1_id, '-998') as chn_attr_1_id
                       ,nvl(g.chn_attr_2_id, '-998') as chn_attr_2_id
                       ,nvl(g.chn_attr_3_id, '-998') as chn_attr_3_id
                       ,nvl(g.chn_attr_4_id, '-998') as chn_attr_4_id
                       ,nvl(g.chn_type, '-998') as chn_type
                       ,'-998' as con_class_1_name
                       ,'-998' as copyright_type
                       ,nvl(h.b_type_id, '-998') as busi_type_id
                       ,'-998' as program_id
                       ,a.user_id as user_id
                       ,0 as third_add_period_revenue       --天。第三方新增包周期收入
                       ,0 as mon_third_add_period_revenue   --月。第三方新增包周期收入
                       ,0 as mon_third_actual_revenue       --月。第三方实际收入(包月分摊+按次）
                       ,'N' as third_add_period_order_user_flag      --天。第三方新增包周期订购用户
                       ,'N' as mon_third_add_period_order_user_flag   --月。第三方新增包周期订购用户       
                       ,'N' as third_add_time_order_user_flag         --天。第三方新增按次订购用户
                       ,'N' as mon_third_add_time_order_user_flag     --月。第三方新增按次订购用户    
                       ,0 as third_add_time_revenue           --天。第三方新增按次收入
                       ,0 as mon_third_add_time_revenue       --月。第三方新增按次收入      
                       ,'N' as mon_third_month_share_pay_user_flag  --月。第三方包月分摊付费用户
                       ,'N' as third_pay_user_flag  --天。第三方付费用户（包月分摊+按次）
                       ,'N' as mon_third_pay_user_flag  --月。第三方付费用户（包月分摊+按次）
                       ,'N' as boss_third_add_period_order_user_flag      --天。boss+第三方新增包周期订购用户
                       ,'N' as mon_boss_third_add_period_order_user_flag  --月。boss+第三方新增包周期订购用户      
                       ,'N' as boss_third_add_time_order_user_flag        --天。boss+第三方新增按次订购用户
                       ,'N' as mon_boss_third_add_time_order_user_flag    --月。boss+第三方新增按次订购用户  
                       ,case when a.src_file_day = '${SRC_FILE_DAY}' and a.chrgprod_price > 0 and a.sub_status is null 
                             and a.order_status <> '4' then 'Y' else 'N' end as boss_third_month_in_order_user_flag    --天。boss+第三方包月在订用户(其二) 
                       ,0 as sale_revenue --销售收入      
                  from rptdata.fact_ugc_order_relation_detail_daily a
                  left join rptdata.dim_server h
                    on a.sub_busi_id = h.sub_busi_id --and h.dw_delete_flag = 'N'
                  left join rptdata.dim_dept_term_prod b --<部门-终端产品维表> b
                    on a.term_prod_id = b.term_prod_id  --and b.dw_delete_flag = 'N'
                  left join rptdata.dim_dept_busi c --<部门-业务维表> c
                    on h.business_id = c.business_id --and c.dw_delete_flag = 'N'
                  left join rptdata.dim_dept_chn d --<部门-渠道维表> d
                    on a.chn_id = d.chn_id --and d.dw_delete_flag = 'N'
                  left join rpt.dim_term_prod_v e
                    on a.term_prod_id = e.term_prod_id --and e.dw_delete_flag = 'N'
                  left join rptdata.dim_region f
                    on a.city_id = f.region_id --and f.dw_delete_flag = 'N'
                  left join rptdata.dim_chn g
                    on a.chn_id = g.chn_id --and g.dw_delete_flag = 'N'
                 where a.src_file_day = '${SRC_FILE_DAY}'  --当月最后一天 即 整个月       
               ) tt
         group by dept_id,term_prod_id,term_video_type_id,term_video_soft_id,term_prod_type_id,term_version_id,term_os_type_id,busi_id,sub_busi_id,
                  net_type_id,cp_id,product_id,content_id,content_type,broadcast_type_id,user_type_id,province_id,city_id,phone_province_id,
                  phone_city_id,company_id,chn_id,chn_attr_1_id,chn_attr_2_id,chn_attr_3_id,chn_attr_4_id,chn_type, con_class_1_name,copyright_type,
                  busi_type_id, program_id,user_id
        grouping sets ( -- 核心
                       (dept_id, user_id), 
                       (dept_id, term_video_type_id, user_id),
                       (dept_id, term_video_type_id, term_prod_id, user_id),
                       (term_video_type_id, user_id),
                       (term_video_type_id, term_prod_id, user_id),
                       -- 终端
                       --(term_video_type_id, user_id),
                       (term_video_type_id, user_type_id, user_id),
                       (net_type_id, term_video_type_id, user_id),
                       (broadcast_type_id, term_video_type_id, user_id),
                       (busi_id, term_video_type_id, user_id),
                       (term_video_type_id, term_prod_id, user_id),
                       (term_video_type_id, term_prod_id, user_type_id, user_id),
                       (term_prod_type_id, user_id),
                       (term_video_soft_id, user_id),
                       (term_video_type_id, term_prod_id, term_version_id, user_type_id, user_id),
                       (term_video_type_id, term_prod_id, term_version_id, user_id),
                       -- 合作伙伴
                       (dept_id, busi_id, company_id, user_type_id, user_id),
                       (dept_id, busi_id, company_id, user_id),
                       (dept_id, busi_id, sub_busi_id, company_id, user_id),
                       (dept_id, busi_id, company_id, chn_id, chn_attr_1_id, chn_attr_2_id, user_id),--add by 20170510
                       (dept_id, phone_province_id, company_id, user_id),
                       (dept_id, busi_id, phone_province_id, company_id, user_type_id, user_id),
                       (dept_id, busi_id, phone_province_id, company_id, user_id),
                       (dept_id, busi_id, phone_province_id, company_id, user_type_id, chn_id, chn_attr_1_id, chn_attr_2_id, user_id),--add by 20170510
                       (dept_id, term_video_type_id, company_id, user_id),
                       (dept_id, term_video_type_id, company_id, user_type_id, user_id),
                       (dept_id, busi_id, phone_province_id, company_id, chn_id, chn_attr_1_id, chn_attr_2_id, user_id),--add by 20170510
                       -- 地域
                       (phone_province_id, user_id),
                       (phone_province_id, phone_city_id, user_id),
                       (phone_province_id, term_video_type_id, user_id),
                       (phone_province_id, phone_city_id, term_video_type_id, user_id),
                       (phone_province_id, term_prod_type_id, user_id),
                       (phone_province_id, phone_city_id, term_prod_type_id, user_id),
                       (phone_province_id, net_type_id, user_id),
                       (phone_province_id, phone_city_id, net_type_id, user_id),
                       (phone_province_id, busi_id, user_id),
                       (phone_province_id, busi_id, sub_busi_id, user_id),
                       (phone_province_id, phone_city_id, busi_id, user_id),
                       (phone_province_id, phone_city_id, busi_id, sub_busi_id, user_id),
                       (phone_province_id, broadcast_type_id, net_type_id, user_id),
                       (phone_province_id, phone_city_id, broadcast_type_id, net_type_id, user_id),
                       (phone_province_id, term_video_type_id, net_type_id, user_id),
                       (phone_province_id, phone_city_id, term_video_type_id, net_type_id, user_id),
                       (phone_province_id, busi_id, net_type_id, user_id),
                       (phone_province_id, phone_city_id, busi_id, net_type_id, user_id),
                       -- 渠道主题报表
                       (term_video_type_id, user_type_id, chn_type, user_id),
                       (term_video_type_id, user_type_id, chn_attr_1_id, chn_attr_2_id, user_id),
                       (term_video_type_id, user_type_id, chn_attr_1_id, user_id),
                       (user_type_id, province_id, city_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type, user_id),
                       (user_type_id, province_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type, user_id),
                       (term_video_type_id, user_type_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type, user_id),
                       (term_prod_id, term_video_type_id, user_type_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type, user_id),
                       (term_prod_id, term_video_type_id, user_type_id, chn_type, user_id),
                       (user_type_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type, user_id),
                       (busi_id, user_type_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type, user_id),
                       (busi_id, user_type_id, province_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type, user_id),
                       (busi_id, user_type_id, province_id, city_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type, user_id),
                       (term_prod_id, user_type_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type, user_id),
                       (term_prod_id, busi_id, user_type_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type, user_id),
                       (term_prod_id, busi_id, user_type_id, province_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type, user_id),
                       (term_prod_id, user_type_id, province_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type, user_id),
                       (term_prod_id, busi_id, user_type_id, province_id, city_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type, user_id),
                       (term_prod_id, user_type_id, province_id, city_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type, user_id),
                       (user_type_id, province_id, chn_attr_1_id, chn_attr_2_id, user_id),
                       (user_type_id, chn_attr_1_id, chn_attr_2_id, user_id),
                       (user_type_id, province_id, chn_attr_1_id, user_id),
                       (user_type_id, chn_attr_1_id, user_id),
                       (busi_id, user_type_id, chn_attr_1_id, chn_attr_2_id, user_id),
                       (busi_id, user_type_id, chn_attr_1_id, user_id),
                       (busi_id, user_type_id, chn_type, user_id),
                       (sub_busi_id, user_type_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type, user_id),
                       (sub_busi_id, user_type_id, province_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_type, user_id),
                       -- 客服
                       (busi_id, sub_busi_id, busi_type_id, user_id),
                       --(phone_province_id, user_id),
                       --(phone_province_id, phone_city_id, user_id),
                       (busi_id, sub_busi_id, phone_province_id, phone_city_id, busi_type_id, user_id),
                       (busi_id, sub_busi_id, phone_province_id, busi_type_id, user_id),
                       (chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, user_id),
                       (phone_province_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, user_id),
                       (phone_province_id, phone_city_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, user_id),
                       (busi_type_id, user_id),
                       --内容引入
                       --(term_video_type_id, term_prod_type_id, cp_id, product_id, content_id, con_class_1_name, copyright_type, user_id),
                       --(term_video_type_id, term_prod_type_id, cp_id, content_id, con_class_1_name, copyright_type, user_id),
                       --(cp_id, user_id),
                       --(cp_id, con_class_1_name, user_id),
                       --(con_class_1_name, user_id),
                       --(cp_id, copyright_type, user_id),
                       --(cp_id, product_id, user_id)
                       -- 新增组合1
                       --(phone_province_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_attr_4_id, chn_type, user_id),
                       -- 新增组合2
                       --(busi_id, phone_province_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_attr_4_id, chn_type, user_id),
                       -- 新增组合3
                       (cp_id, content_id, busi_id, phone_province_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_attr_4_id, chn_type, con_class_1_name, copyright_type, user_id),
                       (sub_busi_id, phone_province_id, chn_id, chn_attr_1_id, chn_attr_2_id, chn_attr_3_id, chn_attr_4_id, chn_type, user_id),
                       
                       --NCP
                       (term_prod_id, term_prod_type_id, busi_id, cp_id, content_id, chn_id, program_id)
                      )
       ) bb
 group by dept_id,term_prod_id,term_video_type_id,term_video_soft_id,term_prod_type_id,term_version_id,term_os_type_id,busi_id,sub_busi_id,
          net_type_id,cp_id,product_id,content_id,content_type,broadcast_type_id,user_type_id,province_id,city_id,phone_province_id,phone_city_id,
          company_id,chn_id,chn_attr_1_id,chn_attr_2_id,chn_attr_3_id,chn_attr_4_id,chn_type, con_class_1_name,copyright_type, busi_type_id, 
          program_id, dim_group_id
