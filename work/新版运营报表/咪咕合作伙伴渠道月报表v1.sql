create or replace view rpt.cdmp_rpt_cp_channel_monthly_v as  
select case when B.dim_group_id & cast(power(2, 7) as bigint) > 0 then nvl(C.business_name, '未知')
            else '剔重汇总' end as business_name,
       case when B.dim_group_id & cast(power(2, 8) as bigint) > 0 then nvl(D.sub_busi_name, '未知')
            else '剔重汇总' end as sub_busi_name,
       case when B.dim_group_id & cast(power(2, 15) as bigint) > 0 then nvl(E.user_type_name, '未知')
            else '剔重汇总'end as user_type_name,
       
       nvl(F.busi_type, '未知') as busi_type,
       
       case when B.dim_group_id & cast(power(2, 18) as bigint) > 0 then nvl(G.prov_name, '未知')
            else '剔重汇总' end as prov_name,
       case when B.dim_group_id & cast(power(2, 20) as bigint) > 0 then nvl(I.comp_name, '未知')
            else '剔重汇总' end as cp_name,
       
       
       
       case when B.dim_group_id & cast(power(2, 20) as bigint) > 0 then nvl(B.company_id, '未知')
            else '剔重汇总' end as cp_id,
       
       case when B.dim_group_id & cast(power(2, 2) as bigint) > 0 then nvl(H.term_video_type_name,'未知' )
            else '剔重汇总'end as term_video_type_name,
       case when B.dim_group_id & cast(power(2, 21) as bigint) > 0 then nvl(J.chn_name,'未知' )
            else '剔重汇总' end as chn_name,
       
       case when B.dim_group_id & cast(power(2, 21) as bigint) > 0 then nvl(B.chn_id,'未知' )
            else '剔重汇总' end as chn_id,
       case when B.dim_group_id & cast(power(2, 22) as bigint) > 0 then nvl(K.chn_attr_1_name,'未知' )
            else '剔重汇总' end as chn_attr_1_name,
       case when B.dim_group_id & cast(power(2, 23) as bigint) > 0 then nvl(l.chn_attr_2_name,'未知' )
            else '剔重汇总' end as chn_attr_2_name,
       
       B.dim_group_id,
       B.src_file_month,
       B.busi_id,
       B.sub_busi_id,
       B.term_video_type_id,
       B.province_id,
       B.company_id,
       B.user_type_id,
       
       B.unknown_complain_num,
       B.new_complain_num,
       B.old_complain_num,
       B.idea_pay_user_num,
       B.visit_user_num,
       B.use_user_num,
       B.use_duration,
       B.use_flow_kb,
       
       B.boss_time_amount,
       B.boss_month_add_info_fee_amount,
       B.boss_month_retention_info_amount,
       B.boss_pay_user_num,
       B.boss_month_add_order_user_num,
       B.boss_month_cancel_order_user_num,
       B.boss_month_in_order_user_num
  from (
        select src_file_month,
               busi_id,
               sub_busi_id,
               term_video_type_id,
               province_id,
               company_id,
               user_type_id,
               chn_id,
               chn_attr_1_id, 
               chn_attr_2_id,
               dim_group_id,
               
               sum(unknown_complain_num) as unknown_complain_num,
               sum(new_complain_num) as new_complain_num,
               sum(old_complain_num) as old_complain_num,
               sum(idea_pay_user_num) as idea_pay_user_num,
               
               sum(visit_user_num) as visit_user_num,
               sum(use_user_num) as use_user_num,
               sum(boss_pay_user_num) as boss_pay_user_num,
               sum(boss_month_add_order_user_num) as boss_month_add_order_user_num,
               sum(boss_month_cancel_order_user_num) as boss_month_cancel_order_user_num,
               sum(boss_month_in_order_user_num) as boss_month_in_order_user_num,
               
               sum(boss_time_amount) as boss_time_amount,
               sum(boss_month_add_info_fee_amount) as boss_month_add_info_fee_amount,
               sum(boss_month_retention_info_amount) as boss_month_retention_info_amount,
               sum(use_duration) as use_duration,
               sum(use_flow_kb) as use_flow_kb
          from (
                select src_file_month,
                       busi_id,
                       sub_busi_id,
                       term_video_type_id,
                       phone_province_id as province_id,
                       company_id,
                       user_type_id,
                       chn_id,
                       chn_attr_1_id, 
                       chn_attr_2_id,
                       dim_group_id,
                       
                       unknown_complain_num,
                       new_complain_num,
                       old_complain_num,
                       idea_pay_user_num,
                       
                       visit_user_num,
                       use_user_num,
                       boss_pay_user_num,
                       boss_month_add_order_user_num,
                       boss_month_cancel_order_user_num,
                       boss_month_in_order_user_num,
                       
                       0 as boss_time_amount,
                       0 as boss_month_add_info_fee_amount,
                       0 as boss_month_retention_info_amount,
                       0 as use_duration,
                       0 as use_flow_kb
                       
                  from rptdata.cdmp_rpt_user_num_monthly 
                 where dept_id = 'D04'
                   and lpad(bin(dim_group_id), 31, '0') = '0000000111100000000000010000001'
                   
                 union all 

                select src_file_month,
                       busi_id,
                       sub_busi_id,
                       term_video_type_id,
                       phone_province_id as province_id,
                       company_id,
                       user_type_id,
                       chn_id,
                       chn_attr_1_id, 
                       chn_attr_2_id,
                       dim_group_id,
                       
                       0 as unknown_complain_num,
                       0 as new_complain_num,
                       0 as old_complain_num,
                       0 as idea_pay_user_num,
                       
                       0 as visit_user_num,
                       0 as use_user_num,
                       0 as boss_pay_user_num,
                       0 as boss_month_add_order_user_num,
                       0 as boss_month_cancel_order_user_num,
                       0 as boss_month_in_order_user_num,
                       
                       boss_time_amount/100 as boss_time_amount,
                       boss_month_add_info_fee_amount/100 as boss_month_add_info_fee_amount,
                       boss_month_retention_info_amount/100 as boss_month_retention_info_amount, 
                       use_duration/60 as use_duration,
                       use_flow_kb/1024 as use_flow_kb
                       
                  from rptdata.cdmp_rpt_cnt_monthly
                 where dept_id = 'D04'
                   and lpad(bin(dim_group_id), 31, '0') ='0000000111100000000000010000001'
               ) all
          group by src_file_month,
                   busi_id,
                   sub_busi_id,
                   term_video_type_id,
                   province_id,
                   company_id,
                   user_type_id,
                   chn_id,
                   chn_attr_1_id, 
                   chn_attr_2_id,
                   dim_group_id
       )B
left join (select distinct business_id,business_name from rptdata.dim_server) C
  on B.busi_id = C.business_id
left join (select distinct sub_busi_id,sub_busi_name from rptdata.dim_server) D
  on B.sub_busi_id = D.sub_busi_id
left join (select distinct business_id,busi_type from  rptdata.dim_comp_busi) F
  on B.busi_id = F.business_id
left join (select distinct prov_id,prov_name from rptdata.dim_region) G
  on B.province_id = G.prov_id
left join rpt.dim_term_prod_video_type_v H
  on B.term_video_type_id = H.term_video_type_id
left join (select  distinct comp_id,comp_name  from rptdata.dim_comp_busi) I
  on B.company_id = I.comp_id
left join (select  distinct chn_id,chn_name  from rptdata.dim_chn) J
  on B.chn_id = J.chn_id

left join (select distinct chn_attr_1_id,chn_attr_1_name from rptdata.dim_chn) K
  on B.chn_attr_1_id = K.chn_attr_1_id
left join (select distinct chn_attr_2_id,chn_attr_2_name from rptdata.dim_chn) L
  on B.chn_attr_2_id = L.chn_attr_2_id

left join (select  distinct user_type_id,user_type_name  from rptdata.dim_user_type) E
  on B.user_type_id = E.user_type_id;
  
  