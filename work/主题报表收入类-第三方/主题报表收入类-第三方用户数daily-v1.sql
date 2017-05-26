--set mapreduce.job.name=rptdata.fact_ugc_order_user_num_daily${SRC_FILE_DAY};

--set hive.groupby.skewindata=false;
--set hive.optimize.skewjoin=true;
--set hive.merge.mapredfiles = true;
--set hive.exec.reducers.max = 7099;

--insert overwrite table rptdata.fact_ugc_order_user_num_daily partition (src_file_day='${SRC_FILE_DAY}')
select 
    bb.dept_id as dept_id,
    bb.term_prod_id as term_prod_id,
    bb.term_video_type_id as term_video_type_id,
    bb.term_video_soft_id as term_video_soft_id,
    bb.term_prod_type_id as term_prod_type_id,
    bb.term_version_id as term_version_id,
    bb.term_os_type_id as term_os_type_id,
    bb.busi_id as busi_id,
    bb.sub_busi_id as sub_busi_id,
    bb.net_type_id as net_type_id,
    bb.cp_id as cp_id,
    bb.product_id as product_id,
    bb.content_id as content_id,
    bb.content_type as content_type,
    bb.broadcast_type_id as broadcast_type_id,
    bb.user_type_id as user_type_id,
    bb.province_id as province_id,
    bb.city_id as city_id,
    bb.phone_province_id as phone_province_id,
    bb.phone_city_id as phone_city_id,
    bb.company_id as company_id,
    bb.chn_id as chn_id,
    bb.chn_attr_1_id as chn_attr_1_id,
    bb.chn_attr_2_id as chn_attr_2_id,
    bb.chn_attr_3_id as chn_attr_3_id,
    bb.chn_attr_4_id as chn_attr_4_id,
    bb.chn_type as chn_type, 
    bb.con_class_1_name as con_class_1_name,
    bb.copyright_type as copyright_type,
    bb.busi_type_id as busi_type_id,
    bb.program_id as program_id,
    SUM(case when (bb.boss_time_add_order_user_flag = 'Y') then 1 else 0 end) as boss_time_add_order_user_num,
    SUM(case when (bb.mon_boss_time_add_order_user_flag = 'Y') then 1 else 0 end) as mon_boss_time_add_order_user_num,
    SUM(case when (bb.boss_month_in_order_user_flag = 'Y') then 1 else 0 end) as boss_month_in_order_user_num,
    SUM(case when (bb.mon_boss_month_in_order_user_flag = 'Y') then 1 else 0 end) as mon_boss_month_in_order_user_num,
    SUM(case when (bb.boss_month_add_order_user_flag = 'Y') then 1 else 0 end) as boss_month_add_order_user_num,
    SUM(case when (bb.mon_boss_month_add_order_user_flag = 'Y') then 1 else 0 end) as mon_boss_month_add_order_user_num,
    SUM(case when (bb.boss_month_cancel_order_user_flag = 'Y') then 1 else 0 end) as boss_month_cancel_order_user_num,
    SUM(case when (bb.mon_boss_month_cancel_order_user_flag = 'Y') then 1 else 0 end) as mon_boss_month_cancel_order_user_num,
    SUM(case when (bb.boss_month_add_order_user_flag = 'Y' and bb.boss_month_cancel_order_user_flag ='Y') then 1 else 0 end) as boss_month_add_and_cancel_user_num,
    SUM(case when (bb.mon_boss_month_add_order_user_flag = 'Y' and bb.mon_boss_month_cancel_order_user_flag = 'Y') then 1 else 0 end) as mon_boss_month_add_and_cancel_user_num,
    SUM(case when (bb.boss_month_in_order_user_flag = 'Y' or bb.boss_month_add_order_user_flag ='Y') then 1 else 0 end) as boss_month_pay_user_num,
    SUM(case when (bb.mon_boss_month_in_order_user_flag = 'Y' or bb.mon_boss_month_add_order_user_flag ='Y') then 1 else 0 end) as mon_boss_month_pay_user_num,    
    SUM(case when (bb.third_add_month_order_user_flag = 'Y') then 1 else 0 end) as third_add_month_order_user_num,      --第三方新增包月(周期)订购用户数
    SUM(case when (bb.mon_third_add_month_order_user_flag = 'Y') then 1 else 0 end) as mon_third_add_month_order_user_num,    
    SUM(case when (bb.third_add_time_order_user_flag = 'Y') then 1 else 0 end) as third_add_time_order_user_num,        --第三方新增按次订购用户数
    SUM(case when (bb.mon_third_add_time_order_user_flag = 'Y') then 1 else 0 end) as mon_third_add_time_order_user_num,    
    SUM(case when (bb.third_prepay_user_flag = 'Y') then 1 else 0 end) as third_prepay_user_num,                        --第三方预付用户数
    SUM(case when (bb.mon_third_prepay_user_flag = 'Y') then 1 else 0 end) as mon_third_prepay_user_num,    
    SUM(case when (bb.third_month_share_pay_user_flag = 'Y') then 1 else 0 end) as third_month_share_pay_user_num,      --第三方包月分摊付费用户数
    SUM(case when (bb.mon_third_month_share_pay_user_flag = 'Y') then 1 else 0 end) as mon_third_month_share_pay_user_num,    
    SUM(case when (bb.third_pay_user_flag = 'Y') then 1 else 0 end) as third_pay_user_num,                  --第三方付费用户数（包月分摊+按次） 
    SUM(case when (bb.mon_third_pay_user_flag = 'Y') then 1 else 0 end) as mon_third_pay_user_num,    
    SUM(case when (bb.add_month_order_user_flag = 'Y') then 1 else 0 end) as add_month_order_user_num,      --(boss+第三方)新增包周期订购用户数
    SUM(case when (bb.mon_add_month_order_user_flag = 'Y') then 1 else 0 end) as mon_add_month_order_user_num,    
    SUM(case when (bb.add_time_order_user_flag = 'Y') then 1 else 0 end) as add_time_order_user_num,        --(boss+第三方)新增按次订购用户数
    SUM(case when (bb.mon_add_time_order_user_flag = 'Y') then 1 else 0 end) as mon_add_time_order_user_num,    
    SUM(case when (bb.month_in_order_user_flag = 'Y') then 1 else 0 end) as month_in_order_user_num,        --(boss+第三方)包月在订用户数
    SUM(case when (bb.mon_month_in_order_user_flag = 'Y') then 1 else 0 end) as mon_month_in_order_user_num,      
    SUM(case when (bb.boss_pay_user_flag = 'Y' or bb.third_pay_user_flag ='Y') then 1 else 0 end) as pay_user_num,  --(boss+第三方)付费用户数
    SUM(case when (bb.mon_boss_pay_user_flag = 'Y' or bb.mon_third_pay_user_flag ='Y') then 1 else 0 end) as mon_pay_user_num,    
    SUM(case when (bb.boss_pay_user_flag = 'Y') then 1 else 0 end) as boss_pay_user_num,
    SUM(case when (bb.mon_boss_pay_user_flag = 'Y') then 1 else 0 end) as mon_boss_pay_user_num,
    SUM(case when (bb.boss_pay_active_user_flag = 'Y' and bb.active_visit_user_flag = 'Y') then 1 else 0 end) as boss_pay_active_user_num,
    SUM(case when (bb.mon_boss_pay_active_user_flag = 'Y' and bb.mon_active_visit_user_flag = 'Y') then 1 else 0 end) as mon_boss_pay_active_user_num,
    SUM(case when (bb.prov_develop_boss_add_order_user_flag = 'Y') then 1 else 0 end) as prov_develop_boss_add_order_user_num,
    SUM(case when (bb.mon_prov_develop_boss_add_order_user_flag = 'Y') then 1 else 0 end) as mon_prov_develop_boss_add_order_user_num,
    SUM(case when (bb.natural_develop_boss_add_order_user_flag = 'Y') then 1 else 0 end) as natural_develop_boss_add_order_user_num,
    SUM(case when (bb.mon_natural_develop_boss_add_order_user_flag = 'Y') then 1 else 0 end) as mon_natural_develop_boss_add_order_user_num,
    SUM(case when (bb.prov_develop_boss_month_in_order_user_flag = 'Y') then 1 else 0 end) as prov_develop_boss_month_in_order_user_num,
    SUM(case when (bb.mon_prov_develop_boss_month_in_order_user_flag = 'Y') then 1 else 0 end) as mon_prov_develop_boss_month_in_order_user_num,
    SUM(case when (bb.natural_develop_boss_month_in_order_user_flag = 'Y') then 1 else 0 end) as natural_develop_boss_month_in_order_user_num,
    SUM(case when (bb.mon_natural_develop_boss_month_in_order_user_flag = 'Y') then 1 else 0 end) as mon_natural_develop_boss_month_in_order_user_num,
    SUM(case when (bb.idea_pay_user_flag ='Y') then 1 else 0 end) as idea_pay_user_num,
    SUM(case when (bb.mon_idea_pay_user_flag = 'Y') then 1 else 0 end) as mon_idea_pay_user_num,
    cast(bb.dim_group_id as int) & 2147483647 as dim_group_id
from ( 
    select 
        tmp.dept_id as dept_id,
        tmp.term_prod_id as term_prod_id,
        tmp.term_video_type_id as term_video_type_id,
        tmp.term_video_soft_id as term_video_soft_id,
        tmp.term_prod_type_id as term_prod_type_id,
        tmp.term_version_id as term_version_id,
        tmp.term_os_type_id as term_os_type_id,
        tmp.busi_id as busi_id,
        tmp.sub_busi_id as sub_busi_id,
        tmp.net_type_id as net_type_id,
        tmp.cp_id as cp_id,
        tmp.product_id as product_id,
        tmp.content_id as content_id,
        tmp.content_type as content_type,
        tmp.broadcast_type_id as broadcast_type_id,
        tmp.user_type_id as user_type_id,
        tmp.province_id as province_id,
        tmp.city_id as city_id,
        tmp.phone_province_id as phone_province_id,
        tmp.phone_city_id as phone_city_id,
        tmp.company_id as company_id,
        tmp.chn_id as chn_id,
        tmp.chn_attr_1_id as chn_attr_1_id,
        tmp.chn_attr_2_id as chn_attr_2_id,
        tmp.chn_attr_3_id as chn_attr_3_id,
        tmp.chn_attr_4_id as chn_attr_4_id,
        tmp.chn_type as chn_type, 
        tmp.con_class_1_name as con_class_1_name,
        tmp.copyright_type as copyright_type,
        tmp.busi_type_id as busi_type_id,
        tmp.program_id as program_id,
        tmp.user_id as user_id,
        MAX(tmp.boss_time_add_order_user_flag) as boss_time_add_order_user_flag,
        MAX(tmp.mon_boss_time_add_order_user_flag) as mon_boss_time_add_order_user_flag,
        MAX(tmp.boss_month_in_order_user_flag) as boss_month_in_order_user_flag,
        MAX(tmp.mon_boss_month_in_order_user_flag) as mon_boss_month_in_order_user_flag,
        MAX(tmp.boss_month_add_order_user_flag) as boss_month_add_order_user_flag,
        MAX(tmp.mon_boss_month_add_order_user_flag) as mon_boss_month_add_order_user_flag,
        MAX(tmp.boss_month_cancel_order_user_flag) as boss_month_cancel_order_user_flag,
        MAX(tmp.mon_boss_month_cancel_order_user_flag) as mon_boss_month_cancel_order_user_flag,
        --MAX(tmp.boss_month_add_and_cancel_user_flag) as boss_month_add_and_cancel_user_flag,
        --MAX(tmp.mon_boss_month_add_and_cancel_user_flag) as mon_boss_month_add_and_cancel_user_flag,
        --MAX(tmp.boss_month_pay_user_flag) as boss_month_pay_user_flag,
        --MAX(tmp.mon_boss_month_pay_user_flag) as mon_boss_month_pay_user_flag,                
        MAX(tmp.third_add_month_order_user_flag) as third_add_month_order_user_flag,
        MAX(tmp.mon_third_add_month_order_user_flag) as mon_third_add_month_order_user_flag,        
        MAX(tmp.third_add_time_order_user_flag) as third_add_time_order_user_flag,
        MAX(tmp.mon_third_add_time_order_user_flag) as mon_third_add_time_order_user_flag,        
        MAX(tmp.third_prepay_user_flag) as third_prepay_user_flag,
        MAX(tmp.mon_third_prepay_user_flag) as mon_third_prepay_user_flag,        
        MAX(tmp.third_month_share_pay_user_flag) as third_month_share_pay_user_flag,
        MAX(tmp.mon_third_month_share_pay_user_flag) as mon_third_month_share_pay_user_flag,        
        MAX(tmp.third_pay_user_flag) as third_pay_user_flag,
        MAX(tmp.mon_third_pay_user_flag) as mon_third_pay_user_flag,        
        MAX(tmp.add_month_order_user_flag) as add_month_order_user_flag,
        MAX(tmp.mon_add_month_order_user_flag) as mon_add_month_order_user_flag,        
        MAX(tmp.add_time_order_user_flag) as add_time_order_user_flag,
        MAX(tmp.mon_add_time_order_user_flag) as mon_add_time_order_user_flag,        
        MAX(tmp.month_in_order_user_flag) as month_in_order_user_flag,
        MAX(tmp.mon_month_in_order_user_flag) as mon_month_in_order_user_flag,
        --MAX(tmp.pay_user_flag) as pay_user_flag,
        --MAX(tmp.mon_pay_user_flag) as mon_pay_user_flag,                
        MAX(tmp.boss_pay_user_flag) as boss_pay_user_flag,
        MAX(tmp.mon_boss_pay_user_flag) as mon_boss_pay_user_flag,
        MAX(tmp.boss_pay_active_user_flag) as boss_pay_active_user_flag,
        MAX(tmp.mon_boss_pay_active_user_flag) as mon_boss_pay_active_user_flag,
        MAX(tmp.prov_develop_boss_add_order_user_flag) as prov_develop_boss_add_order_user_flag,
        MAX(tmp.mon_prov_develop_boss_add_order_user_flag) as mon_prov_develop_boss_add_order_user_flag,
        MAX(tmp.natural_develop_boss_add_order_user_flag) as natural_develop_boss_add_order_user_flag,
        MAX(tmp.mon_natural_develop_boss_add_order_user_flag) as mon_natural_develop_boss_add_order_user_flag,
        MAX(tmp.prov_develop_boss_month_in_order_user_flag) as prov_develop_boss_month_in_order_user_flag,
        MAX(tmp.mon_prov_develop_boss_month_in_order_user_flag) as mon_prov_develop_boss_month_in_order_user_flag,
        MAX(tmp.natural_develop_boss_month_in_order_user_flag) as natural_develop_boss_month_in_order_user_flag,
        MAX(tmp.mon_natural_develop_boss_month_in_order_user_flag) as mon_natural_develop_boss_month_in_order_user_flag,
        MAX(tmp.idea_pay_user_flag) as idea_pay_user_flag,
        MAX(tmp.mon_idea_pay_user_flag) as mon_idea_pay_user_flag,
        MAX(tmp.active_visit_user_flag) as active_visit_user_flag,
        MAX(tmp.mon_active_visit_user_flag) as mon_active_visit_user_flag,
        grouping__id as dim_group_id
    from (
            select 
                CASE WHEN e.term_prod_name IN ('和视频OPENAPI','和视频SDK') THEN nvl(d.dept_id,'-998') ELSE nvl(b.dept_id,nvl(c.dept_id,nvl(d.dept_id,'-998'))) END as dept_id,
                a.term_prod_id as term_prod_id,
                nvl(e.term_video_type_id, '-998') as term_video_type_id,
                nvl(e.term_video_soft_id, '-998') as term_video_soft_id,
                nvl(e.term_prod_type_id, '-998') as term_prod_type_id,
                a.term_version_id as term_version_id,
                nvl(e.term_os_type_id, '-998') as term_os_type_id,
                nvl(h.business_id, '-998') as busi_id,
                a.sub_busi_id as sub_busi_id,
                a.network_type as net_type_id,
                a.cp_id as cp_id,
                a.product_id as product_id,
                '-999' as content_id,
                '-999' as content_type,
                a.use_type as broadcast_type_id,
                a.user_type as user_type_id,
                nvl(f.prov_id, '-998') as province_id,
                a.city_id as city_id,
                nvl(f.prov_id, '-998') as phone_province_id,
                a.city_id as phone_city_id,
                a.company_id as company_id,
                nvl(g.chn_id, '-998') as chn_id,
                nvl(g.chn_attr_1_id, '-998') as chn_attr_1_id,
                nvl(g.chn_attr_2_id, '-998') as chn_attr_2_id,
                nvl(g.chn_attr_3_id, '-998') as chn_attr_3_id,
                nvl(g.chn_attr_4_id, '-998') as chn_attr_4_id,
                nvl(g.chn_type, '-998') as chn_type,
                '-998' as con_class_1_name,
                '-998' as copyright_type,
                nvl(h.b_type_id, '-998') as busi_type_id,
                a.program_id as program_id,
                a.serv_number as user_id,
                case when a.src_file_day='${SRC_FILE_DAY}' and (a.period_unit = 'HOUR' and a.pay_chn_type in (50,58,59,302,304,306) and a.order_status = 5 and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')) then 'Y' else 'N' end as boss_time_add_order_user_flag,
                case when (a.period_unit = 'HOUR' and a.pay_chn_type in (50,58,59,302,304,306) and a.order_status = 5 and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')) then 'Y' else 'N' end as mon_boss_time_add_order_user_flag,
                'N' as boss_month_in_order_user_flag,
                'N' as mon_boss_month_in_order_user_flag,
                case when a.src_file_day='${SRC_FILE_DAY}' and (a.authorize_type = 'BOSS_MONTH' and a.order_status = 5 and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')) then 'Y' else 'N' end as boss_month_add_order_user_flag,
                case when (a.authorize_type = 'BOSS_MONTH' and a.order_status = 5 and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')) then 'Y' else 'N' end as mon_boss_month_add_order_user_flag,
                case when a.src_file_day='${SRC_FILE_DAY}' and (a.authorize_type = 'BOSS_MONTH' and a.order_status in (12,14,15)) then 'Y' else 'N' end as boss_month_cancel_order_user_flag,
                case when (a.authorize_type = 'BOSS_MONTH' and a.order_status in (12,14,15)) then 'Y' else 'N' end as mon_boss_month_cancel_order_user_flag,
                --'N' as boss_month_add_and_cancel_user_flag,
                --case when () then else end as mon_boss_month_add_and_cancel_user_flag,
                --'N' as boss_month_pay_user_flag,
                --'N' as mon_boss_month_pay_user_flag,                
                case when a.src_file_day='${SRC_FILE_DAY}' and (a.period_unit <> 'HOUR' and a.pay_chn_type not in (50,58,59,302,304,306) and a.order_status in ('5','9') and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')) then 'Y' else 'N' end as third_add_month_order_user_flag,
                case when (a.period_unit <> 'HOUR' and a.pay_chn_type not in (50,58,59,302,304,306) and a.order_status in ('5','9') and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')) then 'Y' else 'N' end as mon_third_add_month_order_user_flag,
                case when a.src_file_day='${SRC_FILE_DAY}' and (a.period_unit = 'HOUR' and a.pay_chn_type not in (50,58,59,302,304,306) and a.order_status in ('5','9') and  a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')) then 'Y' else 'N' end as third_add_time_order_user_flag,
                case when (a.period_unit = 'HOUR' and a.pay_chn_type not in (50,58,59,302,304,306) and a.order_status in ('5','9') and  a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')) then 'Y' else 'N' end as mon_third_add_time_order_user_flag,
                case when a.src_file_day='${SRC_FILE_DAY}' and (a.pay_chn_type not in (50,58,59,302,304,306) and a.order_status in ('5','9') and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')) then 'Y' else 'N' end as third_prepay_user_flag,
                case when (a.pay_chn_type not in (50,58,59,302,304,306) and a.order_status in ('5','9') and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')) then 'Y' else 'N' end as mon_third_prepay_user_flag,
                'N' as third_month_share_pay_user_flag,
                'N' as mon_third_month_share_pay_user_flag,
                case when a.src_file_day = '${SRC_FILE_DAY}' and  a.period_unit = 'hour'   
                      and a.main = 0 and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE') and a.pay_chn_type not in (50,58,59,302,304,306) and a.order_status in ('5','9') 
                     then 'Y' else 'N' end as third_pay_user_flag,        --天。第三方付费用户（包月分摊+按次）之按次  指标28    
                case when a.period_unit = 'hour'  
                      and a.main = 0 and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE') and a.pay_chn_type not in (50,58,59,302,304,306) and a.order_status in ('5','9') 
                     then 'Y' else 'N' end as mon_third_pay_user_flag,    --月。第三方付费用户（包月分摊+按次）之按次  
                case when a.src_file_day='${SRC_FILE_DAY}' and (a.period_unit = 'MONTH' and a.order_status in ('5','9') and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')) then 'Y' else 'N' end as add_month_order_user_flag,
                case when (a.period_unit = 'MONTH' and a.order_status in ('5','9') and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')) then 'Y' else 'N' end as mon_add_month_order_user_flag,
                case when a.src_file_day='${SRC_FILE_DAY}' and (a.period_unit = 'HOUR' and a.order_status in ('5','9') and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')) then 'Y' else 'N' end as add_time_order_user_flag,
                case when (a.period_unit = 'HOUR' and a.order_status in ('5','9') and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')) then 'Y' else 'N' end as mon_add_time_order_user_flag,
                'N' as month_in_order_user_flag,
                'N' as mon_month_in_order_user_flag,
                --'N' as pay_user_flag,
                --'N' as mon_pay_user_flag,                
                case when (a.src_file_day='${SRC_FILE_DAY}' and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE') and a.main = 0 and a.pay_chn_type in (50,58,59,302,304,306) and a.order_status = 5) then 'Y' else 'N' end as boss_pay_user_flag,
                case when (a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE') and a.main = 0 and a.pay_chn_type in (50,58,59,302,304,306) and a.order_status = 5) then 'Y' else 'N' end as mon_boss_pay_user_flag,
                case when (a.src_file_day='${SRC_FILE_DAY}'and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE') and a.main = 0 and a.pay_chn_type in (50,58,59,302,304,306) and a.order_status = 5) then 'Y' else 'N' end as boss_pay_active_user_flag,
                case when (a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE') and a.main = 0 and a.pay_chn_type in (50,58,59,302,304,306) and a.order_status = 5) then 'Y' else 'N' end  as mon_boss_pay_active_user_flag,
                case when a.src_file_day='${SRC_FILE_DAY}' and (a.authorize_type = 'BOSS_MONTH' and a.order_status = 5 and g.chn_attr_2_name = '省公司') then 'Y' else 'N' end as prov_develop_boss_add_order_user_flag,
                case when (a.authorize_type = 'BOSS_MONTH' and a.order_status = 5 and g.chn_attr_2_name = '省公司') then 'Y' else 'N' end as mon_prov_develop_boss_add_order_user_flag,
                case when a.src_file_day='${SRC_FILE_DAY}' and (a.authorize_type = 'BOSS_MONTH' and a.order_status = 5 and g.chn_attr_2_name <> '省公司') then 'Y' else 'N' end as natural_develop_boss_add_order_user_flag,
                case when (a.authorize_type = 'BOSS_MONTH' and a.order_status = 5 and g.chn_attr_2_name <> '省公司') then 'Y' else 'N' end as mon_natural_develop_boss_add_order_user_flag,
                'N' as prov_develop_boss_month_in_order_user_flag,
                'N' as mon_prov_develop_boss_month_in_order_user_flag,
                'N' as natural_develop_boss_month_in_order_user_flag,
                'N' as mon_natural_develop_boss_month_in_order_user_flag,
                case when (a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE') and a.main = 0 and a.pay_chn_type in (50,58,59,302,304,306) and a.order_status = 5) then 'Y' else 'N' end  as idea_pay_user_flag,
                case when (a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE') and a.main = 0 and a.pay_chn_type in (50,58,59,302,304,306) and a.order_status = 5) then 'Y' else 'N' end  as mon_idea_pay_user_flag,
                'N' as active_visit_user_flag,
                'N' as mon_active_visit_user_flag
            from rptdata.fact_ugc_order_detail_daily a
            left outer join rptdata.dim_server h
            on a.sub_busi_id = h.sub_busi_id --and h.dw_delete_flag = 'N'
            left outer join rptdata.dim_dept_term_prod b --<部门-终端产品维表> b
            on a.term_prod_id = b.term_prod_id  --and b.dw_delete_flag = 'N'
            left outer join rptdata.dim_dept_busi c --<部门-业务维表> c
            on h.business_id = c.business_id --and c.dw_delete_flag = 'N'
            left outer join rptdata.dim_dept_chn d --<部门-渠道维表> d
            on a.chn_id = d.chn_id --and d.dw_delete_flag = 'N'
            left outer join rpt.dim_term_prod_v e
            on a.term_prod_id = e.term_prod_id --and e.dw_delete_flag = 'N'
            left outer join rptdata.dim_region f
            on a.city_id = f.region_id --and f.dw_delete_flag = 'N'
            left outer join rptdata.dim_chn g
            on a.chn_id = g.chn_id --and g.dw_delete_flag = 'N'
            where a.src_file_day>='${SRC_FILE_MONTH}01' and a.src_file_day<='${SRC_FILE_DAY}'
            
            union all
            
            select 
                CASE WHEN e.term_prod_name IN ('和视频OPENAPI','和视频SDK') THEN nvl(d.dept_id,'-998') ELSE nvl(b.dept_id,nvl(c.dept_id,nvl(d.dept_id,'-998'))) END as dept_id,
                a.term_prod_id as term_prod_id,
                nvl(e.term_video_type_id, '-998') as term_video_type_id,
                nvl(e.term_video_soft_id, '-998') as term_video_soft_id,
                nvl(e.term_prod_type_id, '-998') as term_prod_type_id,
                a.term_version_id as term_version_id,
                nvl(e.term_os_type_id, '-998') as term_os_type_id,
                nvl(h.business_id, '-998') as busi_id,
                a.sub_busi_id as sub_busi_id,
                a.net_type_id as net_type_id,
                a.cp_id as cp_id,
                a.product_id as product_id,
                '-999' as content_id,
                '-999' as content_type,
                a.broadcast_type_id as broadcast_type_id,
                a.user_type_id as user_type_id,
                nvl(f.prov_id, '-998') as province_id,
                a.city_id as city_id,
                nvl(f.prov_id, '-998') as phone_province_id,
                a.city_id as phone_city_id,
                a.company_id as company_id,
                nvl(g.chn_id, '-998') as chn_id,
                nvl(g.chn_attr_1_id, '-998') as chn_attr_1_id,
                nvl(g.chn_attr_2_id, '-998') as chn_attr_2_id,
                nvl(g.chn_attr_3_id, '-998') as chn_attr_3_id,
                nvl(g.chn_attr_4_id, '-998') as chn_attr_4_id,
                nvl(g.chn_type, '-998') as chn_type,
                '-998' as con_class_1_name,
                '-998' as copyright_type,
                nvl(h.b_type_id, '-998') as busi_type_id,
                '-998' as program_id,
                a.user_id as user_id,
                'N' as boss_time_add_order_user_flag,
                'N' as mon_boss_time_add_order_user_flag,
                'N' as boss_month_in_order_user_flag,
                'N' as mon_boss_month_in_order_user_flag,
                'N' as boss_month_add_order_user_flag,
                'N' as mon_boss_month_add_order_user_flag,
                'N' as boss_month_cancel_order_user_flag,
                'N' as mon_boss_month_cancel_order_user_flag,
                --'N' as boss_month_add_and_cancel_user_flag,
                --case when () then else end as mon_boss_month_add_and_cancel_user_flag,
                --'N' as boss_month_pay_user_flag,
                --'N' as mon_boss_month_pay_user_flag,                
                'N' as third_add_month_order_user_flag,
                'N' as mon_third_add_month_order_user_flag,
                'N' as third_add_time_order_user_flag,
                'N' as mon_third_add_time_order_user_flag,
                'N' as third_prepay_user_flag,
                'N' as mon_third_prepay_user_flag,
                'N' as third_month_share_pay_user_flag,
                'N' as mon_third_month_share_pay_user_flag,
                'N' as third_pay_user_flag,
                'N' as mon_third_pay_user_flag,
                'N' as add_month_order_user_flag,
                'N' as mon_add_month_order_user_flag,
                'N' as add_time_order_user_flag,
                'N' as mon_add_time_order_user_flag,
                'N' as month_in_order_user_flag,
                'N' as mon_month_in_order_user_flag,
                --'N' as pay_user_flag,
                --'N' as mon_pay_user_flag,                
                'N' as boss_pay_user_flag,
                'N' as mon_boss_pay_user_flag,
                'N' as boss_pay_active_user_flag,
                'N' as mon_boss_pay_active_user_flag,
                'N' as prov_develop_boss_add_order_user_flag,
                'N' as mon_prov_develop_boss_add_order_user_flag,
                'N' as natural_develop_boss_add_order_user_flag,
                'N' as mon_natural_develop_boss_add_order_user_flag,
                'N' as prov_develop_boss_month_in_order_user_flag,
                'N' as mon_prov_develop_boss_month_in_order_user_flag,
                'N' as natural_develop_boss_month_in_order_user_flag,
                'N' as mon_natural_develop_boss_month_in_order_user_flag,
                case when (a.chrgprod_price > 0 and a.order_status <> '4' and a.sub_status is null) then 'Y' else 'N' end as idea_pay_user_flag,
                case when (a.chrgprod_price > 0 and a.order_status <> '4' and a.sub_status is null) then 'Y' else 'N' end as mon_idea_pay_user_flag,
                'N' as active_visit_user_flag,
                'N' as mon_active_visit_user_flag
            from rptdata.fact_ugc_order_relation_detail_daily a
            left outer join rptdata.dim_server h
            on a.sub_busi_id = h.sub_busi_id --and h.dw_delete_flag = 'N'
            left outer join rptdata.dim_dept_term_prod b --<部门-终端产品维表> b
            on a.term_prod_id = b.term_prod_id  --and b.dw_delete_flag = 'N'
            left outer join rptdata.dim_dept_busi c --<部门-业务维表> c
            on h.business_id = c.business_id --and c.dw_delete_flag = 'N'
            left outer join rptdata.dim_dept_chn d --<部门-渠道维表> d
            on a.chn_id = d.chn_id --and d.dw_delete_flag = 'N'
            left outer join rpt.dim_term_prod_v e
            on a.term_prod_id = e.term_prod_id --and e.dw_delete_flag = 'N'
            left outer join rptdata.dim_region f
            on a.city_id = f.region_id --and f.dw_delete_flag = 'N'
            left outer join rptdata.dim_chn g
            on a.chn_id = g.chn_id --and g.dw_delete_flag = 'N'
            where a.src_file_day='${SRC_FILE_END_MONTH_DAY}'  ----统计上月底快照
            
            union all
            
            select 
                CASE WHEN e.term_prod_name IN ('和视频OPENAPI','和视频SDK') THEN nvl(d.dept_id,'-998') ELSE nvl(b.dept_id,nvl(c.dept_id,nvl(d.dept_id,'-998'))) END as dept_id,
                a.term_prod_id as term_prod_id,
                nvl(e.term_video_type_id, '-998') as term_video_type_id,
                nvl(e.term_video_soft_id, '-998') as term_video_soft_id,
                nvl(e.term_prod_type_id, '-998') as term_prod_type_id,
                a.term_version_id as term_version_id,
                nvl(e.term_os_type_id, '-998') as term_os_type_id,
                nvl(h.business_id, '-998') as busi_id,
                a.sub_busi_id as sub_busi_id,
                a.net_type_id as net_type_id,
                a.cp_id as cp_id,
                a.product_id as product_id,
                '-999' as content_id,
                '-999' as content_type,
                a.broadcast_type_id as broadcast_type_id,
                a.user_type_id as user_type_id,
                nvl(f.prov_id, '-998') as province_id,
                a.city_id as city_id,
                nvl(f.prov_id, '-998') as phone_province_id,
                a.city_id as phone_city_id,
                a.company_id as company_id,
                nvl(g.chn_id, '-998') as chn_id,
                nvl(g.chn_attr_1_id, '-998') as chn_attr_1_id,
                nvl(g.chn_attr_2_id, '-998') as chn_attr_2_id,
                nvl(g.chn_attr_3_id, '-998') as chn_attr_3_id,
                nvl(g.chn_attr_4_id, '-998') as chn_attr_4_id,
                nvl(g.chn_type, '-998') as chn_type,
                '-998' as con_class_1_name,
                '-998' as copyright_type,
                nvl(h.b_type_id, '-998') as busi_type_id,
                '-998' as program_id,
                a.user_id as user_id,
                'N' as boss_time_add_order_user_flag,
                'N' as mon_boss_time_add_order_user_flag,
                case when (a.src_file_day='${SRC_FILE_DAY}' and a.chrgprod_price > 0 and a.order_status <> '4' and a.sub_status is null) then 'Y' else 'N' end as boss_month_in_order_user_flag,
                case when (a.chrgprod_price > 0 and a.order_status <> '4' and a.sub_status is null) then 'Y' else 'N' end as mon_boss_month_in_order_user_flag,
                'N' as boss_month_add_order_user_flag,
                'N' as mon_boss_month_add_order_user_flag,
                'N' as boss_month_cancel_order_user_flag,
                'N' as mon_boss_month_cancel_order_user_flag,
                --'N' as boss_month_add_and_cancel_user_flag,
                --case when () then else end as mon_boss_month_add_and_cancel_user_flag,
                --'N' as boss_month_pay_user_flag,
                --'N' as mon_boss_month_pay_user_flag,                
                'N' as third_add_month_order_user_flag,
                'N' as mon_third_add_month_order_user_flag,
                'N' as third_add_time_order_user_flag,
                'N' as mon_third_add_time_order_user_flag,
                'N' as third_prepay_user_flag,
                'N' as mon_third_prepay_user_flag,
                'N' as third_month_share_pay_user_flag,
                'N' as mon_third_month_share_pay_user_flag,
                'N' as third_pay_user_flag,
                'N' as mon_third_pay_user_flag,
                'N' as add_month_order_user_flag,
                'N' as mon_add_month_order_user_flag,
                'N' as add_time_order_user_flag,
                'N' as mon_add_time_order_user_flag,
                case when (a.src_file_day='${SRC_FILE_DAY}' and a.chrgprod_price > 0 and a.order_status <> '4' and a.sub_status is null) then 'Y' else 'N' end as month_in_order_user_flag, ----天。boss+第三方包月在订用户(boss) 
                case when (a.chrgprod_price > 0 and a.order_status <> '4' and a.sub_status is null) then 'Y' else 'N' end as mon_month_in_order_user_flag,
                --'N' as pay_user_flag,
                --'N' as mon_pay_user_flag,
                case when (a.src_file_day='${SRC_FILE_DAY}' and a.chrgprod_price > 0 and a.order_status <> '4' and a.sub_status is null) then 'Y' else 'N' end as boss_pay_user_flag,
                case when (a.chrgprod_price > 0 and a.order_status <> '4' and a.sub_status is null) then 'Y' else 'N' end as mon_boss_pay_user_flag,
                case when (a.src_file_day='${SRC_FILE_DAY}' and a.chrgprod_price > 0 and a.order_status <> '4' and a.sub_status is null) then 'Y' else 'N' end as boss_pay_active_user_flag,
                case when (a.chrgprod_price > 0 and a.order_status <> '4' and a.sub_status is null) then 'Y' else 'N' end as mon_boss_pay_active_user_flag,
                'N' as prov_develop_boss_add_order_user_flag,
                'N' as mon_prov_develop_boss_add_order_user_flag,
                'N' as natural_develop_boss_add_order_user_flag,
                'N' as mon_natural_develop_boss_add_order_user_flag,
                case when (a.src_file_day='${SRC_FILE_DAY}' and a.chrgprod_price > 0 and a.order_status <> '4' and a.sub_status is null and g.chn_attr_2_name = '省公司') then 'Y' else 'N' end as prov_develop_boss_month_in_order_user_flag,
                case when (a.chrgprod_price > 0 and a.order_status <> '4' and a.sub_status is null and g.chn_attr_2_name = '省公司') then 'Y' else 'N' end as mon_prov_develop_boss_month_in_order_user_flag,
                case when (a.src_file_day='${SRC_FILE_DAY}' and a.chrgprod_price > 0 and a.order_status <> '4' and a.sub_status is null and g.chn_attr_2_name <> '省公司') then 'Y' else 'N' end as natural_develop_boss_month_in_order_user_flag,
                case when (a.chrgprod_price > 0 and a.order_status <> '4' and a.sub_status is null and g.chn_attr_2_name <> '省公司') then 'Y' else 'N' end as mon_natural_develop_boss_month_in_order_user_flag,
                'N' as idea_pay_user_flag,
                'N' as mon_idea_pay_user_flag,
                'N' as active_visit_user_flag,
                'N' as mon_active_visit_user_flag
            from rptdata.fact_ugc_order_relation_detail_daily a
            left outer join rptdata.dim_server h
            on a.sub_busi_id = h.sub_busi_id --and h.dw_delete_flag = 'N'
            left outer join rptdata.dim_dept_term_prod b --<部门-终端产品维表> b
            on a.term_prod_id = b.term_prod_id  --and b.dw_delete_flag = 'N'
            left outer join rptdata.dim_dept_busi c --<部门-业务维表> c
            on h.business_id = c.business_id --and c.dw_delete_flag = 'N'
            left outer join rptdata.dim_dept_chn d --<部门-渠道维表> d
            on a.chn_id = d.chn_id --and d.dw_delete_flag = 'N'
            left outer join rpt.dim_term_prod_v e
            on a.term_prod_id = e.term_prod_id --and e.dw_delete_flag = 'N'
            left outer join rptdata.dim_region f
            on a.city_id = f.region_id --and f.dw_delete_flag = 'N'
            left outer join rptdata.dim_chn g
            on a.chn_id = g.chn_id --and g.dw_delete_flag = 'N'
            where a.src_file_day='${SRC_FILE_DAY}'
            
            union all   --第三方(在订+包月分摊)
                        
         select CASE WHEN e.term_prod_name IN ('和视频OPENAPI','和视频SDK') THEN nvl(d.dept_id,'-998') ELSE nvl(b.dept_id,nvl(c.dept_id,nvl(d.dept_id,'-998'))) END as dept_id,
                a.term_prod_id as term_prod_id,
                nvl(e.term_video_type_id, '-998') as term_video_type_id,
                nvl(e.term_video_soft_id, '-998') as term_video_soft_id,
                nvl(e.term_prod_type_id, '-998') as term_prod_type_id,
                a.term_version_id as term_version_id,
                nvl(e.term_os_type_id, '-998') as term_os_type_id,
                nvl(h.business_id, '-998') as busi_id,
                a.sub_busi_id as sub_busi_id,
                a.network_type as net_type_id,
                a.cp_id as cp_id,
                a.product_id as product_id,
                '-999' as content_id,
                '-999' as content_type,
                a.use_type as broadcast_type_id,
                a.user_type as user_type_id,
                nvl(f.prov_id, '-998') as province_id,
                a.city_id as city_id,
                nvl(f.prov_id, '-998') as phone_province_id,
                a.city_id as phone_city_id,
                a.company_id as company_id,
                nvl(g.chn_id, '-998') as chn_id,
                nvl(g.chn_attr_1_id, '-998') as chn_attr_1_id,
                nvl(g.chn_attr_2_id, '-998') as chn_attr_2_id,
                nvl(g.chn_attr_3_id, '-998') as chn_attr_3_id,
                nvl(g.chn_attr_4_id, '-998') as chn_attr_4_id,
                nvl(g.chn_type, '-998') as chn_type,
                '-998' as con_class_1_name,
                '-998' as copyright_type,
                nvl(h.b_type_id, '-998') as busi_type_id,
                a.program_id as program_id,
                a.serv_number as user_id,
               'N' as boss_time_add_order_user_flag,
               'N' as mon_boss_time_add_order_user_flag,
               'N' as boss_month_in_order_user_flag,
               'N' as mon_boss_month_in_order_user_flag,
               'N' as boss_month_add_order_user_flag,
               'N' as mon_boss_month_add_order_user_flag,
               'N' as boss_month_cancel_order_user_flag,
               'N' as mon_boss_month_cancel_order_user_flag,
               --'N' as boss_month_add_and_cancel_user_flag,
               --case when () then else end as mon_boss_month_add_and_cancel_user_flag,
               --'N' as boss_month_pay_user_flag,
               --'N' as mon_boss_month_pay_user_flag,              
               'N' as third_add_month_order_user_flag,
               'N' as mon_third_add_month_order_user_flag,
               'N' as third_add_time_order_user_flag,
               'N' as mon_third_add_time_order_user_flag,
               'N' as third_prepay_user_flag,
               'N' as mon_third_prepay_user_flag,               
               case when a.src_file_day = '${SRC_FILE_DAY}' and a.expire_time >= unix_timestamp('${SRC_FILE_DAY}','yyyyMMdd') and a.valid_start_time <= unix_timestamp('${SRC_FILE_DAY}','yyyyMMdd') 
                    then 'Y' else 'N' end as third_month_share_pay_user_flag, --第三方包月分摊付费用户
               case when a.expire_time >= '${SRC_FILE_DAY}' and a.valid_start_time <= '${SRC_FILE_DAY}' 
                    then 'Y' else 'N' end as mon_third_month_share_pay_user_flag,                   
               case when a.src_file_day = '${SRC_FILE_DAY}' and a.expire_time >= unix_timestamp('${SRC_FILE_DAY}','yyyyMMdd') and a.valid_start_time <= unix_timestamp('${SRC_FILE_DAY}','yyyyMMdd') 
                    then 'Y' else 'N' end as third_pay_user_flag,      --天。第三方付费用户（包月分摊+按次）之包月分摊
               case when a.expire_time >= unix_timestamp('${SRC_FILE_DAY}','yyyyMMdd') and a.valid_start_time <= unix_timestamp('${SRC_FILE_DAY}','yyyyMMdd')
                    then 'Y' else 'N' end as mon_third_pay_user_flag,                       
               'N' as add_month_order_user_flag,
               'N' as mon_add_month_order_user_flag,
               'N' as add_time_order_user_flag,
               'N' as mon_add_time_order_user_flag,
               case when a.src_file_day = '${SRC_FILE_DAY}' and a.expire_time >= unix_timestamp('${SRC_FILE_DAY}','yyyyMMdd') and a.valid_start_time <= unix_timestamp('${SRC_FILE_DAY}','yyyyMMdd')
                    then 'Y' else 'N' end as month_in_order_user_flag,  --天。boss+第三方包月在订用户(第三方)   
               case when a.expire_time >= unix_timestamp('${SRC_FILE_DAY}','yyyyMMdd') and a.valid_start_time <= unix_timestamp('${SRC_FILE_DAY}','yyyyMMdd')
                    then 'Y' else 'N' end as mon_month_in_order_user_flag,  --月。boss+第三方包月在订用户(第三方)  
               --'N' as pay_user_flag,
               --'N' as mon_pay_user_flag,               
               'N' as boss_pay_user_flag,
               'N' as mon_boss_pay_user_flag,
               'N' as boss_pay_active_user_flag,
               'N' as mon_boss_pay_active_user_flag,
               'N' as prov_develop_boss_add_order_user_flag,
               'N' as mon_prov_develop_boss_add_order_user_flag,
               'N' as natural_develop_boss_add_order_user_flag,
               'N' as mon_natural_develop_boss_add_order_user_flag,
               'N' as prov_develop_boss_month_in_order_user_flag,
               'N' as mon_prov_develop_boss_month_in_order_user_flag,
               'N' as natural_develop_boss_month_in_order_user_flag,
               'N' as mon_natural_develop_boss_month_in_order_user_flag,
               'N' as idea_pay_user_flag,
               'N' as mon_idea_pay_user_flag,
               'N' as active_visit_user_flag,
               'N' as mon_active_visit_user_flag                                              
          from rptdata.fact_user_order_relation_detail a  
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
                       
    ) tmp
    group by dept_id,term_prod_id,term_video_type_id,term_video_soft_id,term_prod_type_id,term_version_id,term_os_type_id,busi_id,sub_busi_id,
        net_type_id,cp_id,product_id,content_id,content_type,broadcast_type_id,user_type_id,province_id,city_id,phone_province_id,phone_city_id,
        company_id,chn_id,chn_attr_1_id,chn_attr_2_id,chn_attr_3_id,chn_attr_4_id,chn_type, con_class_1_name,copyright_type,busi_type_id, program_id,
        user_id
    grouping sets (
                                    -- 核心
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
    company_id,chn_id,chn_attr_1_id,chn_attr_2_id,chn_attr_3_id,chn_attr_4_id,chn_type, con_class_1_name,copyright_type, busi_type_id, program_id, dim_group_id
;