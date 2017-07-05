
create table cdmpview.tmp_dy_05_assess_1_add_revenue (
statis_month string,
business_id string,
accu_add_revenue bigint
)
partitioned by (src_file_month string)


insert overwrite table cdmpview.tmp_dy_05_assess_1_add_revenue partition (src_file_month = '${SRC_FILE_MONTH}')  
select t.statis_month
      ,t.business_id
      ,sum(t.accu_add_revenue) as accu_add_revenue
  from (
        select '${SRC_FILE_MONTH}' as statis_month
              ,sb.business_id
              ,sum(a.payment_amt) as accu_add_revenue
          from rptdata.fact_order_item_detail a
          join rptdata.dim_sub_busi sb
            on a.sub_business_id = sb.sub_busi_id
         where substr(a.src_file_day, 1, 6) = '${SRC_FILE_MONTH}'  --当月  201707
           and a.order_status in (5,9)
         group by sb.business_id
         
         union all
         
        select a.statis_month
              ,a.business_id
              ,a.accu_add_revenue
          from cdmpview.tmp_dy_05_assess_1_add_revenue a
         where (substr('${SRC_FILE_MONTH}', 5, 2) <> '04' and a.src_file_month = '${LAST_MONTH}') --当月不是4月份时， 取上月的累计新增收入
       ) t                   
 group by t.statis_month, t.business_id
 
 
 
