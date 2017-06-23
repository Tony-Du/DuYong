

-- 统计周期：月 (未完待续)

with third_part_info_month as
(
select a.order_id
      ,a.order_user_id
      ,bb.period_unit
      ,bb.main
      ,cc.currency
      ,cc.chn_type
      ,cc.pay_amount
  from intdata.ugc_order_daily a
 inner join (   
            select b.order_id
                  ,b.period_unit
                  ,b.main
              from intdata.ugc_order_item_daily b
             where b.src_file_day >= '${MONTH_START_DAY}'
               and b.src_file_day <= '${MONTH_END_DAY}'
            ) bb
    on a.order_id = bb.order_id 
 inner join (
            select c.order_id
                  ,c.currency
                  ,c.chn_type
                  ,c.pay_amount
              from intdata.ugc_order_paychannel_daily c
             where c.src_file_day >= '${MONTH_START_DAY}'
               and c.src_file_day <= '${MONTH_END_DAY}'
               and c.chn_type not in (50,58,59,302,304,306) 
            ) cc
    on a.order_id = cc.order_id       
 where a.src_file_day >= '${MONTH_START_DAY}'
   and a.src_file_day <= '${MONTH_END_DAY}'
   and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')
   and a.order_status = 5
)

select t1.period_unit
      ,t1.currency
      ,t1.chn_type
      ,sum(t1.pay_amount)
      ,count(distinct t1.user_id)
  from (
        select t.period_unit, t.currency, t.chn_type
               case when t.period_unit <> 'hour' and t.main = 0 then t.pay_amount else 0 end pay_amount --第三方新增包周期收入
              ,case when t.period_unit = 'month' then t.order_user_id else '' end user_id   --第三方新增包周期订购用户数
          from third_part_info_month t
       ) t1
 group by period_unit, currency, chn_type
grouping sets (period_unit, currency, chn_type)

 union all

select 'hour' as period_unit
      ,t2.currency
      ,t2.chn_type
      ,sum(t2.pay_amount)
      ,count(distinct t2.user_id)
  from (
        select t.currency, t.chn_type
               case when t.period_unit = 'hour' and t.main = 0 then t.pay_amount else 0 end pay_amount  --第三方新增按次收入
              ,case when t.period_unit = 'hour' then t.order_user_id else '' end user_id        --第三方新增按次订购用户数
          from third_part_info_month t
       ) t2
 group by currency, chn_type
grouping sets (currency, chn_type)




-- 统计周期：日 (未完待续)

with third_part_info_day as
(
select a.order_id
      ,a.order_user_id
      ,bb.period_unit
      ,bb.main
      ,cc.currency
      ,cc.chn_type
      ,cc.pay_amount
  from intdata.ugc_order_daily a
 inner join (   
            select b.order_id
                  ,b.period_unit
                  ,b.main
              from intdata.ugc_order_item_daily b
             where b.src_file_day = '20170501' --'${SRC_FILE_DAY}'
            ) bb
    on a.order_id = bb.order_id 
 inner join (
            select c.order_id
                  ,c.currency
                  ,c.chn_type
                  ,c.pay_amount
              from intdata.ugc_order_paychannel_daily c
             where c.src_file_day = '20170501' --'${SRC_FILE_DAY}'
               and c.chn_type not in (50,58,59,302,304,306) 
            ) cc
    on a.order_id = cc.order_id       
 where a.src_file_day = '20170501' --'${SRC_FILE_DAY}'
   and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')
   and a.order_status = 5
)
select t1.period_unit
      ,t1.currency
      ,t1.chn_type
      ,sum(t1.pay_amount)
      ,count(distinct t1.user_id)
  from (
        select t.period_unit, t.currency, t.chn_type
              ,case when t.period_unit <> 'hour' and t.main = 0 then t.pay_amount else 0 end pay_amount --第三方新增包周期收入
              ,case when t.period_unit = 'month' then t.order_user_id else '' end user_id   --第三方新增包周期订购用户数
          from third_part_info_day t
       ) t1
 group by period_unit, currency, chn_type
grouping sets (period_unit, currency, chn_type)

 union all

select 'hour' as period_unit
      ,t2.currency
      ,t2.chn_type
      ,sum(t2.pay_amount)
      ,count(distinct t2.user_id)
  from (
        select t.currency, t.chn_type
              ,case when t.period_unit = 'hour' and t.main = 0 then t.pay_amount else 0 end pay_amount  --第三方新增按次收入
              ,case when t.period_unit = 'hour' then t.order_user_id else '' end user_id        --第三方新增按次订购用户数
          from third_part_info_day t
       ) t2
 group by currency, chn_type
grouping sets (currency, chn_type)



-----------------------------------------------------------------------------------------------------------------------------------

--第三方实际收入（包月分摊+按次）
--统计周期：月
select t.chn_type
      ,sum(t.pay_amount)
  from ( 
        select a.pay_chn_type as chn_type
              ,a.period_amount as pay_amount
          from intdata.ugc_tpp_contract a
         where unix_timestamp() <= cast(a.expire_time as bigint)    --合约到期日期    get_time()???
           and unix_timestamp() >= cast(a.start_time as bigint)       --合约创建日期
           and a.src_file_day >= '${MONTH_START_DAY}'
           and a.src_file_day <= '${MONTH_END_DAY}'

         union all 
         
        select b.chn_type
              ,case when b.main = 0 and b.period_unit = 'hour' then b.pay_amount else 0 end pay_amount  --按次
          from third_part_info_month b
       ) t
  group by t.chn_type  


--第三方包月分摊付费用户数  (未完待续)
--统计维度：货币类型，支付渠道
--统计周期：月    
select count(distinct a.user_id) 
  from intdata.ugc_tpp_contract a
 where unix_timestamp() <= cast(a.expire_time as bigint)
   and unix_timestamp() >= cast(a.start_time as bigint)
   and a.src_file_day >= '20170401' --'${MONTH_START_DAY}'
   and a.src_file_day <= '20170430' --'${MONTH_END_DAY}'
