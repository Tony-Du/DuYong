
--第三方新增包周期收入
--统计周期：日、月
--统计维度：订购周期（月，季，年，月自动续订），货类型币，支付渠道

select order_id
      ,period_unit
      ,currency
      ,chn_type
      ,sum(pay_amount)
  from (
          select a.order_id
                ,bb.period_unit      --订购周期
                ,cc.currency         --货币类型
                ,cc.chn_type         --支付渠道类型
                ,cc.pay_amount
            from intdata.ugc_order_daily a
           inner join (     
                        select b.order_id
                              ,b.period_unit
                          from intdata.ugc_order_item_daily b
                         where b.main = 0 
                           and b.period_unit <> 'hour' 
                           and b.src_file_day = '20170501' --'${SRC_FILE_DAY}'
                      ) bb
              on a.order_id = bb.order_id 
           inner join (
                        select c.order_id
                              ,c.currency
                              ,c.chn_type
                              ,c.pay_amount
                          from intdata.ugc_order_paychannel_daily c
                         where c.chn_type not in (50,58,59,302,304,306) 
                           and c.src_file_day = '20170501' --'${SRC_FILE_DAY}'
                      ) cc
              on a.order_id = cc.order_id     
           where a.src_file_day = '20170501' --'${SRC_FILE_DAY}'
             and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')
             and a.order_status = 5
       ) t
  group by order_id, period_unit, currency, chn_type   
grouping sets((order_id, period_unit),
              (order_id, currency),
              (order_id, chn_type)
             );

--------------------------------------------------------------------------------------------------------------------------------
			 
select order_id
      ,period_unit
      ,currency
      ,chn_type
      ,sum(pay_amount)
  from (
          select a.order_id
                ,bb.period_unit      --订购周期
                ,cc.currency         --货币类型
                ,cc.chn_type         --支付渠道类型
                ,cc.pay_amount
            from intdata.ugc_order_daily a
           inner join (     
                        select b.order_id
                              ,b.period_unit
                          from intdata.ugc_order_item_daily b
                         where b.main = 0 
                           and b.period_unit <> 'hour' 
                           and b.src_file_day >= '${MONTH_START_DAY}'
                           and b.src_file_day <= '${MONTH_END_DAY}'
                      ) bb
              on a.order_id = bb.order_id 
           inner join (
                        select c.order_id
                              ,c.currency
                              ,c.chn_type
                              ,c.pay_amount
                          from intdata.ugc_order_paychannel_daily c
                         where c.chn_type not in (50,58,59,302,304,306) 
                           and c.src_file_day >= '${MONTH_START_DAY}'
                           and c.src_file_day <= '${MONTH_END_DAY}'
                      ) cc
              on a.order_id = cc.order_id     
           where a.src_file_day >= '${MONTH_START_DAY}'
             and a.src_file_day <= '${MONTH_END_DAY}'
             and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')
             and a.order_status = 5
       ) t
  group by order_id, period_unit, currency, chn_type   
grouping sets((order_id, period_unit),
              (order_id, currency),
              (order_id, chn_type)
             );			 


--第三方新增包周期订购用户数
--统计维度：订购周期（月，季，年，月自动续订），货币类型，支付渠道
--统计周期：日、月
select order_id
      ,period_unit
      ,currency
      ,chn_type
      ,count(distinct order_user_id)
  from (
        select a.order_id
              ,bb.period_unit      --订购周期
              ,cc.currency         --货币类型
              ,cc.chn_type         --支付渠道类型
              ,a.order_user_id  
          from intdata.ugc_order_daily a
         inner join (   
                    select b.order_id
                          ,b.period_unit
                      from intdata.ugc_order_item_daily b
                     where b.src_file_day >= '20170401' --'${MONTH_START_DAY}'
                       and b.src_file_day <= '20170430' --'${MONTH_END_DAY}'
                       and b.period_unit = 'month'  
                    ) bb
            on a.order_id = bb.order_id 
         inner join (
                    select c.order_id
                          ,c.currency
                          ,c.chn_type
                          --,c.pay_amount
                      from intdata.ugc_order_paychannel_daily c
                     where c.src_file_day >= '20170401' --'${MONTH_START_DAY}'
                       and c.src_file_day <= '20170430' --'${MONTH_END_DAY}'
                       and c.chn_type not in (50,58,59,302,304,306) 
                    ) cc
            on a.order_id = cc.order_id       
         where a.src_file_day >='20170401' --'${MONTH_START_DAY}'
           and a.src_file_day <= '20170430' --'${MONTH_END_DAY}'
           and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')
           and a.order_status = 5
       ) t
 group by order_id, period_unit, currency, chn_type   
grouping sets((order_id, period_unit),
              (order_id, currency),
              (order_id, chn_type)
             );

--第三方新增按次订购用户数
--统计维度：货币类型，支付渠道
--统计周期：日、月
select order_id
      ,currency
      ,chn_type
      ,count(distinct order_user_id)
  from (
        select a.order_id
              ,cc.currency
              ,cc.chn_type
              ,a.order_user_id
          from intdata.ugc_order_daily a
         inner join (   
                    select b.order_id
                          --,b.period_unit
                      from intdata.ugc_order_item_daily b
                     where b.src_file_day >= '${MONTH_START_DAY}'
                       and b.src_file_day <= '${MONTH_END_DAY}'
                       and b.period_unit = 'hour'   
                    ) bb
            on a.order_id = bb.order_id 
         inner join (
                    select c.order_id
                          ,c.currency
                          ,c.chn_type
                          --,c.pay_amount
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
       ) t
 group by order_id, currency, chn_type
grouping sets((order_id, currency), (order_id, chn_type));

 
--第三方新增按次收入
--统计维度：货币类型，支付渠道
--统计周期：日、月
select order_id
      ,currency
      ,chn_type
      ,sum(pay_amount)
  from (
        select a.order_id
              ,cc.currency
              ,cc.chn_type
              ,cc.pay_amount
          from intdata.ugc_order_daily a
         inner join (   
                    select b.order_id
                          --,b.period_unit
                      from intdata.ugc_order_item_daily b
                     where b.src_file_day >= '${MONTH_START_DAY}'
                       and b.src_file_day <= '${MONTH_END_DAY}'
                       and b.period_unit = 'hour' 
                       and b.main = 0              
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
       ) t
 group by order_id, currency, chn_type
grouping sets((order_id,currency),(order_id,chn_type))


--第三方包月分摊付费用户数  (未完待续)
--统计维度：货币类型，支付渠道
--统计周期：月    
select count(distinct a.user_id) 
  from intdata.ugc_tpp_contract a
 where unix_timestamp() <= cast(a.expire_time as bigint)
   and unix_timestamp() >= cast(a.start_time as bigint)
   and a.src_file_day >= '${MONTH_START_DAY}'
   and a.src_file_day <= '${MONTH_END_DAY}'
   

   
   
--第三方实际收入（包月分摊+按次）(未完待续)
--统计周期：月
    
select t.pay_chn_type
      ,t.period_amount 
  from intdata.ugc_tpp_contract t
 where get_time() <= t.expire_time  	--合约到期日期    get_time()???
   and get_time() >= t.start_time       --合约创建日期 
   and t.src_file_day >= '${MONTH_START_DAY}'
   and t.src_file_day <= '${MONTH_END_DAY}'

 union all
   
select a.order_id
      ,cc.chn_type         --支付渠道类型
      ,sum(cc.pay_amount)
  from intdata.ugc_order_daily a
 inner join (   
            select b.order_id
                  ,b.period_unit
              from intdata.ugc_order_item_daily b
             where b.src_file_day >= '${MONTH_START_DAY}'
               and b.src_file_day <= '${MONTH_END_DAY}'
               and b.main = 0 
               and b.period_unit = 'hour'   --按次
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
 group by a.order_id, cc.chn_type 



			 
			 
--数据源

intdata.ugc_order_daily
    order_id                string       --订单id                             
    order_user_id           string       --订单用户id                               
    phone_number            string       --用户手机号码                               
    phone_range             string       --用户手机号码号段                             
    create_time             string       --订单创建时间                               
    order_status            string       --订单状态                             
    merchant_account        string       --商户账号                             
    cancel_order_id         string       --订单取消id                               
    total_amount            decimal(38,4)--订单总额，即指所有订单子项的实际金额之和                             
    service_code            string       --卖法                               
    service_count           string       -- 计费点数量                               
    pay_type                string       --支付方式：1、直接支付；2、短信支付；3、页面支付；4、找人代付                             
    chn_id_source           string       --渠道id                                
    payment_id              string       --支付交易编号/流水号                   
    app_name                string       --app名称                                      
    external_order_id       string       --外部订单id  
    goods_id                string       --商品id  
    goods_type              string       --商品类型  
    resource_id             string       --触发订单的资源id，代表触发订购的节目  
    resource_type           string       --触发订单的资源类型  
    cp_id                   string       --
    sub_busi_id             string       --子业务id  
    term_version_id         string       --版本号  
    chn_id                  string       --渠道id  
    chn_busi_type           string       --渠道业务类型-99000                             
    term_prod_id            string       --终端产品id                               
    city_id                 string       --地市                               
    new_version_id          string       --6位版本号                                
    user_type               string       --用户类型(默认1，注册用户)                               
    network_type            string       --网络类型                             
    use_type                string       --使用方式                             
    company_id              string       --合作公司                             
    src_file_day            string       --统计日期，分区字段                            
    

intdata.ugc_order_item_daily    
    order_id                string            --订单编号                        
    item_id                 string            --交付物编号                       
    amount                  string            --授权周期                        
    authorize_type          string            --授权类型:BOSS_MONTH, NULL, PERIOD, TIMES                        
    period_unit             string            --周期单位        *************                       
    auto_relet              string            --自动续订                                
    chn_id_source           string            --渠道ID                                
    expire_time             string            --过期时间                                
    extend_authorize        string            --                        
    product_id              string            --当交付物为pms产品时填写id                         
    product_type            string            --pms产品类型                         
    subtype                 string            --子状态                         
    user_num                string            --授权用户                        
    valid_start_time        string            --授权开始时间                          
    description             string            --描述                          
    handler                 string            --授权交付系统:MIGU_AUTH, AAA, CONTRACT, MIGU_ACCOUNT                       
    main                    string            --是否主交付物                          
    quantity                bigint            --数量                          
    unit_price              decimal(38,4)     --单价                          
    resource_id             string            --当交付物为节目时填写节目id                          
    cp_id                   string            --b.sp_id as cp_id                        
    src_file_day            string      
    
    
intdata.ugc_order_paychannel_daily
    order_id                string            --订单id                        
    pay_chn_id              string            --支付渠道ID                          
    chn_type                string            --渠道类型（boss、支付宝、微信等）                          
    currency                string            --货币类型（现金或者券）                         
    pay_amount              decimal(38,4)     --支付金额                        
    src_file_day            string  
    
--支付渠道类型    
16
payment虚拟货币支付方式
17
支付宝手机客户端支付
23
支付宝web即时到账支付
26
Appstore支付（验证receipt）
28
联通号码话费扣费
44
支付宝SDK支付
50
话费点播支付
55
微信SDK支付
57
Payment虚拟货币支付（预留+确认）
58
咪咕一级支付
59
咪咕SDK支付
60
电信IAP   SDK支付
61
支付宝代扣费
62
银视通支付
63
浦发银行先看后付
300
支付宝扫码支付
301
微信扫码支付
302
阳光计划话费支付(新六套)
303
支付宝扫码即时到账支付
304
网状网支付
306 阳光计划话费支付(老五套)