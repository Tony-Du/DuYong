
--建表
create table rptdata.fact_ugc_tpp_order_relation_detail (
user_type          string,
network_type       string,
cp_id              string,
use_type           string,
term_prod_id       string,
term_version_id    string,
chn_id             string,
city_id            string,
sub_busi_id        string,
user_id            string,
period_unit        string,
pay_chn_type       string,
order_status       string,
authorize_type     string,
currency           string,
company_id         string,
goods_type         string,
product_id         string,
product_type       string,
main               string,
program_id         string,
opr_time           string,
substatus          string,
order_id           string,
payment_id         string,
amount             decimal(38,4),
payment_result     string,
serv_number        string,
sp_id              string,
authorize_period   string,
valid_start_time   string,
expire_time        string,
opr_login          string,
totalamount        decimal(38,4),
period_amount      decimal(38,4)
)
partitioned by (src_file_day string)
stored as parquet;





--存储逻辑

insert overwrite table rptdata.fact_ugc_tpp_order_relation_detail partition (src_file_day = '${SRC_FILE_DAY}')
select  t.user_type
       ,t.network_type
       ,t.cp_id
       ,t.use_type
       ,t.term_prod_id      
       ,t.term_version_id   
       ,t.chn_id            
       ,t.city_id           
       ,t.sub_busi_id       
       ,t.user_id           
       ,t.period_unit       
       ,t.pay_chn_type      
       ,t.order_status      
       ,t.authorize_type    
       ,t.currency          
       ,t.company_id        
       ,t.goods_type        
       ,t.product_id        
       ,t.product_type      
       ,t.main              
       ,t.program_id        
       ,t.opr_time          
       ,t.substatus         
       ,t.order_id          
       ,t.payment_id        
       ,t.amount            
       ,t.payment_result    
       ,t.serv_number       
       ,t.sp_id             
       ,t.authorize_period  
       ,t.valid_start_time  
       ,t.expire_time       
       ,t.opr_login         
       ,t.totalamount
       ,t.period_amount
  from (
        select a.user_type
              ,a.network_type
              ,a.cp_id
              ,a.use_type
              ,a.term_prod_id      
              ,a.term_version_id   
              ,a.chn_id            
              ,a.city_id           
              ,a.sub_busi_id       
              ,a.user_id           
              ,a.period_unit       --授权时间单位: hour(按次) day month(包周期)
              ,a.pay_chn_type      
              ,a.order_status      
              ,a.authorize_type    --授权类型：BOSS_MONTH, NULL, PERIOD, TIMES
              ,a.currency          
              ,a.company_id        
              ,a.goods_type        
              ,a.product_id        
              ,a.product_type      
              ,a.main              
              ,a.program_id        
              ,a.opr_time           --创建时间
              ,a.substatus         
              ,a.order_id          
              ,a.payment_id        
              ,a.amount             --intdata.ugc_order_paychannel_daily 中的 pay_amount 钱
              ,a.payment_result    
              ,a.serv_number        --phone_number电话号码
              ,a.sp_id             
              ,a.authorize_period   --授权周期 intdata.ugc_order_item_daily 中的 amount 时间
              ,a.valid_start_time  
              ,a.expire_time       
              ,a.opr_login          --phone_number电话号码
              ,a.totalamount       	--合约周期总价 intdata.ugc_order_daily 中的 total_amount 钱                                       
              ,case when a.period_unit = 'MONTH' then (cast(a.totalamount as decimal(38,4)) / cast(a.authorize_period as int)) end as period_amount  --合约周期单价(包月分摊)       
          from rptdata.fact_ugc_order_detail_daily a 
         where a.pay_chn_type not in (50,58,59,302,304,306)     --第三方
           and a.main = 0
           and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')
           and a.order_status in ('5', '9')
           and a.src_file_day = '${SRC_FILE_DAY}';  -- T
               
         union all

        select b.user_type
              ,b.network_type
              ,b.cp_id
              ,b.use_type
              ,b.term_prod_id      
              ,b.term_version_id   
              ,b.chn_id            
              ,b.city_id           
              ,b.sub_busi_id       
              ,b.user_id           
              ,b.period_unit       
              ,b.pay_chn_type      
              ,b.order_status      
              ,b.authorize_type    
              ,b.currency          
              ,b.company_id        
              ,b.goods_type        
              ,b.product_id        
              ,b.product_type      
              ,b.main              
              ,b.program_id        
              ,b.opr_time          
              ,b.substatus         
              ,b.order_id          
              ,b.payment_id        
              ,b.amount            
              ,b.payment_result    
              ,b.serv_number       
              ,b.sp_id             
              ,b.authorize_period  
              ,b.valid_start_time  
              ,b.expire_time       
              ,b.opr_login         
              ,b.totalamount   
              ,b.period_amount
          from rptdata.fact_ugc_tpp_order_relation_detail b
         where '${SRC_FILE_DAY}' <= b.expire_time
           and b.src_file_day = '${YESTERDAY}'      -- T-1
       ) t ;
       

--举例：
--某用户在5.23下单，订购包月会员3个月，即5.23-8.22
--5.23, 在rptdata.fact_ugc_tpp_order_relation_detail中存在这条订购记录，我们直接计算出每个月的分摊费用period_amount（包月分摊）
--但是该条数据，在月最后一天才会被取出来，5月、6月、7月各取一次
--当在5.31时，rptdata.fact_ugc_tpp_order_relation_detail中是不存在这条记录的（因为它的数据一直是新增的），
--所以我们需要把历史数据取出来，即轮动取rptdata.fact_ugc_tpp_order_relation_detail表中昨天的数据
--6.30和7.31同理  


       
--数据源
       
hive> desc rptdata.fact_ugc_order_detail_daily;
OK
user_type               string                                      
network_type            string                                      
cp_id                   string                                      
use_type                string                                      
term_prod_id            string                                      
term_version_id         string                                      
chn_id                  string                                      
city_id                 string                                      
sub_busi_id             string                                      
user_id                 string                                      
period_unit             string                                      
pay_chn_type            string                                      
order_status            string                                      
authorize_type          string                                      
currency                string                                      
company_id              string                                      
goods_type              string                                      
product_id              string                                      
product_type            string                                      
main                    string                                      
program_id              string                                      
opr_time                string                                      
substatus               string                                      
order_id                string                                      
payment_id              string                                      
amount                  decimal(38,4)                               
payment_result          string                                      
serv_number             string                                      
sp_id                   string                                      
authorize_period        string                                      
valid_start_time        string                                      
expire_time             string                                      
opr_login               string                                      
totalamount             string           ???                           
src_file_day            string 