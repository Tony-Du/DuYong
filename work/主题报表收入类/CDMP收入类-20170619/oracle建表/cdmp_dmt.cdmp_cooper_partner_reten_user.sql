
create table cdmp_cooper_partner_reten_user 
(
    src_file_month                 char(6),
    business_type                  varchar2(128),                            
    business_name                  varchar2(128),                    
    sub_business_name              varchar2(128),                        
    phone_province_name            varchar2(128), 
    channel_id                     varchar2(128),                                   
    chn_name                       varchar2(128),                   
    chn_attr_1_name                varchar2(128),              
    chn_attr_2_name                varchar2(128), 
    new_add_order_user_cnt         number(10),
    this_mon_reten_user_cnt        number(10),
    second_mon_reten_user_cnt      number(10),
    after_2_mon_reten_user_cnt     number(10),
    after_3_mon_reten_user_cnt     number(10),
    after_4_mon_reten_user_cnt     number(10),
    after_5_mon_reten_user_cnt     number(10),
    after_6_mon_reten_user_cnt     number(10)
)
nologging
partition by range (src_file_month)
  (partition p_max values less than (maxvalue));

comment on table cdmp_cooper_partner_reten_user  is '合作伙伴留存用户数';

comment on column cdmp_cooper_partner_reten_user.src_file_month is 
'数据周期(月)';
comment on column cdmp_cooper_partner_reten_user.business_type is 
'业务类型';                                                                                             
comment on column cdmp_cooper_partner_reten_user.business_name is 
'业务名称';                                                                                                 
comment on column cdmp_cooper_partner_reten_user.sub_business_name is 
'子业务名称';                                                                                                            
comment on column cdmp_cooper_partner_reten_user.phone_province_name is 
'（电话）省份';                                                                                                  
comment on column cdmp_cooper_partner_reten_user.channel_id is 
'渠道ID';                                                                                                                                  
comment on column cdmp_cooper_partner_reten_user.chn_name is 
'渠道名称';                                                                                                                            
comment on column cdmp_cooper_partner_reten_user.chn_attr_1_name is 
'渠道一级';                                                     
comment on column cdmp_cooper_partner_reten_user.chn_attr_2_name is 
'渠道二级';                                                                                   
comment on column cdmp_cooper_partner_reten_user.new_add_order_user_cnt is 
'新增用户数（当月）';                                                    
comment on column cdmp_cooper_partner_reten_user.this_mon_reten_user_cnt is 
'本月留存用户数';
comment on column cdmp_cooper_partner_reten_user.second_mon_reten_user_cnt is 
'第二个月的留存用户数';
comment on column cdmp_cooper_partner_reten_user.after_2_mon_reten_user_cnt is 
'2个月后的留存用户数';
comment on column cdmp_cooper_partner_reten_user.after_3_mon_reten_user_cnt is 
'3个月后的留存用户数';
comment on column cdmp_cooper_partner_reten_user.after_4_mon_reten_user_cnt is 
'4个月后的留存用户数';
comment on column cdmp_cooper_partner_reten_user.after_5_mon_reten_user_cnt is 
'5个月后的留存用户数';
comment on column cdmp_cooper_partner_reten_user.after_6_mon_reten_user_cnt is 
'6个月后的留存用户数';
