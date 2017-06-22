

select substr('${SRC_FILE_DAY}', 1, 6) as stat_month          --日期
      ,'698040' as migu_company_code          --咪咕子公司
      ,a.channel_id                           --子渠道ID
      ,nvl(b.chn_name, '') as chn_name        --子渠道名称
      ,'' as cooperator_id                    --主渠道ID
      ,a.cooperator_name                      --主渠道名称
      ,'' as start_date                       --生效日期
      ,'' as expire_date                      --失效日期
      ,'1' as flag_1                          --标识1
      ,'0' as flag_2                          --标识2
  from mscdata.dim_cpa_channel2cooperator a
  left join rptdata.dim_chn b
    on a.channel_id = b.chn_id
 group by a.channel_id,
          nvl(b.chn_name, ''),
          a.cooperator_name
          
          
                 
    
  > desc mscdata.dim_cpa_channel2cooperator;
OK
umeng_name              string                                      
product_name            string                                      
channel_id              string                                      
cooperator_short_name   string                                      
cooperator_name         string                                      
cooperator_key          string                                      
dw_crt_day              string   
  
  
  > desc rptdata.dim_chn;
OK
chn_id                  string                                      
chn_name                string                                      
chn_type                string                                      
chn_attr_1_id           string                                      
chn_attr_1_name         string                                      
chn_attr_2_id           string                                      
chn_attr_2_name         string                                      
chn_attr_3_id           string                                      
chn_attr_3_name         string                                      
chn_attr_4_id           string                                      
chn_attr_4_name         string                                      
chn_attr_5_id           string                                      
chn_attr_5_name         string                                      
chn_type_id             string                                      
chn_class               string                                      
dw_create_by            string                                      
dw_create_time          string                                      
dw_update_by            string                                      
dw_update_time          string                                      
dw_delete_flag          string                                      
dw_crc                  string
  
  
  
  