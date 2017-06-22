

select substr('${src_file_day}',1,6) stat_month  --月份
      ,'698040' as migu_company_code          --咪咕子公司（编码）
      ,a.term_video_type_id as product_id     --产品ID
      ,a.term_video_type_name as product_name --产品名称
      ,case when a.term_os_type_id = '00' then '0' 
            when a.term_os_type_id = '04' then '1'
            when a.term_os_type_id = '05' then '1'
            when a.term_os_type_id = '06' then '0'
            when a.term_os_type_id = '09' then '0' else '' end as product_type   --产品类型
      ,'' as start_date                       --生效日期
      ,'' as expire_date                      --失效日期
  from rptdata.dim_term_prod a 
 group by a.term_video_type_id
         ,a.term_video_type_name
         ,case when a.term_os_type_id = '00' then '0' 
               when a.term_os_type_id = '04' then '1'
               when a.term_os_type_id = '05' then '1'
               when a.term_os_type_id = '06' then '0'
               when a.term_os_type_id = '09' then '0' else '' end
  
  


hive> desc rptdata.dim_term_prod;
OK
term_prod_id            string                                      
term_prod_name          string                                      
term_prod_type_id       string                                      
term_prod_type_name     string                                      
term_prod_class_id      string                                      
term_prod_class_name    string                                      
term_version_id         string                                      
term_os_type_id         string     --product_type                                   
term_os_type_name       string                                      
term_video_type_id      string     --product_id                                 
term_video_type_name    string     --product_name                               
term_video_soft_id      string                                      
term_video_soft_name    string                                      
dw_create_by            string                                      
dw_create_time          string                                      
dw_update_by            string                                      
dw_update_time          string                                      
dw_delete_flag          string                  Y/N                 
dw_crc                  string

    > desc rpt.dim_term_prod_v;
OK
term_prod_id            string                                      
term_prod_name          string                                      
term_prod_type_id       string                                      
term_prod_type_name     string                                      
term_prod_class_id      string                                      
term_prod_class_name    string                                      
term_os_type_id         string                                      
term_os_type_name       string                                      
term_video_type_id      string                                      
term_video_type_name    string                                      
term_video_soft_id      string                                      
term_video_soft_name    string 




    > desc rptdata.dim_os_type;
OK
os_type_id              string                                      
os_type_name            string                                      
dw_create_by            string                                      
dw_create_time          string                                      
dw_update_by            string                                      
dw_update_time          string                                      
dw_delete_flag          string                  Y/N                 
dw_crc                  string


hive> select * from dim_os_type;
OK
00  android         NULL    NULL    NULL    NULL    NULL    NULL
04  iphone          NULL    NULL    NULL    NULL    NULL    NULL
05  ipad            NULL    NULL    NULL    NULL    NULL    NULL
06  androidpad      NULL    NULL    NULL    NULL    NULL    NULL
07  vvp             NULL    NULL    NULL    NULL    NULL    NULL
08  TV              NULL    NULL    NULL    NULL    NULL    NULL
09  android sdk     NULL    NULL    NULL    NULL    NULL    NULL
10  open api        NULL    NULL    NULL    NULL    NULL    NULL
-998    未知类型    NULL    NULL    NULL    NULL    NULL    NULL