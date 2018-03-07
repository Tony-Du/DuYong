

--去过服装店，也去过其他店的人数    关联人数
--去过服装店，没去过其他店的人数    非关联人数


${PROJECT_ID} --1
${CATEGORY} --服装
${EVENT_ID} --25001
${START_DAY} --2016-01-01
${END_DAY} --2016-05-31






select Itemset,
       nvl(SupportCount,0) as SupportCount, --关联人数
       nvl(num,0)-nvl(SupportCount,0) as non-SupportCount, --非关联人数
       case when nvl(total,0)=0 then 0 else cast(nvl(SupportCount,0)/nvl(total,0) as decimal(38,4)) end as Support, --支持度 
       case when nvl(num,0)=0 then 0 else cast(nvl(SupportCount,0)/nvl(num,0) as decimal(38,4)) end as Confidence,  --置信度
       case when nvl(num,0)=0 then 0 else cast(nvl(SupportCount,0)/nvl(SupportCountB,0) as decimal(38,4)) end as ReverseConfidence,  --反向置信度
       case when (nvl(num,0)=0 or nvl(total,0)=0 or nvl(SupportCountB,0)=0) then 0 else nvl(cast((SupportCount/num)/(SupportCountB/total)as decimal(38,4)),0) end as Lift  --提升度
  from (
        select count(mallcoo_id) as total,  
               sum(item_sc) as num          
          from (
                select mallcoo_id,
                       max(if(if(shop_commercialtypename is null or trim(cast(shop_commercialtypename as string))='','未知',
                                    cast(shop_commercialtypename as string))='${CATEGORY}', 1, 0)) as item_sc    
                  from medusa.kudu_event_extend 
                 where id between '${PROJECT_ID}-' and '${PROJECT_ID}-z' 
                   and projectid = '${PROJECT_ID}' 
                   and (month between to_date(trunc('${START_DAY}','MM')) and to_date(trunc('${END_DAY}','MM')) 
                        and day between '${START_DAY}' and '${END_DAY}') 
                   and event_type = ${EVENT_ID} 
                 group by mallcoo_id
                ) t 
       ) a
cross join (
        select Itemset,
               sum(num) as SupportCountB,  
               sum(hadBeen_count) as SupportCount
          from (
                select Itemset,
                       count(distinct mallcoo_id) as num,
                       if(hadBeen=1, count(distinct mallcoo_id), null) as hadBeen_count
                  from (
                        select mallcoo_id,
                               max(if(if(shop_commercialtypename is null or trim(case(shop_commercialtypename as string))='',
                                     '未知',trim(case(shop_commercialtypename as string)))='${CATEGORY}',1,0)) over(partition by mallcoo_id) as hadBeen,
                               if(shop_commercialtypename is null or trim(case(shop_commercialtypename as string))='',
                                     '未知',trim(case(shop_commercialtypename as string))) as Itemset
                          from medusa.kudu_event_extend 
                         where id between '${PROJECT_ID}-' and '${PROJECT_ID}-z'
                           and project = '${PROJECT_ID}'
                           and (month between to_date(trunc('${START_DAY}','MM')) and to_date(trunc('${END_DAY}','MM')) 
                                and day between '${START_DAY}' and '${END_DAY}')
                           and event_type = ${EVENT_ID}     
                        ) x
                  group by Itemset,hadBeen
               ) b
         group by Itemset
       ) c
;      




