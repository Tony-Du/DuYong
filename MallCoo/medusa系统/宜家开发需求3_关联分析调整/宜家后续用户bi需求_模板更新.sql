
with stg_total as (    
select count(mallcoo_id) as total
  from ( 
        select mallcoo_id       
          from {{ @table }} 
         where {{ @project }}
           and {{ @time_range }}
           and {{ @filter }}
         group by mallcoo_id 
        ) t
),
stg_kudu_event as (
select mallcoo_id,
       {{ `if(#item.field# is null or trim(cast(#item.field# as string))='', '未知', cast(#item.field# as string))` }} itemset 
  from {{ @table }} 
 where {{ @project }}
   and {{ @time_range }}
   and {{ @filter }}
 group by mallcoo_id, {{ `#item.field#` }}
),
stg_num as (
select itemset,
       count(mallcoo_id) as num
  from stg_kudu_event
 group by itemset
),
stg_supportcount as (
select itemset_a,
       itemset_b,
       supportcount
  from (
        select itemset_a,
               itemset_b,
               supportcount,
               rank() over (partition by itemset_a order by supportcount desc) as rnk
          from (       
                select itemset_a, 
                       itemset_b,
                       supportcount
                  from (
                       select itemset_a, 
                              itemset_b,
                              count(distinct mallcoo_id) as supportcount 
                         from (
                               select a.mallcoo_id, 
                                      a.itemset as itemset_a,
                                      b.itemset as itemset_b
                                 from stg_kudu_event a
                                 join stg_kudu_event b 
                                   on a.mallcoo_id = b.mallcoo_id
                              ) t
                        {{ #item.rule#.where('itemset_a').prefix('WHERE') }}
                        group by itemset_a, itemset_b 
                       ) a
                 where itemset_a <> itemset_b
               ) b 
       ) c
where rnk <= 10      
),
stg_supportcount_num_supportcountb as (
select a.itemset_a
      ,a.itemset_b
      ,a.supportcount
      ,b.num
      ,c.num as supportcountb
  from stg_supportcount a 
  join stg_num b 
    on a.itemset_a = b.itemset
  join stg_num c
    on a.itemset_b = c.itemset
)
select t1.itemset_a
      ,t1.itemset_b
      ,nvl(t1.supportcount,0) as supportcount
      ,nvl(t1.num,0) - nvl(t1.supportcount,0) as non_supportcount 
      ,case when nvl(t2.total,0)=0 then 0 else cast(nvl(t1.supportcount,0)/t2.total as decimal(38,4)) end as support 
      ,case when nvl(t1.num,0)=0 then 0 else cast(nvl(t1.supportcount,0)/t1.num as decimal(38,4)) end as confidence       
      ,case when nvl(t1.supportcountb,0)=0 then 0 else cast(nvl(t1.supportcount,0)/t1.supportcountb as decimal(38,4)) end as reverseconfidence  
      ,case when (nvl(t1.supportcount,0)=0 or nvl(t1.supportcountb,0)=0) then 0 else cast((nvl(t1.supportcount,0)/t1.num)/(nvl(t1.supportcountb,0)/t2.total)as decimal(38,4)) end as lift  
  from stg_supportcount_num_supportcountb t1
 cross join stg_total t2 
-- {{ #item.filter.event#.where('itemset_b').prefix('WHERE') }};






