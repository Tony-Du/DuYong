


select case when nvl(b.wechat_follow_cnt,0) = 0 then 0 else round(nvl(a.open_app_page_cnt,0)/wechat_follow_cnt, 4) end as wechat_active_rate
from (
    select count(distinct mallcoo_id) as open_app_page_cnt
    from {{ @_table_a }}
    where {{ @project }}
    and {{ #time#.@time_range() }}
    and {{ #filter_a#.@filter() }}
) a
cross join (
    select count(distinct mallcoo_id) as wechat_follow_cnt
    from {{ @_table_b }}
    where {{ @project }}
    and {{ #filter_b#.@filter() }}
) b;






select
    case when nvl(wechat_follow_cnt,0) = 0 then 0 else round(nvl(open_app_page_cnt,0)/wechat_follow_cnt, 4) end as active_rate
	{{ #self#.union(@_dimensions_common,#dimensions.dimensions.user#).map(`a.#alias#`).join(' , ').prefix(',') }}
from (
    select
	    count(distinct mallcoo_id) as open_app_page_cnt
        {{ #self#.union(@_dimensions_a,@_dimensions_common,#dimensions.dimensions.user#).map { @__dimension -> dimension, #alias# -> alias => @__dimension_unknown }.join(',').prefix(',') }}
    from {{ @_table_a() }}
    where {{ @project }}
    and {{ #time#.@time_range() }}
    and {{ #filter_a#.@filter() }}
       {{ #self#.union(@_dimensions_a,@_dimensions_common,#dimensions.dimensions.user#).map(#alias#).join(',').prefix('GROUP BY ') }}
) a
{{ match {
		#self#.union(@_dimensions_common,#dimensions.dimensions.user#) is 'empty' => 'CROSS',
		_ => ''
	}
}} JOIN
(
    select
      {{
         match {
          #self#.union(@_dimensions_common,#dimensions.dimensions.user#).filter(#field# = 'time') is 'empty' => 'wechat_follow_cnt',
          _ => #self#.union(@_dimensions_b,@_dimensions_common,#dimensions.dimensions.user#).filter(#field# = 'time').map(#alias#).join(',').prefix('sum(wechat_follow_cnt) over(order by ').suffix(') as wechat_follow_cnt')
        }
      }}
      {{ #self#.union(@_dimensions_b,@_dimensions_common,#dimensions.dimensions.user#).map(#alias#).join(',').prefix(',') }}
    from (
        select
           count(distinct mallcoo_id) as wechat_follow_cnt
           {{ #self#.union(@_dimensions_b,@_dimensions_common,#dimensions.dimensions.user#).map { @__dimension -> dimension, #alias# -> alias => @__dimension_unknown }.join(',').prefix(',') }}
        from {{ @_table_b() }}
        where {{ @project }}
        and {{ #filter_b#.@filter() }}
        and month <= to_date(trunc( {{ `'#time.time_range.0.1#'` }},'MM'))
        and day <= {{ `'#time.time_range.0.1#'` }}
        {{ #self#.union(@_dimensions_b,@_dimensions_common,#dimensions.dimensions.user#).map(#alias#).join(',').prefix('GROUP BY ') }}
    ) t
) b
{{ #self#.union(@_dimensions_common,#dimensions.dimensions.user#).map(`a.#alias#=b.#alias#`).join(' AND ').prefix('on ') }}
;








{
    "columns": [
        {
            "code": "active_rate",
            "type": "percentage",
            "text": "指标一",
            "clickable": false
        }
    ],
    "parameters": [
        {
            "code": "dimensions",
            "controlType": "dimensions",
            "events": ["13001","13002"],                            
            "showType": "onlyCommon",
            "target": "User",
            "enableInjection": true,
            "text": "分组查看"
        },
        {
            "code": "filter_b",
            "controlType": "filter",
            "events": ["13001"],                            
            "showType": "normal",
            "target": "User",
            "enableInjection": false,
            "text": "筛选条件一"
        },
        {
            "code": "filter_a",
            "controlType": "filter",
            "events": ["13002"],                            
            "showType": "normal",
            "target": "User",
            "enableInjection": true,
            "text": "筛选条件二"
        },
        {
            "code": "time",
            "controlType": "time_range",
            "text":"选择时间"
        },
        
        
        {
            "controlType":"hidden"
            "value":{
                "measures":[{
                    "event_type": 13001,
                    "measure": "count",
                    "alias": "open_app_page_cnt"                
                }]          
            }
        }
    ]
}
    



