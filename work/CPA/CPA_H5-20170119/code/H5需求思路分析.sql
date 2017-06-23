解析各个参数（type：common，pagevisit，logevent），写到int层对应的3个表中
rptdata的4个表
app2个表

注：
除了产品，其他维度（页面,渠道）需要统计

任务：app & 数据导出


-- 1.分析数据源表 ods.kesheng_h5_json_ex

--(1)查看建表语句
CREATE EXTERNAL TABLE `ods.kesheng_h5_json_ex`(
  `json` string)
PARTITIONED BY ( 
  `src_file_day` string, 
  `src_file_hour` string)
--(2)查看分区
show partitions ods.kesheng_h5_json_ex;
OK
src_file_day=20170208/src_file_hour=15
--(3)查看具体数据
select * from ods.kesheng_h5_json_ex where src_file_day=20170208 and src_file_hour=15 limit 3;
OK
{"measure":{"URL":"http://h5.miguvideo.com/mgsp/yx01.jsp?channelId=12345678901"
			,"OS":"iOS"
			,"timeStamp":"1480521763049"}
,"param": {"type":"common"	-- type为 common
			,"IP":"127.0.0.1"
			,"cookieId":"asdf435234fqw2323vwe34"
			,"title":"migu movie"
			,"domain":"222.66.199.177"
			,"referrer":"222.66.199.177"
			,"width":"360"
			,"height":"640"
			,"lang":"zh-CN"}
}	
20170208	
15

{"measure":{"URL":"h5.miguvideo.com/mgzb/yx02.jsp?channelId=22345678901"
			,"OS":"iOS"
			,"timeStamp":"1480521763049"}
,"param": {"type":"pageVisit"	-- type为 pageVisit
			, "cookieId":"asdf435234fqw2323vwe34"
			, "IP":"127.0.0.1"
			,"startTime":"1480521763049"
			,"endTime":"1480521763049"
			, "visitTime": 234}
}	
20170208	
15

{"measure":{"URL":"http://127.0.0.1/migu/aa.jsp"
			,"OS":"iOS"
			,"timeStamp":"1480521763049"}
,"param": {"type":"downloadClick"	-- type为 downloadClick
			,"description":"xxxxxx"
			,"aaa":"bbb"}
}	
20170208	
15


-- 2. 分析视图 ods.kesheng_h5_json_ex_v

--(1)查看建表语句
CREATE VIEW `ods.kesheng_h5_json_ex_v` 
AS 
select `a1`.`url`	-- url的解析风险很大 
      ,`a1`.`array_url`[size(`a1`.`array_url`)-2] `product_id`
      ,substr(`a1`.`array_url`[size(`a1`.`array_url`)-1],1,instr(`a1`.`array_url`[size(`a1`.`array_url`)-1], '.jsp')-1) `page_id`	--STRING substr(...) & INT instr(...)
      ,substr(`a1`.`array_url`[size(`a1`.`array_url`)-1],instr(`a1`.`array_url`[size(`a1`.`array_url`)-1], 'channelId=')+10,30) `channel_id`
      ,`a1`.`os` 
	  ,`a1`.`time_stamp` 
	  ,`a1`.`data_src_type` 
	  ,`a1`.`param_json`
      ,`a1`.`src_file_day`
	  ,`a1`.`src_file_hour`
   from (select `m1`.`url` 
			   ,split(`m1`.`url`, '/') `array_url`
               ,`m1`.`os` 
			   ,`m1`.`time_stamp` 
			   ,`pt`.`data_src_type` 
			   ,`a1`.`param_json`
               ,`a0`.`src_file_day`
			   ,`a0`.`src_file_hour`
           from `ods`.`kesheng_h5_json_ex` `a0`
                lateral view json_tuple(`a0`.`json`, 'measure', 'param') `a1` as `measure_json`, `param_json`	-- 这是第一层
                lateral view json_tuple(`a1`.`measure_json`, 'URL', 'OS', 'timeStamp') `m1` as `url`, `os`, `time_stamp`	-- 这是第二层
                lateral view json_tuple(`a1`.`param_json`, 'type') `pt` as `data_src_type`	-- 这是第二层
       ) `a1`;

split(`m1`.`url`, '/') `array_url` -> "URL":"http://h5.miguvideo.com/
											mgsp/
											yx01.jsp?channelId=12345678901"
`a1`.`array_url`[size(`a1`.`array_url`)-2] `product_id` -> array_url的倒数第二位为product_id : mgsp
instr(`a1`.`array_url`[size(`a1`.`array_url`)-1], '.jsp') -> 找到array_url最后一位中的".jsp"的位置
	   
-- (2)查看 data_src_type 有多少种
select data_src_type from ods.kesheng_h5_json_ex_v  limit 100;
OK
common
pageVisit
downloadClick

--(3)查看具体数据
select * from ods.kesheng_h5_json_ex_v limit 3;
OK
http://h5.miguvideo.com/mgsp/yx01.jsp?channelId=12345678901	    --  `a1`.`url`
mgsp	                                                        --  ,`a1`.`array_url`[size(`a1`.`array_url`)-2] `product_id`
yx01	                                                        --  , `page_id`
12345678901	                                                    --  , `channel_id`
iOS	                                                            --  ,`a1`.`os` 
1480521763049	                                                --  ,`a1`.`time_stamp` 
common	                                                        --  ,`a1`.`data_src_type` 
{"type":"common"                                                --  ,`a1`.`param_json`
	,"IP":"127.0.0.1"                                                  
	,"cookieId":"asdf435234fqw2323vwe34"                               
	,"title":"migu movie"
	,"domain":"222.66.199.177"
	,"referrer":"222.66.199.177"
	,"width":"360"
	,"height":"640"
	,"lang":"zh-CN"}	
20170208	                                                    --   ,`a1`.`src_file_day`
15                                                              --   ,`a1`.`src_file_hour

h5.miguvideo.com/mgzb/yx02.jsp?channelId=22345678901	
mgzb	
yx02	
22345678901	
iOS	
1480521763049	
pageVisit	
{"type":"pageVisit"
	,"cookieId":"asdf435234fqw2323vwe34"
	,"IP":"127.0.0.1"
	,"startTime":"1480521763049"
	,"endTime":"1480521763049"
	,"visitTime":234}	
20170208	
15

http://127.0.0.1/migu/aa.jsp	
migu	
aa		
iOS	
1480521763049	
downloadClick	
{"type":"downloadClick"
	,"description":"xxxxxx"
	,"aaa":"bbb"}	
20170208	
15







