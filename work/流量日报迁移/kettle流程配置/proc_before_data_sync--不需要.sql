
create or replace procedure proc_before_data_sync (v_table_name in varchar2, v_src_file_day in varchar2)
/*==========================================================================+								
名    称: proc_before_data_sync
内容摘要: 检查指定日期的数据是否存在,存在则删除
调    用: 无
被 调 用: 
创 建 者: duyong
创建日期: 2017-05-10
更改历史：
    更改日期     更改人      更改说明
+===========================================================================*/
as 
	vs_stat_date_data number(6);
	vs_sql            varchar2(500);
begin
	select count(1) into vs_stat_date_data
	  from v_table_name t1
	 where t1.src_file_day = v_src_file_day;
	 
	if vs_stat_date_data <> 0 then 
	  vs_sql := 'delete from '|| v_table_name ||' t where t.src_file_day = '|| v_src_file_day;
	
	execute immediate vs_sql;
	end if;
end;


