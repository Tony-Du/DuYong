


create or replace procedure proc_data_sync_prelude_monthly(v_table_name in varchar2, v_src_file_month in varchar2)	
/*==========================================================================+								
名    称: proc_data_sync_prelude_monthly
内容摘要: 检查指定日期分区是否存在,不存在则创建;最后truncate分区
调    用: 无
被 调 用: 
创 建 者: duyong
创建日期: 2017-08-15
更改历史：
    更改日期     更改人      更改说明
+===========================================================================*/
as
  vs_part_cnt number(6);
  vs_sql      varchar2(500);
begin
  select count(1) into vs_part_cnt
    from user_tab_partitions t1
   where t1.table_name = upper(v_table_name)
     and t1.partition_name = 'P_'||v_src_file_month;

  if vs_part_cnt = 0 then
    vs_sql := 'alter table '|| v_table_name ||' split partition p_max '                               --v_table_name: cpa_event_occur_daily 
             ||' at('|| to_char(add_months(to_date(v_src_file_month,'yyyymm'),1), 'yyyymm') ||')'     --v_src_file_day: 201705
             ||' into(partition p_'|| v_src_file_month ||', partition p_max)';
  execute immediate vs_sql;
  end if;

  vs_sql := 'alter table '|| v_table_name ||' truncate partition p_'||v_src_file_month;
  execute immediate vs_sql;

end;















