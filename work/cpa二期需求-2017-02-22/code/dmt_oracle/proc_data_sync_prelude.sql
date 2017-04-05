


create or replace procedure proc_data_sync_prelude(v_table_name in varchar2, v_src_file_day in varchar2)
/*==========================================================================+
名    称: proc_data_sync_prelude
内容摘要: 检查指定日期分区是否存在,不存在则创建;最后truncate分区
调    用: 无
被 调 用: 
创 建 者: gongjianhui
创建日期: 2017-03-26
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
     and t1.partition_name = 'P_'||v_src_file_day;

  if vs_part_cnt = 0 then
    vs_sql := 'alter table '|| v_table_name ||' split partition p_max '
             ||' at('|| to_char(to_date(v_src_file_day,'yyyymmdd')+1, 'yyyymmdd') ||')'
             ||' into(partition p_'|| v_src_file_day ||', partition p_max)';
  execute immediate vs_sql;
  end if;

  vs_sql := 'alter table '|| v_table_name ||' truncate partition p_'||v_src_file_day;
  execute immediate vs_sql;

end;
/


-- 如果我在表上增加个分区，则Oracle会自动维护分区的索引,注意此时加分区必须是用split,直接加会出错的。
--  alter table test split partition p3 at (30000) into (partition p3, partition p4);































create or replace procedure hive_oracle_partitions_tab (hiv_table_name in varchar2(64), src_file_day in varchar2(64))
as 
v_par_cnt number(8),
v_sql varchar2(64)
begin 
select count(1) into v_par_cnt 
from user_tab_partitions t1 
where t1.table_name = hiv_table_name
  and t1.partition_name = 'P_'||src_file_day;

if v_par_cnt = 0 then 

  
  
  
  
  

