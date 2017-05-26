


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
    vs_sql := 'alter table '|| v_table_name ||' split partition p_max '                   --v_table_name: cpa_event_occur_daily 
             ||' at('|| to_char(to_date(v_src_file_day,'yyyymmdd')+1, 'yyyymmdd') ||')'   --v_src_file_day: 20170501
             ||' into(partition p_'|| v_src_file_day ||', partition p_max)';
  execute immediate vs_sql;
  end if;

  vs_sql := 'alter table '|| v_table_name ||' truncate partition p_'||v_src_file_day;
  execute immediate vs_sql;

end;
/


-- alter table cpa_event_occur_daily split partition p_max at(21070502) into (partition p_20170501, partition p_max);
-- alter table cpa_event_occur_daily truncate partition p_20170501

-- 如果我在表上增加个分区，则Oracle会自动维护分区的索引,注意此时加分区必须是用split,直接加会出错的。
--  alter table test split partition p3 at (30000) into (partition p3, partition p4);
当要在两个已知的分区中创建分区，需要手动创建分区
alter table fact_live_program_rating_daily split partition P_20170426 at(20170426)
into(partition P_20170425, partition P_20170427)


/*
oracle 字符串连接操作字符： ||
(+): Oracle专用的连接符，在条件中出现在左边指右外联接，出现在右边指左外联接。

伪列：在oracle的表的使用过程中，实际表中还有一些附加的列，称为伪列。

ROWID与ROWNUM：
ROWID 是插入记录时生成，ROWNUM 是查询数据时生成。
ROWID 标识的是行的物理地址。ROWNUM标识的是查询结果中的行的次序。


查询出工资最高的前5名员工的姓名、工作和工资。
select ROWNUM, t.* from 
	(select ename, job, sal from EMP order by sal desc) t
where ROWNUM <= 5 ;


检查授予用户的角色
通过查询 user_role_privs 可以检查已经授予一个用户有哪些角色

user_sys_privs 用户系统级权限表
  
role_sys_privs 角色系统级权限表

用ROWID删除重复数据














