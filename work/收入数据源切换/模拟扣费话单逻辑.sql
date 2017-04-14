
--总共3个过程，数据源有 td_aaa_bill_d, td_aaa_order_d, td_aaa_order_log_d，目标表有 tm_month_fee_exception, tm_month_fee_info_d，一个过程更新month_fee_exception，另两个分别根据order_d, order_log_d更新month_fee_info_d
--每月月末计算上月，是因为boss扣费话单在每月15号之后才会提供上月

create or replace procedure cdmp_mk.pm_month_fee_exception_m
(
    iv_month    in  varchar2,
    oi_return out integer
)

/** head
  * @name cdmp_mk#pm_month_fee_exception_m
  * @caption mk层包月费用异常表
  * @type 日汇总
  * @parameter iv_month in varchar2 统计日期，格式：yyyymm
  * @parameter oi_return out integer 执行状态码，0 正常，其它 出错
  * @description mk层包月费用异常表
  * @target cdmp_mk#tm_month_fee_exception 异常表
  * @source cdmp_dw#td_aaa_bill_d 包月收入估算表
  * @middle
  * @version 1.0.0
  * @author guorui
  * @create-date 20141017
  * @todo <工作计划>
  * @version 1.01
  * @copyright 上海移动视频基地经分系统
  */

is
  vi_task_id    integer;      -- 任务日志id
  vv_task_name  varchar2(30); -- 任务名
  vv_table_name varchar2(30); -- 表名
  vv_task_pos   varchar2(50); -- 任务位置

  vi_err_code integer;        -- 出错代码
  vv_err_msg  varchar2(200);  -- 出错信息

  vi_result integer;          -- 临时结果
  vi_count  integer;          -- 计数参数

  exc_return exception;       -- 程序中间返回自定义异常
  exc_error exception;        -- 程序出错返回自定义异常

begin

    execute immediate 'alter session enable parallel dml';
    --变量初始化
    vv_task_name  := 'pm_month_fee_exception_m';
    vv_table_name := 'tm_month_fee_exception';

    vv_task_pos := '程序开始日志';
    cdmp_dw.psc_sys_log(vi_task_id, 1, vv_task_name, vv_table_name, null, vv_err_msg, vv_task_pos, vi_result);

    vv_task_pos := '检查输入参数是否正确';
    if (iv_month is null) then
      vi_err_code := -1;
      vv_err_msg  := '没有输入统计日期参数';
      raise exc_error;
    end if;

    vv_task_pos := '检查数据源是否存在';
    vi_count := fun_check_table('cdmp_mk', 'pm_month_fee_exception_m', iv_month, vv_err_msg);

    if vi_count <> 0 then
      vi_err_code := -2;
      raise exc_error;
    end if;

    delete from cdmp_mk.tm_month_fee_exception_bak t where t.statis_month = iv_month;	--删除符合where条件的所有字段的数据

    vv_task_pos := '备份异常包月费用表';
    insert into cdmp_mk.tm_month_fee_exception_bak
    (
           statis_month,
           serv_number,
           product_id,
           is_add,
           insert_month
    )
    select iv_month,
           serv_number,
           product_id,
           is_add,
           insert_month
      from cdmp_mk.tm_month_fee_exception;

    vv_task_pos := '包月费用异常表';
    insert into cdmp_mk.tm_month_fee_exception
    (
            serv_number,
            product_id,
            is_add,
            insert_month
    )
    select /*+ parallel(a,16) */ serv_number,	--设置并行
            product_id,
            is_add,
            iv_month
      from cdmp_mk.tm_month_fee_info_d a
     where a.statis_day between iv_month||'01'
       and to_char(last_day(to_date(iv_month,'yyyymm')),'yyyymmdd')
       and not exists (select /*+ parallel(t,16) */1				--该子查询where后面的条件
                           from cdmp_dw.td_aaa_bill_d t
                          where t.opt_type = '4'
                            and t.statis_day between iv_month||'01'
                            and to_char(last_day(to_date(iv_month,'yyyymm')),'yyyymmdd')
                            and bill_fee >0
                            and t.charge_type = '0'		--包月
                            and t.serv_number = a.serv_number
                            and t.product_id = a.product_id)
       and not exists (select 1 from cdmp_mk.tm_month_fee_exception b
                        where b.serv_number = a.serv_number
                          and b.product_id = a.product_id );
    commit;
    delete /*+ parallel(a,16) */from cdmp_mk.tm_month_fee_exception a
     where exists (select /*+ parallel(a,50) */1
                     from cdmp_dw.td_aaa_bill_d t
                    where t.opt_type = '4'
                      and t.statis_day between iv_month||'01'
                      and to_char(last_day(to_date(iv_month,'yyyymm')),'yyyymmdd')
                      and bill_fee >0
                      and t.charge_type = '0'
                      and t.serv_number = a.serv_number
                      and t.product_id = a.product_id);
	--exists: 强调的是是否返回结果集，不要求知道返回什么, 比如：
	--select name from student where sex = 'm' and mark exists(select 1 from grade where ...) ,只要
	--exists引导的子句有结果集返回，那么exists这个条件就算成立了,大家注意返回的字段始终为1，如果改成“select 2 from grade where ...”，那么返回的字段就是2，这个数字没有意义。
	--所以exists子句不在乎返回什么，而是在乎是不是有结果集返回。

    --得到数据量
    vi_result := sql%rowcount;		--sql%rowcount中的sql是oracle的内部游标,rowcount的意思是之前的dml sql语句影响的数据有多少行
    --提交事务
    commit;

    vv_task_pos := '程序结束日志';
    cdmp_dw.psc_sys_log(vi_task_id, 0, null, null, null, null, null, vi_result);
    oi_return := 0;

exception
    when exc_return then
        /** @description 程序中间返回，记录程序结束日志，正常返回
          * @call cdmp_dw#psc_sys_log
          * @field-mapping oi_return = (0)
          */
        cdmp_dw.psc_sys_log(vi_task_id, vi_err_code, null, null, null, vv_err_msg, vv_task_pos, vi_result);
        oi_return := 0;

    when exc_error then
        /** @description 程序出错返回自定义异常，记录程序出错日志，出错返回
          * @call cdmp_dw#psc_sys_log
          * @field-mapping oi_return = (vi_err_code)
          */
        rollback;
        cdmp_dw.psc_sys_log(vi_task_id, vi_err_code, null, null, null, vv_err_msg, vv_task_pos, vi_result);
        oi_return := vi_err_code;

    when others then
        /** @description 程序出错，得到出错信息，回滚事务，记录程序出错日志，出错返回
          * @field-mapping vi_err_code = (sqlcode)
          * @field-mapping vv_err_msg = 取前200个字符(sqlerrm)
          * @call cdmp_dw#psc_sys_log
          * @field-mapping oi_return = (vi_err_code)
          */
        vi_err_code := sqlcode;
        vv_err_msg  := substr(sqlerrm, 1, 200);
        rollback;
        cdmp_dw.psc_sys_log(vi_task_id, vi_err_code, null, null, null, vv_err_msg, vv_task_pos, vi_result);
        oi_return := vi_err_code;
end pm_month_fee_exception_m;


===================================================================================================================================================================
===================================================================================================================================================================


--每天跑，如果在order_log_d中有，且订购关系不在异常表中，则放入模拟扣费表

create or replace procedure cdmp_mk.pm_month_fee_order_log_d
(
    iv_day    in  varchar2,
    oi_return out integer
)

/** head
  * @name cdmp_mk#pm_month_fee_order_log_d
  * @caption mk层订购日志估算收入
  * @type 日汇总
  * @parameter iv_day in varchar2 统计日期，格式：yyyymmdd
  * @parameter oi_return out integer 执行状态码，0 正常，其它 出错
  * @description mk层订购日志估算收入
  * @target cdmp_mk#tm_month_fee_info_d 包月收入估算表
  * @source cdmp_mk#tm_user_order_log_d 订购日志表
  * @middle
  * @version 1.0.0
  * @author guorui
  * @create-date 20141017
  * @todo <工作计划>
  * @version 1.01
  * @copyright 上海移动视频基地经分系统
  */

is
  vi_task_id    integer;      -- 任务日志id
  vv_task_name  varchar2(30); -- 任务名
  vv_table_name varchar2(30); -- 表名
  vv_task_pos   varchar2(50); -- 任务位置

  vi_err_code integer;        -- 出错代码
  vv_err_msg  varchar2(200);  -- 出错信息

  vi_result integer;          -- 临时结果
  vi_count  integer;          -- 计数参数

  exc_return exception;       -- 程序中间返回自定义异常
  exc_error exception;        -- 程序出错返回自定义异常

begin

    execute immediate 'alter session enable parallel dml';
    --变量初始化
    vv_task_name  := 'pm_month_fee_order_log_d';
    vv_table_name := 'tm_month_fee_info_d';

    vv_task_pos := '程序开始日志';
    cdmp_dw.psc_sys_log(vi_task_id, 1, vv_task_name, vv_table_name, iv_day, vv_err_msg, vv_task_pos, vi_result);

    vv_task_pos := '检查输入参数是否正确';
    if (iv_day is null) then
      vi_err_code := -1;
      vv_err_msg  := '没有输入统计日期参数';
      raise exc_error;
    end if;

    vv_task_pos := '检查数据源是否存在';
    vi_count := fun_check_table('cdmp_mk', 'pm_month_fee_order_log_d', iv_day, vv_err_msg);

    if vi_count <> 0 then
      vi_err_code := -2;
      raise exc_error;
    end if;

    vv_task_pos := '清空旧数据、建分区';
    select count(1)
      into vi_result
      from all_tab_partitions t where t.partition_name = 'PART_'||iv_day
       and t.table_name = upper(vv_table_name);


    if vi_result = 1 then

            delete from cdmp_mk.tm_month_fee_info_d t where t.statis_day =  iv_day and is_add = '1';
            delete from cdmp_mk.tm_month_fee_info_d t where t.statis_day =  iv_day and is_add = '0' and data_source_flag = '1';



    else
    
    end if;

    vv_task_pos := '处理订购日志用户';
    insert into cdmp_mk.tm_month_fee_info_d nologging
    (
           statis_day,
           prov_id,
           region_id,
           serv_number,
           product_id,
           bill_fee,
           business_id,
           sub_busi_id,
           chn_id_new,
           version_id,
           term_prod_id,
           is_add,
           new_product_id,
           fee_time
    )
    select /*+ parallel(t3,16) */statis_day,
           prov_id,
           region_id,
           serv_number,
           product_id,
           product_price,
           business_id,
           sub_busi_id,
           chn_id_new,
           version_id,
           term_prod_id,
           is_add,
           new_product_id,
           opr_time
     from (select /*+ parallel(t,16) */iv_day statis_day,
                   t.prov_id,
                   t.region_id,
                   t.serv_number,
                   t.product_id,
                   t1.product_price,
                   t.business_id,
                   t.sub_busi_id,
                   t.chn_id_new,
                   t.version_id,
                   t.term_prod_id,
                   t.opr_time,
                   '1' is_add,
                   row_number() over(partition by t.serv_number,t.product_id order by t.opr_time) rnum,
                   t.new_product_id
              from cdmp_dw.td_aaa_order_log_d t,cdmp_dw.td_pms_product_d t1
             where t.statis_day = iv_day
               and t.order_type = '1'
               and t1.product_price > 0
               and t.substatus is null
               and t.business_id not in (select aa.business_id
                                           from cdmp_ods.to_business_info aa
                                          where aa.business_class = '9')
               and t.product_id = t1.product_id
               and not exists (select 1 from cdmp_mk.tm_month_fee_exception t3
                                where t.serv_number = t3.serv_number
                                  and t.product_id = t3.product_id )
               and not exists (select 1 from cdmp_mk.tm_month_fee_info_d t2
                                where t2.statis_day between substr(iv_day,1,6)||'01' and to_char(to_date(iv_day,'yyyymmdd')-1,'yyyymmdd')
                                  and t.serv_number = t2.serv_number
                                  and t.product_id = t2.product_id)) t3
     where t3.rnum = 1;
     commit;

    vv_task_pos := '处理锁定用户';
    insert into cdmp_mk.tm_month_fee_info_d nologging
    (
           statis_day,
           prov_id,
           region_id,
           serv_number,
           product_id,
           bill_fee,
           business_id,
           sub_busi_id,
           chn_id_new,
           version_id,
           term_prod_id,
           is_add,
           new_product_id,
           fee_time,
           data_source_flag
    )
    select /*+ parallel(t,16) */iv_day statis_day,
           t.prov_id,
           t.region_id,
           t.serv_number,
           t.product_id,
           t1.product_price,
           t.business_id,
           t.sub_busi_id,
           t.chn_id_new,
           t.version_id,
           t.term_prod_id,
           '0' is_add,
           new_product_id,
           last_order_time,
           '1' data_source_flag
      from cdmp_dw.td_aaa_order_fact_d t,cdmp_dw.td_pms_product_d t1
     where t.statis_day = to_char(to_date(iv_day,'yyyymmdd') - 1,'yyyymmdd')
       and t.order_status = '5'
       and t.substatus is null
       and t.product_id = t1.product_id
       and t.business_id not in (select aa.business_id
                                   from cdmp_ods.to_business_info aa
                                  where aa.business_class = '9')
       and t1.product_price > 0
       and not exists (select 1 from cdmp_mk.tm_month_fee_info_d t2
                        where t2.statis_day between substr(iv_day,1,6)||'01' and to_char(to_date(iv_day,'yyyymmdd')-1,'yyyymmdd')
                          and t.serv_number = t2.serv_number
                          and t.product_id = t2.product_id )
       and (t.serv_number,t.product_id) in (select t3.serv_number,t3.product_id
                                              from cdmp_dw.td_aaa_order_log_d t3
                                             where t3.statis_day = iv_day
                                               and t3.order_type = '5')
       and not exists (select 1 from cdmp_mk.tm_month_fee_exception t3
                        where t.serv_number = t3.serv_number
                          and t.product_id = t3.product_id);

    --得到数据量
    vi_result := sql%rowcount;
    --提交事务
    commit;

    vv_task_pos := '程序结束日志';
    cdmp_dw.psc_sys_log(vi_task_id, 0, null, null, null, null, null, vi_result);
    oi_return := 0;

exception
    when exc_return then
        /** @description 程序中间返回，记录程序结束日志，正常返回
          * @call cdmp_dw#psc_sys_log
          * @field-mapping oi_return = (0)
          */
        cdmp_dw.psc_sys_log(vi_task_id, vi_err_code, null, null, null, vv_err_msg, vv_task_pos, vi_result);
        oi_return := 0;

    when exc_error then
        /** @description 程序出错返回自定义异常，记录程序出错日志，出错返回
          * @call cdmp_dw#psc_sys_log
          * @field-mapping oi_return = (vi_err_code)
          */
        rollback;
        cdmp_dw.psc_sys_log(vi_task_id, vi_err_code, null, null, null, vv_err_msg, vv_task_pos, vi_result);
        oi_return := vi_err_code;

    when others then
        /** @description 程序出错，得到出错信息，回滚事务，记录程序出错日志，出错返回
          * @field-mapping vi_err_code = (sqlcode)
          * @field-mapping vv_err_msg = 取前200个字符(sqlerrm)
          * @call cdmp_dw#psc_sys_log
          * @field-mapping oi_return = (vi_err_code)
          */
        vi_err_code := sqlcode;
        vv_err_msg  := substr(sqlerrm, 1, 200);
        rollback;
        cdmp_dw.psc_sys_log(vi_task_id, vi_err_code, null, null, null, vv_err_msg, vv_task_pos, vi_result);
        oi_return := vi_err_code;
end pm_month_fee_order_log_d;


===================================================================================================================================================================
===================================================================================================================================================================


--每月第一天算上月末在订用户带来收入

create or replace procedure cdmp_mk.pm_month_fee_order_m
(
    iv_month    in  varchar2,
    oi_return out integer
)

/** head
  * @name cdmp_mk#pm_month_fee_order_m
  * @caption mk层订购关系估算收入
  * @type 日汇总
  * @parameter iv_month in varchar2 统计日期，格式：yyyymmdd
  * @parameter oi_return out integer 执行状态码，0 正常，其它 出错
  * @description mk层订购关系估算收入
  * @target cdmp_mk#tm_month_fee_info_d 包月收入估算表
  * @source cdmp_dw#td_aaa_order_fact_d 订购关系实际收费表
  * @middle
  * @version 1.0.0
  * @author guorui
  * @create-date 20141017
  * @todo <工作计划>
  * @version 1.01
  * @copyright 上海移动视频基地经分系统
  */

is
  vi_task_id    integer;      -- 任务日志id
  vv_task_name  varchar2(30); -- 任务名
  vv_table_name varchar2(30); -- 表名
  vv_task_pos   varchar2(50); -- 任务位置

  vi_err_code integer;        -- 出错代码
  vv_err_msg  varchar2(200);  -- 出错信息

  vi_result integer;          -- 临时结果
  vi_count  integer;          -- 计数参数

  exc_return exception;       -- 程序中间返回自定义异常
  exc_error exception;        -- 程序出错返回自定义异常

  v_net_month varchar2(8);
begin

    execute immediate 'alter session enable parallel dml';
    --变量初始化
    vv_task_name  := 'pm_month_fee_order_m';
    vv_table_name := 'tm_month_fee_info_d';
    v_net_month := to_char(add_months(to_date(iv_month,'yyyymm'),1),'yyyymmdd');
    vv_task_pos := '程序开始日志';
    cdmp_dw.psc_sys_log(vi_task_id, 1, vv_task_name, vv_table_name, iv_month, vv_err_msg, vv_task_pos, vi_result);

    vv_task_pos := '检查输入参数是否正确';
    if (iv_month is null) then
      vi_err_code := -1;
      vv_err_msg  := '没有输入统计日期参数';
      raise exc_error;
    end if;

    vv_task_pos := '检查数据源是否存在';
    vi_count := fun_check_table('cdmp_mk', 'pm_month_fee_order_m', iv_month, vv_err_msg);

    if vi_count <> 0 then
      vi_err_code := -2;
      raise exc_error;
    end if;

    vv_task_pos := '清空旧数据、建分区';
    select count(1)
      into vi_result
      from all_tab_partitions t where t.partition_name = 'PART_'||v_net_month
       and t.table_name = upper(vv_table_name);

    if vi_result = 1 then
        delete from cdmp_mk.tm_month_fee_info_d t where t.statis_day =  v_net_month and is_add = '0' and data_source_flag = '2';
    else
    
    end if;


    (
           statis_day,
           prov_id,
           region_id,
           serv_number,
           product_id,
           bill_fee,
           business_id,
           sub_busi_id,
           chn_id_new,
           version_id,
           term_prod_id,
           is_add,
           new_product_id,
           fee_time,
           data_source_flag
    )
    select v_net_month statis_day,
           t.prov_id,
           t.region_id,
           t.serv_number,
           t.product_id,
           t1.product_price,
           t.business_id,
           t.sub_busi_id,
           t.chn_id_new,
           t.version_id,
           t.term_prod_id,
           '0' is_add,
           new_product_id,
           last_order_time,
           '2' data_source_flag
      from cdmp_dw.td_aaa_order_fact_d t,cdmp_dw.td_pms_product_d t1
     where t.statis_day = to_char(last_day(to_date(iv_month,'yyyymm')),'yyyymmdd')
       and t.order_status = '1'
       and t.substatus is null
       and t.order_expire_date='20381231235959' --add on 20161228
       and t.product_id = t1.product_id
       and t1.product_price > 0
       and t.business_id not in (select aa.business_id
                                   from cdmp_ods.to_business_info aa
                                  where aa.business_class = '9')
       and not exists (select 1 from cdmp_mk.tm_month_fee_exception t3
                        where t.serv_number = t3.serv_number
                          and t.product_id = t3.product_id);
    commit;
    --得到数据量
    vi_result := sql%rowcount;
    --提交事务
    commit;

    vv_task_pos := '程序结束日志';
    cdmp_dw.psc_sys_log(vi_task_id, 0, null, null, null, null, null, vi_result);
    oi_return := 0;

exception
    when exc_return then
        /** @description 程序中间返回，记录程序结束日志，正常返回
          * @call cdmp_dw#psc_sys_log
          * @field-mapping oi_return = (0)
          */
        cdmp_dw.psc_sys_log(vi_task_id, vi_err_code, null, null, null, vv_err_msg, vv_task_pos, vi_result);
        oi_return := 0;

    when exc_error then
        /** @description 程序出错返回自定义异常，记录程序出错日志，出错返回
          * @call cdmp_dw#psc_sys_log
          * @field-mapping oi_return = (vi_err_code)
          */
        rollback;
        cdmp_dw.psc_sys_log(vi_task_id, vi_err_code, null, null, null, vv_err_msg, vv_task_pos, vi_result);
        oi_return := vi_err_code;

    when others then
        /** @description 程序出错，得到出错信息，回滚事务，记录程序出错日志，出错返回
          * @field-mapping vi_err_code = (sqlcode)
          * @field-mapping vv_err_msg = 取前200个字符(sqlerrm)
          * @call cdmp_dw#psc_sys_log
          * @field-mapping oi_return = (vi_err_code)
          */
        vi_err_code := sqlcode;
        vv_err_msg  := substr(sqlerrm, 1, 200);
        rollback;
        cdmp_dw.psc_sys_log(vi_task_id, vi_err_code, null, null, null, vv_err_msg, vv_task_pos, vi_result);
        oi_return := vi_err_code;
end pm_month_fee_order_m;
