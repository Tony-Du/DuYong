
create table rptdata.fact_cdn_flow_daily (
    flow_000001 decimal(38,4),
    flow_000002 decimal(38,4),
    flow_000003 decimal(38,4),
    flow_000004 decimal(38,4),
    flow_000005 decimal(38,4),
    flow_000006 decimal(38,4),
    flow_000007 decimal(38,4),
    flow_000008 decimal(38,4),
    flow_000009 decimal(38,4),
    flow_000010 decimal(38,4),
    flow_000011 decimal(38,4),
    flow_000012 decimal(38,4),
    flow_000013 decimal(38,4),
    flow_000014 decimal(38,4),
    flow_000015 decimal(38,4)
) 
partitioned by (src_file_day string);