#!/bin/bash

start_month=$1
end_month=$2


hive -v -e "
INSERT INTO TABLE eyes.AppRetentionCount
SELECT
    concat(max(s.this_month),'_',s.type,'_',s.id) id,
    TO_MONGO_DATE_TIME('$end_month'),
    from_unixtime(unix_timestamp()),
    if(s.type=1, s.id, NULL) AppSettingID,
    if(s.type=1, NULL, s.id) MallID,
    s.type,
    collect_all(list)
FROM (
    SELECT 
        l.id id,l.type type,l.this_month this_month,l.pre_month pre_month,
        STRUCT(
            TO_MONGO_DATE_TIME(l.pre_month),
            l.pre_uuid,
            COALESCE(l.retention, 0),
            COALESCE(round(l.retention/l.pre_uuid, 4), 0),
            COALESCE(round(l.retention/l.this_uuid, 4), 0)
        ) list
    FROM (
        SELECT
            contrast.id id,
            contrast.type type,
            count(DISTINCT contrast.this_uuid),
            count(DISTINCT contrast.pre_uuid) pre_uuid,
            count(DISTINCT if(contrast.this_uuid IS NOT NULL AND contrast.pre_uuid IS NOT NULL, contrast.this_uuid, NULL)) retention,
            contrast.this_month this_month,
            contrast.pre_month pre_month,
            FIRST_VALUE(count(DISTINCT contrast.this_uuid)) OVER(PARTITION BY contrast.id,contrast.type ORDER BY COALESCE(contrast.pre_month,'000000')) this_uuid
        FROM (
            SELECT COALESCE(last.id, new.id) id,
                   COALESCE(last.type, new.type) type,
                   COALESCE(last.month,'$end_month') this_month,
                   new.month pre_month,
                   last.uuid this_uuid,
                   new.uuid pre_uuid
            FROM (
                SELECT DISTINCT a.uuid uuid,a.type type,if(a.type=1, m.AppSettingID, m.newmallid) id,a.month month
                FROM mongo.mall m JOIN (
                    SELECT uuid,type,mallid,DATE_PARSE(date,3) month
                    FROM appuuid_all
                    WHERE DATE_PARSE(date,3)= $end_month
                ) a ON a.mallid=m.mallid
                WHERE if(a.type=1, m.AppSettingID, a.newmallid) IS NOT NULL 
            ) last FULL JOIN (
                SELECT DISTINCT
                    CASE 
                        WHEN ta.ContainerSource=1 OR ta.ContainerSource=2 THEN 1
                        WHEN ta.ContainerSource=5 THEN 2
                        WHEN ta.ContainerSource=3 THEN 3
                        WHEN ta.ContainerSource=7 THEN 5    --增加口碑数据                          
                    END Type,
                    CASE 
                        WHEN ta.ContainerSource=1 OR ta.ContainerSource=2 THEN AppSettingID
                        WHEN ta.ContainerSource=5 OR ta.ContainerSource=3 OR ta.ContainerSource=7 THEN tb.newmallid  --增加口碑数据     
                    END id,
                    ta.uuid,
                    DATE_PARSE(ta.date,3) month
                FROM qmiddle.mq_newuuid ta
                left join (select mallid, newmallid from mongo.mall) tb
                   on ta.mallid = tb.mallid  
                WHERE DATE_PARSE(ta.date,3)>= $start_month
                  AND DATE_PARSE(ta.date,3)<= $end_month
                  AND ta.ContainerSource in (1,2,3,5,7)   --增加口碑数据     
                  AND ta.AppSettingID IS NOT NULL
            ) new ON last.uuid=new.uuid AND last.type=new.type AND last.id=new.id
        ) contrast
        WHERE contrast.pre_month IS NOT NULL
        GROUP BY id,contrast.type,contrast.pre_month,contrast.this_month
        GROUPING SETS (
            (id,contrast.type,contrast.this_month),
            (id,contrast.type,contrast.pre_month,contrast.this_month)
        )
    ) l
    WHERE l.pre_month IS NOT NULL
) s
GROUP BY s.type,s.id;
"
hive -v -e "
INSERT INTO TABLE eyes.AppRetentionCount
SELECT
    concat(max(s.this_month),'_4_',s.id) id,
    TO_MONGO_DATE_TIME('$end_month'),
    from_unixtime(unix_timestamp()),
    NULL AppSettingID,
    s.id MallID,
    4 type,
    collect_all(list)
FROM (
    SELECT 
        l.id id,l.this_month this_month,l.pre_month pre_month,
        STRUCT(
            TO_MONGO_DATE_TIME(l.pre_month),
            l.pre_uuid,
            COALESCE(l.retention, 0),
            COALESCE(round(l.retention/l.pre_uuid, 4), 0),
            COALESCE(round(l.retention/l.this_uuid, 4), 0)
        ) list
    FROM (
        SELECT
            contrast.id id,
            count(DISTINCT contrast.this_uuid),
            count(DISTINCT contrast.pre_uuid) pre_uuid,
            count(DISTINCT if(contrast.this_uuid IS NOT NULL AND contrast.pre_uuid IS NOT NULL, contrast.this_uuid, NULL)) retention,
            contrast.this_month this_month,
            contrast.pre_month pre_month,
            FIRST_VALUE(count(DISTINCT contrast.this_uuid)) OVER(PARTITION BY contrast.id ORDER BY COALESCE(contrast.pre_month,'000000')) this_uuid
        FROM (
            SELECT 
                COALESCE(last.id, new.id) id,
                COALESCE(last.month,'$end_month') this_month,
                new.month pre_month,
                last.uuid this_uuid,
                new.uuid pre_uuid
            FROM (
                SELECT DISTINCT a.uuid,
                       b.newmallid id,
                       DATE_PARSE(a.date,3) month
                FROM appuuid_all a
                left join (select mallid, newmallid from mongo.mall) b
                  on a.mallid = b.mallid  
                WHERE DATE_PARSE(a.date,3)= $end_month 
                  AND b.newmallid IS NOT NULL 
                  AND type=1
            ) last FULL JOIN (
               select distinct tb.newmallid id,
                       ta.uuid,
                       date_parse(ta.date,3) month
                from eyes.mallid_uuid ta
                left join (select mallid, newmallid from mongo.mall) tb
                  on ta.mallid = tb.mallid  
               where date_parse(ta.date,3) >= $start_month 
                 and date_parse(ta.date,3) <= $end_month 
                 and ta.containersource in (1,2)
            ) new ON last.uuid=new.uuid  AND last.id=new.id
        ) contrast
        WHERE contrast.pre_month IS NOT NULL
        GROUP BY id,contrast.pre_month,contrast.this_month
        GROUPING SETS (
            (id,contrast.this_month),
            (id,contrast.pre_month,contrast.this_month)
        )
    ) l
    WHERE l.pre_month IS NOT NULL
) s
GROUP BY s.id;
"