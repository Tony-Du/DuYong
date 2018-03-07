

set hive.auto.convert.join=false;
--insert overwrite table qmiddle.mall 
select a.mallid,
       nvl(b.newmallid,a.mallid) as newmallid,
       a.name,
       'Program' as flag
  from mallcoo.mall a
  left join medusa.mallupdate b
    on a.mallid = b.oldmallid 

union 

select mallid,
       mallid as newmallid,
       name,
       city,
       location,
       maptopleftloc,
       mapbottomrightloc,
       floorlist,
       status,
       'Standard' as flag 
  from mallcoo_sp.mall 
 where malltype = 1     --正式的，0为测试的
 
 
 
 
 
 
 
 
 
'/script/isolation/common.sh' 
insert overwrite table qmiddle.mall 
select a.mallid as mallid,
       nvl(b.newmallid,a.mallid) as newmallid,
       a.name,
       a.city,
       a.location,
       a.maptopleftloc,
       a.mapbottomrightloc,
       a.floorlist,
       a.status,
       'Program' as flag
  from mallcoo.mall a
  left join medusa.mallupdate b
    on a.mallid = b.oldmallid

union 

select mallid, 
       mallid as newmallid,
       name,
       city,
       location,
       maptopleftloc,
       mapbottomrightloc,
       floorlist,
       status,
       'Standard' as flag
from mallcoo_sp.mall 
where malltype=1;"




'script/prepare/common.sh'
hive -v -e "
INSERT OVERWRITE TABLE mongo.mall
SELECT m.mallid,
       m.newmallid,
       m.name,
       m.city,
       m.location,
       m.MapTopLeftLoc,
       m.MapBottomRightLoc,
       m.FloorList,
       s.AppSettingID,
       m.flag,
       m.status
FROM qmiddle.mall m 
LEFT JOIN (
    SELECT min(id) AppSettingID,
           idx MallID
    FROM mongo.AppSetting0
    LATERAL VIEW explode(idlist) subview AS idx
    WHERE Category=1
    GROUP BY idx
) s 
ON s.MallID=m.mallid 
where m.status =1;
"