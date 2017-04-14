##调用方法

HIVE UDF jar包引用

>
	add jar hdfs://ns1/user/hadoop/hive_udf/migu-udf-1.0-main.jar;
>
	create temporary function migu_AesDecrypt as 'cn.cmvideo.migu.hive.udf.UDFAesDecrypt';
	create temporary function migu_AesEncrypt as 'cn.cmvideo.migu.hive.udf.UDFAesEncrypt';
	create temporary function migu_Crc32 as 'cn.cmvideo.migu.hive.udf.UDFCrc32';
	create temporary function migu_Md5 as 'cn.cmvideo.migu.hive.udf.UDFMd5';
	create temporary function migu_Sha1 as 'cn.cmvideo.migu.hive.udf.UDFSha1';
	create temporary function migu_AesDecryptwithFilter as 'cn.cmvideo.migu.hive.udf.UDFAesDecryptWithFilter';
	create temporary function migu_isUTF8 as 'cn.cmvideo.migu.hive.udf.UDFisUTF8';
	create temporary function migu_getIpAttr as 'cn.cmvideo.migu.hive.udf.UDFGetIPAttribute';
>
	create temporary function migu_UDTFEachTopK as 'cn.cmvideo.migu.hive.udf.UDTFEachTopK'; 

##使用举例
>
	use yanfa;
	select imei_new, migu_Crc32(imei_new), migu_Md5(imei_new), migu_Sha1(imei_new)
	from td_pub_visit_log_d 
	limit 50;



##UDF函数说明

* `migu_AesDecrypt(密文)` //根据cdmp默认密钥解密
* `migu_AesDecrypt(密文, 密钥)` //根据指定密钥解密
* `migu_AesEncrypt(明文)` //根据cdmp默认密钥加密
* `migu_AesEncrypt(明文, 密钥)` //根据指定密钥加密
* `migu_Crc32(字符串)` //使用CRC32函数对字符串进行处理
* `migu_Crc32(字节流)` //使用CRC32函数对字节流进行处理
* `migu_Md5(字符串)` //使用MD5函数对字符串进行处理
* `migu_Md5(字节流)` //使用MD5函数对字节流进行处理
* `migu_Sha1(字符串)` //使用SHA1函数对字符串进行处理
* `migu_Sha1(字节流)` //使用SHA1函数对字节流进行处理
* `migu_AesDecryptwithFilter(密文,需要过滤的正则表达式)` //根据指定密钥解密，并根据填写的正则表达式在结果中过滤相关字符
* `migu_isUTF8(字节流)` //判断是否为乱码，返回false为乱码（即不符合UTF-8格式）
* `migu_getIpAttr(ip地址)` //根据预设的ip对应地址列表返回此ip的属性

##UDTF函数说明
* `migu_UDTFEachTopK(最大行数, 分组字段, 排序字段, 需要一同输出的0-n个字段...)`
>	输出：
>	rownum, 排序字段, 需要一同输出的0-n个字段...
> 
	SELECT
	each_top_k(5, page-id, clicks, page-id, user-id)
	AS (rank, clicks, page-id, user-id)
	FROM (
	  SELECT
	  page-id, user-id, clicks
	  FROM mytable
	  DISTRIBUTE BY page-id SORT BY page-id
	) t1



