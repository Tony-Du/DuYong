package cn.cmvideo.migu.hive.udf;

/**
 * Created by HuangYong on 2016/12/26.
 */

import cn.cmvideo.migu.hive.udf.utils.GetConfFileInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;


/*
 * add file hdfs://ns1/user/hadoop/msc/cdn_ip_attribute/mydata4vipday2.dat
 * CREATE TEMPORARY FUNCTION getipattr AS 'cn.cmvideo.migu.hive.udf.UDFGetIPAttribute';
 */

@Description(name = "UDFGetIPAttribute",
        value = "_FUNC_(ip) - Returns attributes of an IP address from a libarar loaded.\n" +
                "The libaray is located on hdfs://ns1/user/hadoop/hive_udf_data/mydata4vipday2.dat \n"
                + "Usage: \n"
                + " > _FUNC_(\"1.14.224.5\")")
public class UDFGetIPAttribute extends UDF {

    private static final HashMap<Long, Long> ipRange = new HashMap<Long, Long>(); //ip段范围，key为起始ip，value为结束ip
    private static final TreeMap<Long, String> ipAttrs = init();    //在加载时初始化，读取ip库数据文件并存入ipAttrs
    private Text result = new Text();

    private static TreeMap<Long, String> init() {
        TreeMap<Long, String> ipAttrs = new TreeMap<Long, String>();
        String ipAttrFilePath = GetConfFileInfo.getConf("ipAttrFilePath");
        BufferedReader reader = null;
        FileSystem hdfs = null;
        String line;
        try {
            ipAttrs.put(-1L, "非法ip\t*\t*\t*");  //用于处理ip解析为-1的情况
            ipRange.put(-1L, Long.MAX_VALUE);
            Configuration conf = new Configuration();
            hdfs = FileSystem.get(URI.create(ipAttrFilePath), conf);
            reader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(ipAttrFilePath)), "UTF-8"));
            while ((line = reader.readLine()) != null) {
                String[] ipAttrStr = line.split("\t");
                if (ipAttrStr.length == 8) {
                    ipAttrs.put(convertIP(ipAttrStr[0]), ipAttrStr[2] + "\t" + ipAttrStr[3] + "\t" + ipAttrStr[4] + "\t" + ipAttrStr[6]);
                    ipRange.put(convertIP(ipAttrStr[0]), convertIP(ipAttrStr[1]));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
//                if (hdfs != null) {
//                    hdfs.close();
//                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return ipAttrs;
    }

    public Text evaluate(Text ip) {
        if (ip == null) {
            return null;
        }

        result.set(getAttrByIp(ip.toString()));
        return result;
    }

    //把IP地址转换成long数值
    private static long convertIP(String ipStr) {
        try {
            long[] ipLongs = new long[4];
            String[] ips = ipStr.split("\\.");
            if (ips.length != 4) {
                return -1L;
            } else {
                for (int i = 0; i < ips.length; i++) {
                    long ipLong = Long.parseLong(ips[i]);
                    if (ipLong > 255L) {
                        return -1L;
                    }
                    ipLongs[i] = ipLong;
                }
            }
            return (ipLongs[0] << 24) + (ipLongs[1] << 16) + (ipLongs[2] << 8) + ipLongs[3];
        } catch (Exception e) {
            return -1L;
        }
    }

    private String getAttrByIp(String ipStr) {
        long ip = convertIP(ipStr);
        Map.Entry<Long, String> entry = ipAttrs.floorEntry(ip);
        long startIpRange = entry.getKey();
        String ipResult = entry.getValue();

        if(ipRange.containsKey(startIpRange)) {
            long endIpRange = ipRange.get(startIpRange);
            if(ip <= endIpRange) {
                return ipResult;
            }
        }
        return "此ip不在地址库范围内\t*\t*\t*";
    }

}
