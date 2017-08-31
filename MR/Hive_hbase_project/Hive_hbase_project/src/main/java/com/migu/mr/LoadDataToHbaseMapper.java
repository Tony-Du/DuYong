package com.migu.mr;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.UUID;

/**
 * Created by admin on 2017/1/6.
 */
public class LoadDataToHbaseMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable,Put>{
    //map
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] splits = line.split("\\|");
            String rowKey = createRowKey(splits);
            String familyName = CONSTANT.FAMILY_NAME;
            String qualifierName = CONSTANT.QUALIFIER_NAME;


            ImmutableBytesWritable hkey = new ImmutableBytesWritable(Bytes.toBytes(rowKey));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(qualifierName),Bytes.toBytes(line));
            context.write(hkey,put);

    }

    //create rowkey
    public static String createRowKey(String[] splits){
        String serv_number = reverse(splits[2]);
        String statis_day = splits[0];
        //uuid加密(MD5)，密文是数字和小写字母，取前三位后三位;
        String randomStr = UUID.randomUUID().toString();
        String MD5Str = toMD5(randomStr);
        String rowKey = serv_number + statis_day + MD5Str;
        return rowKey;
    }

    //reverse
    public static String reverse(String s){

        return new StringBuffer(s).reverse().toString();
    }
    //toMD5
    public static String toMD5(String plainText){
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(plainText.getBytes());
            byte[] b = md.digest();
            int i;
            StringBuffer buf = new StringBuffer("");
            for (int offset = 0; offset < b.length; offset++) {
                i = b[offset];
                if (i < 0)
                    i += 256;
                if (i < 16)
                    buf.append("0");
                buf.append(Integer.toHexString(i));
            }
            return buf.toString().substring(0, 6) + buf.toString().substring(26, 32);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
