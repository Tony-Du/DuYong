import com.migu.mr.CONSTANT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * Created by admin on 2017/1/5.
 */
public class test {

    public static void main(String[] args) {

        String str = "20161201192039";

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        try {
            Date date = dateFormat.parse(str);
            Long ts = date.getTime();
            System.out.print(ts);
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

}
