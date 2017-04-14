package cn.cmvideo.migu.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UDFisUTF8 extends UDF {

    private final BooleanWritable result = new BooleanWritable();

    private static Boolean isUTF8(String string) {
        Pattern encode_pattern = Pattern.compile("^(?:[\\x00-\\x7f]|[\\xe0-\\xef][\\x80-\\xbf]{2})+$");
        Matcher encode_matcher = encode_pattern.matcher(string);
        return encode_matcher.matches();
    }

    public BooleanWritable evaluate(Text n) {
        if (n == null) {
            return null;
        }
        result.set(isUTF8(n.toString()));
        return result;
    }

}
