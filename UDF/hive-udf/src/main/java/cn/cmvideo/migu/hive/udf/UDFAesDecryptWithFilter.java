package cn.cmvideo.migu.hive.udf;

import cn.cmvideo.migu.hive.udf.utils.AesUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Created by Smart on 2016/12/9.
 */
public class UDFAesDecryptWithFilter extends UDF {
    private final Text result = new Text();

    /**
     * 按默认密钥解密
     */
    public Text evaluate(Text n, String patten) {
        if (n == null) {
            return null;
        }

        String plainText = AesUtils.aesDecryptwithFilter(n.toString(), patten);
        result.set(plainText);
        return result;
    }
}
