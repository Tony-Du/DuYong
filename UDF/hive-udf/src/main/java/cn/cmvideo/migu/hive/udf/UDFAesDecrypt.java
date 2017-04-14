package cn.cmvideo.migu.hive.udf;

import cn.cmvideo.migu.hive.udf.utils.AesUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Created by Smart on 2016/11/23.
 */
public class UDFAesDecrypt extends UDF {
    private final Text result = new Text();

    /**
     * 按默认密钥解密
     */
    public Text evaluate(Text n) {
        if (n == null) {
            return null;
        }

        String plainText = AesUtils.aesDecrypt(n.toString());
        result.set(plainText);
        return result;
    }

    /**
     * 按指定密钥解密
     */
    public Text evaluate(Text n, String aesToken) {
        if (n == null) {
            return null;
        }

        String plainText = AesUtils.aesDecrypt(n.toString(), aesToken);
        result.set(plainText);
        return result;
    }

}
