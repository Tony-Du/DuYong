package cn.cmvideo.migu.hive.udf;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class UDFMd5 extends UDF {
    private final Text result = new Text();
    private final MessageDigest digest;

    public UDFMd5() {
        try {
            digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Convert String to md5
     */
    public Text evaluate(Text n) {
        if (n == null) {
            return null;
        }

        digest.reset();
        digest.update(n.getBytes(), 0, n.getLength());
        byte[] md5Bytes = digest.digest();
        String md5Hex = Hex.encodeHexString(md5Bytes);

        result.set(md5Hex);
        return result;
    }

    /**
     * Convert bytes to md5
     */
    public Text evaluate(BytesWritable b) {
        if (b == null) {
            return null;
        }

        digest.reset();
        digest.update(b.getBytes(), 0, b.getLength());
        byte[] md5Bytes = digest.digest();
        String md5Hex = Hex.encodeHexString(md5Bytes);

        result.set(md5Hex);
        return result;
    }
}
