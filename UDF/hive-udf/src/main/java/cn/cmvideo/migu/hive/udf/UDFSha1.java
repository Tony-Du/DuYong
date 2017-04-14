package cn.cmvideo.migu.hive.udf;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class UDFSha1 extends UDF {
    private final Text result = new Text();
    private final MessageDigest digest;

    public UDFSha1() {
        try {
            digest = MessageDigest.getInstance("SHA");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Convert String to SHA-1
     */
    public Text evaluate(Text n) {
        if (n == null) {
            return null;
        }

        digest.reset();
        digest.update(n.getBytes(), 0, n.getLength());
        byte[] shaBytes = digest.digest();
        String shaHex = Hex.encodeHexString(shaBytes);

        result.set(shaHex);
        return result;
    }

    /**
     * Convert bytes to SHA-1
     */
    public Text evaluate(BytesWritable b) {
        if (b == null) {
            return null;
        }

        digest.reset();
        digest.update(b.getBytes(), 0, b.getLength());
        byte[] shaBytes = digest.digest();
        String shaHex = Hex.encodeHexString(shaBytes);

        result.set(shaHex);
        return result;
    }
}
