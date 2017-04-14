package cn.cmvideo.migu.hive.udf.utils;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class AesUtils {
    private static final String defaultAesToken = "6fugo5z1x9byouow";
    private static final String ALGORITHM = "AES/CBC/PKCS5Padding";
    private static final byte[] ivb = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f};

    public static String aesEncrypt(String plainText) {
        return aesEncrypt(plainText, defaultAesToken);
    }

    public static String aesDecrypt(String cipherText) {
        return aesDecrypt(cipherText, defaultAesToken);
    }

    public static String aesEncrypt(String plainText, String aesToken) {
        try {
            SecretKeySpec key = new SecretKeySpec(aesToken.getBytes("UTF-8"), "AES");
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            IvParameterSpec iv = new IvParameterSpec(ivb);
            cipher.init(Cipher.ENCRYPT_MODE, key, iv);
            byte[] ciphertext = cipher.doFinal(plainText.getBytes("UTF-8"));
            return byte2hex(ciphertext);
        } catch (Exception e) {
        }
        return "";
    }

    public static String aesDecrypt(String cipherText, String aesToken) {
        try {
            SecretKeySpec key = new SecretKeySpec(aesToken.getBytes("UTF-8"), "AES");
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            IvParameterSpec iv = new IvParameterSpec(ivb);
            cipher.init(Cipher.DECRYPT_MODE, key, iv);
            byte[] plainText = cipher.doFinal(hex2byte(cipherText));
            return new String(plainText, "UTF-8");
        } catch (Exception e) {
        }
        return "";
    }

    public static String aesDecryptwithFilter(String cipherText, String pattern) {
        return aesDecrypt(cipherText).replaceAll(pattern, "");
    }


    private static byte[] hex2byte(String strhex) {
        if (strhex == null) {
            return null;
        }
        int len = strhex.length();
        if (len % 2 == 1) {
            return null;
        }
        byte[] b = new byte[len / 2];
        for (int i = 0; i != len / 2; i++) {
            b[i] = (byte) Integer.parseInt(strhex.substring(i * 2, i * 2 + 2), 16);
        }
        return b;
    }

    private static String byte2hex(byte[] b) {
        String hs = "";
        String stmp;
        for (int n = 0; n < b.length; n++) {
            stmp = (java.lang.Integer.toHexString(b[n] & 0XFF));
            if (stmp.length() == 1) {
                hs = hs + "0" + stmp;
            } else {
                hs = hs + stmp;
            }
        }
        return hs;
    }

    public static void main(String[] args) {
        String ciphertext = "b93a044b6c008b6418abd494f6ebecf835f54145925b42974cf7f97b15109437";
//        String ciphertext = aesDecrypt(aesEncrypt("123  456 \n 789"));

        System.out.println(aesDecryptwithFilter(ciphertext, "\n|\t|\r"));

    }
}
