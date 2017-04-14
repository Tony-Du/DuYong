/**
 * Copyright (c) 1999-2016 科大讯飞 All Rights Reserved.
 * 
 * FileName: ResUtil.java
 * 
 * Description: TODO
 * 
 * History:
 * v1.0.0, weizhang22, Jun 7, 2016, Create
 */
package com.iflytek.gnome.analysis.util;

import java.text.MessageFormat;
import java.util.Locale;
import java.util.ResourceBundle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 资源文件工具类
 * @author weizhang22
 * @date Jun 7, 2016
 *
 */
public class ResUtil {
    /**
     * Log静态变量
     */
    private static Logger log = LoggerFactory.getLogger(ResUtil.class);

    /**
     * jdbc资源文件
     */
    private static ResourceBundle jdbcResource = ResourceBundle.getBundle("jdbc", Locale.getDefault());
    
    private ResUtil()
    {
    }

    /**
     * 获取jdbc.properties内容
     * @param key
     * @param param
     * @return
     */
    public static String getJdbcValue(String key, Object... param)
    {
        String result = key;
        try
        {
            result = jdbcResource.getString(key);
        }
        catch (RuntimeException e)
        {
            logError(jdbcResource, e, key);
        }
        return MessageFormat.format(result, param);
    }
    
    
    private static void logError(ResourceBundle bundle, RuntimeException e, String key)
    {
        StackTraceElement ste = e.getStackTrace()[2];
        log.error("Resource: " + ste.getMethodName() + " not found, key: " + key + " - " + e);
    }
}
