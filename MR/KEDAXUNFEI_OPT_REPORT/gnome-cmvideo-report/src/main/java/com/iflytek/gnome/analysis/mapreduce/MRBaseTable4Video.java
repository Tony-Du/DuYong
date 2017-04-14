/**
 * Copyright (c) 1999-2016 科大讯飞 All Rights Reserved.
 * 
 * FileName: MRBaseTable4Video.java
 * 
 * Description: 生成视频业务基础表
 * 
 * History: v1.0.0, weizhang22, May 25, 2016, Create
 */
package com.iflytek.gnome.analysis.mapreduce;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
import org.apache.hadoop.thirdparty.guava.common.collect.Sets;

import com.iflytek.gnome.analysis.entity.TD_AAA_BILL_D;
import com.iflytek.gnome.analysis.entity.TD_AAA_TOURIST_BILL_D;
import com.iflytek.gnome.analysis.entity.TD_CMS_CONTENT_CLASS_D;
import com.iflytek.gnome.analysis.entity.TD_CMS_CONTENT_D;

/**
 * 生成视频业务基础（中间）表
 * 
 * @author weizhang22
 * @date May 25, 2016
 *
 */
public class MRBaseTable4Video {

    private static final Log LOG = LogFactory.getLog(MRBaseTable4Video.class);

    public static class M1 extends Mapper<Object, Text, String, Text> {

        // 只筛选这四个终端产品ID
        Set<String> terminalIdSet = Sets.newHashSet("P0002221", "P0002522", "P0003124", "P0003126");

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] cols = value.toString().split("\t");

            String statisDay = null;
            String terminalId = null;
            String versionId = null;
            String userType = null;
            String chargeType = null;
            String optType = null;
            String brocastType = null;
            String contentId = null;
            String duration = "-1";
            String playType = "-1";
            String conClass1Name = null;
            //默认手机号是1111111111
            String servNumber="1111111111";

            StringBuilder valueSB = new StringBuilder();

            /* 获取输入数据的路径，以此来判断输入数据的类型 */
            String inputFile = getFilePath(context);

            try {
                if (inputFile.contains("td_aaa_tourist_bill_d/")) {
                    contentId = cols[TD_AAA_TOURIST_BILL_D.CONTENT_ID];

                    statisDay = cols[TD_AAA_TOURIST_BILL_D.STATIS_DAY];
                    terminalId = cols[TD_AAA_TOURIST_BILL_D.TERM_PROD_ID];
                    
                    servNumber = cols[TD_AAA_TOURIST_BILL_D.SERV_NUMBER];

                    if (!terminalIdSet.contains(terminalId)) {
                        return;
                    }

                    versionId = cols[TD_AAA_TOURIST_BILL_D.VERSION_ID];
                    userType = "2";
                    chargeType = cols[TD_AAA_TOURIST_BILL_D.CHARGE_TYPE];
                    optType = cols[TD_AAA_TOURIST_BILL_D.OPT_TYPE];
                    // TODO BROADCAST_TYPE_NEW?
                    brocastType = cols[TD_AAA_TOURIST_BILL_D.BROADCAST_TYPE];
                    // 左表-1-
                    valueSB.append("1-").append("\t" + statisDay).append("\t"+servNumber).append("\t" + terminalId).append("\t" + versionId)
                            .append("\t" + userType).append("\t" + chargeType).append("\t" + optType)
                            .append("\t" + brocastType);

                }
                if (inputFile.contains("td_aaa_bill_d/")) {

                    contentId = cols[TD_AAA_BILL_D.CONTENT_ID];

                    statisDay = cols[TD_AAA_BILL_D.STATIS_DAY];
                    terminalId = cols[TD_AAA_BILL_D.TERM_PROD_ID];
                    servNumber = cols[TD_AAA_BILL_D.SERV_NUMBER];

                    // 如果终端产品ID不在给定集合中，则直接返回
                    if (!terminalIdSet.contains(terminalId)) {
                        return;
                    }

                    versionId = cols[TD_AAA_BILL_D.VERSION_ID];
                    userType = "1";
                    chargeType = cols[TD_AAA_BILL_D.CHARGE_TYPE];
                    optType = cols[TD_AAA_BILL_D.OPT_TYPE];
                    // TODO BROADCAST_TYPE_NEW?
                    brocastType = cols[TD_AAA_BILL_D.BROADCAST_TYPE];
                    // 左表-1-
                    valueSB.append("1-").append("\t" + statisDay).append("\t"+servNumber).append("\t" + terminalId).append("\t" + versionId)
                            .append("\t" + userType).append("\t" + chargeType).append("\t" + optType)
                            .append("\t" + brocastType);

                }
                if (inputFile.contains("td_cms_content_d/")) {
                    contentId = cols[TD_CMS_CONTENT_D.CONTENT_ID];

                    if (StringUtils.isNotBlank(cols[TD_CMS_CONTENT_D.DURATION])) {
                        duration = cols[TD_CMS_CONTENT_D.DURATION];
                    }
                    if (StringUtils.isNotBlank(cols[TD_CMS_CONTENT_D.PLAY_TYPE])) {
                        playType = cols[TD_CMS_CONTENT_D.PLAY_TYPE];
                    }
                    // 中表-2-
                    valueSB.append("2-").append("\t" + duration).append("\t" + playType);
                }
                if (inputFile.contains("td_cms_content_class_d/")) {

                    contentId = cols[TD_CMS_CONTENT_CLASS_D.CONTENT_ID];

                    conClass1Name = cols[TD_CMS_CONTENT_CLASS_D.CON_CLASS_1_NAME];

                    // 右表-3-
                    valueSB.append("3-").append("\t" + conClass1Name);

                }
            } catch (Exception e) {
                LOG.error("***current record ERROR: " + inputFile + " : " + value.toString());
                System.err.println("Exception in getting columns:" + e);
            }

            if (StringUtils.isNotBlank(contentId) && StringUtils.isNotBlank(valueSB.toString())) {
                context.write(contentId, new Text(valueSB.toString()));
            }

        }

        // 获取文件路径
        public String getFilePath(@SuppressWarnings("rawtypes") Mapper.Context context) throws IOException {
            InputSplit split = context.getInputSplit();
            Class<? extends InputSplit> splitClass = split.getClass();
            FileSplit fileSplit = null;
            if (splitClass.equals(FileSplit.class)) {
                fileSplit = (FileSplit) split;
            } else if (splitClass.getName().equals("org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
                try {
                    Method getInputSplitMethod = splitClass.getDeclaredMethod("getInputSplit");
                    getInputSplitMethod.setAccessible(true);
                    fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new IOException("getFilePath fail.");
                }
            }

            return fileSplit.getPath().toString();
        }

    }

    public static class R1 extends Reducer<String, Text, String, Text> {

        @Override
        protected void reduce(String key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // 创建一个空的Text
            Text emptyText = new Text("");
            // 三个表进行连接，生成一张大表
            List<String> leftList = Lists.newArrayList();

            //默认是duration=-1，playType=-1
            String center = "\t-1\t-1";
            
            //td_cms_content_class_d中contentId不全,contentId对应的名称如果不存在
            //默认NA（Not Avaliable）
            String right = "\tNA";

            for (Text text : values) {
                String value = text.toString();
                if (StringUtils.isBlank(value) || value.charAt(1) != '-') {
                    continue;
                }

                // 左表-1，中表-2，右表-3
                char tableType = value.charAt(0);

                switch (tableType) {
                case '1':
                    leftList.add(value.substring(2));
                    break;
                case '2':
                    center = value.substring(2);
                    break;
                case '3':
                    right = value.substring(2);
                    break;
                }

            }

            // 数据不全
            if (leftList.size() == 0) {
                LOG.error("leftList.size==0");
                return;
            }

            // 进行表连接，中表和右表只有一个值
            for (String left : leftList) {
                context.write(key + left + center + right, emptyText);
            }
        }

    }

}
