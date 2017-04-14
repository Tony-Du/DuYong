/**
 * Copyright (c) 1999-2016 科大讯飞 All Rights Reserved.
 * 
 * FileName: MRVideoPlayReport.java
 * 
 * Description: 视频报表MR
 * 
 * History: v1.0.0, weizhang22, May 27, 2016, Create
 */
package com.iflytek.gnome.analysis.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.google.common.collect.Sets;
import com.iflytek.daplat.share.AvroMap;
import com.iflytek.daplat.share.ReportKey;
import com.iflytek.gnome.analysis.entity.BaseTable4Video;
import com.iflytek.gnome.analysis.entity.VideoPlayReprotHeader;

/**
 * 视频报表MR
 * 
 * @author weizhang22
 * @date May 27, 2016
 *
 */
public class MRVideoPlayReport {

    private static final Log LOG = LogFactory.getLog(MRVideoPlayReport.class);

    public static class M1 extends Mapper<Object, Text, ReportKey, AvroMap> {

        Set<String> productID_set = Sets.newHashSet();

        // 印象天下contentId set
        Set<String> yxtx_contentID_set = Sets.newHashSet("2200200699", "2202634723", "2200200717", "2200200738",
                "2200200771", "2200200791", "2200200817", "2200192831", "2200192881", "2200192899", "2200192937",
                "2202454402", "2202454351", "2202454452", "2202454458", "2200335251", "2200316346", "2201019954",
                "2201106597", "2200338505", "2201106606", "2202288106", "2202483015", "2202483098", "2202175961",
                "2202175097", "2201190454", "2202176109", "2203040135 ", "2203040154", "2203040275", "2203063466",
                "2203070014", "2203070189", "2203070257", "2203070309", "2203070453", "2203070366", "2203070523",
                "2203070645", "2203070710", "2203224198", "2203224461", "2203229513", "2203229958", "2203230040",
                "2203230131", "2203230211", "2203230504", "2203231333", "2203231389", "2203234822", "2203237033",
                "2203237332", "2203237490", "2203237857", "2203887200", "2203891577", "2203892083", "2203892441",
                "2203892529", "2203895509", "2203895560", "2203970716", "2203974013", "2205006313", "2205010515",
                "2205010756", "2205010773", "2201121747", "2202224644", "2201140528", "2201138854", "2201138842",
                "2202222983", "2202055892", "2202078213", "2202054373", "2202186855", "2202529327", "2201034071",
                "2200286344", "2200316316", "2200311816", "2201039004", "2201122114", "2200347525", "2201122071",
                "2202645669", "2202683437", "2203017652", "2203025208", "2203025450", "2203028521", "2203049539",
                "2203048114", "2203048191", "2203048358", "2203048644", "2203048707", "2203048824", "2205456312");

        @Override
        protected void setup(Mapper<Object, Text, ReportKey, AvroMap>.Context context)
                throws IOException, InterruptedException {

            try {
                // Path[] cacheFiles = context.getLocalCacheFiles();
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                if (null != cacheFiles && cacheFiles.length > 0) {
                    String line;
                    BufferedReader dataReader = new BufferedReader(new FileReader("PPD"));
                    try {
                        while (null != (line = dataReader.readLine())) {
                            String[] cols = line.split("\t");
                            String product_id = cols[0].trim();
                            int bill_type = Integer.parseInt(cols[3].trim());
                            int product_price = Integer.parseInt(cols[4].trim());
                            if (0 == bill_type && product_price > 0) {
                                productID_set.add(product_id);
                            }
                        }
                    } finally {
                        dataReader.close();
                    }
                }
            } catch (Exception e) {
                System.err.println("Exception in reading DistributedCache:" + e);
            }

        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] cols = value.toString().split("\t");

            // opt_type 0-使用（播放或者下载）；4-充值了，但未使用
            if (StringUtils.isBlank(cols[BaseTable4Video.OPT_TYPE])
                    || Integer.parseInt(cols[BaseTable4Video.OPT_TYPE]) != 0) {
                return;
            }

            /* 获取输入数据的路径，以此来判断输入数据的类型 */
            String inputFile = getFilePath(context);

            try {
                if (inputFile.contains("playTable/")) {

                    // 收费类型：0-汇总，1-收费，2-免费
                    String chargeType = null;
                    if (productID_set.contains(cols[BaseTable4Video.PRODUCT_ID])) {
                        chargeType = "1";
                    } else {
                        chargeType = "2";
                    }

                    // 统计类型： 0-汇总，1-用户，2-游客
                    String userType = cols[BaseTable4Video.USER_TYPE];

                    ReportKey reportKey = new ReportKey();
                    // 设置报表的维度
                    // 日期
                    reportKey.put(VideoPlayReprotHeader.STATIS_DAY, cols[BaseTable4Video.STATIS_DAY]);
                    // 终端产品ID
                    reportKey.put(VideoPlayReprotHeader.TERMINAL_ID, cols[BaseTable4Video.TERMINAL_ID]);
                    // 版本号
                    reportKey.put(VideoPlayReprotHeader.VERSION_ID, cols[BaseTable4Video.VERSION_ID]);
                    // 是否收费
                    reportKey.put(VideoPlayReprotHeader.CHARGE_TYPE, chargeType);
                    // 统计类型
                    reportKey.put(VideoPlayReprotHeader.USER_TYPE, userType);

                    // 初始化报表的指标值（默认为0）
                    AvroMap reportValue = getZeroAvroMap();

                    // 播放类型（broadcast_type=1）： 1，4，7-点播； 2，5，8，9，12-直播
                    // 默认为-1；
                    int playType = -1;
                    if (StringUtils.isNotBlank(cols[BaseTable4Video.PLAY_TYPE])) {
                        playType = Integer.parseInt(cols[BaseTable4Video.PLAY_TYPE]);
                    }
                    // 下载（broadcast_type=2，playType=3，4）
                    // 默认为-1
                    int broadcastType = -1;
                    if (StringUtils.isNotBlank(cols[BaseTable4Video.BROCAST_TYPE])) {
                        broadcastType = Integer.parseInt(cols[BaseTable4Video.BROCAST_TYPE]);
                    }

                    if (broadcastType == 2) {
                        if (playType == 3 || playType == 4) {
                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_DOWNLOAD, 1L);
                        }
                    }

                    if (broadcastType == 1) {

                        // 视频播放总数
                        reportValue.getData().put(VideoPlayReprotHeader.TIMES_TOTAL, 1L);

                        // 视频时长,如果为空，默认为0
                        double videoDuration = -1;
                        if (StringUtils.isNotBlank(cols[BaseTable4Video.DURATION])) {
                            videoDuration = Double.parseDouble(cols[BaseTable4Video.DURATION]);
                            videoDuration = videoDuration*1.0 / 60;
                        }
                        if (0 < videoDuration && videoDuration <= 1) {
                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_0_1_MIN, 1L);
                        } else if (1 < videoDuration && videoDuration <= 3) {
                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_1_3_MIN, 1L);
                        } else if (3 < videoDuration && videoDuration <= 5) {
                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_3_5_MIN, 1L);
                        } else if (5 < videoDuration && videoDuration <= 10) {
                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_5_10_MIN, 1L);
                        } else if (10 < videoDuration) {

                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_10_N_MIN, 1L);
                        }

                        // 点播
                        if (playType == 1 || playType == 4 || playType == 7) {
                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_REQUEST_PLAY, 1L);
                            // 直播
                        } else if (playType == 2 || playType == 5 || playType == 8 || playType == 9 || playType == 12) {
                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_LIVE_PLAY, 1L);
                        }

                        // 印象天下
                        String contentId = cols[BaseTable4Video.CONTENT_ID];
                        if (yxtx_contentID_set.contains(contentId)) {
                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_YXTX, 1L);
                        }
                        // 各个频道

                        // 内容一级类别名称
                        String firstClassName = cols[BaseTable4Video.CON_CLASS_1_NAME];
                        if (StringUtils.isNotBlank(firstClassName) && firstClassName.length() >= 2) {
                            firstClassName = firstClassName.substring(0, 2);
                        }

                        switch (firstClassName) {
                        case "财经":
                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_FINANCE, 1L);
                            break;
                        case "电视":
                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_TV, 1L);

                            break;
                        case "电影":
                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_MOVIE, 1L);

                            break;
                        case "动漫":
                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_CARTOON, 1L);

                            break;
                        case "纪实":
                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_RECORD, 1L);

                            break;
                        case "健康":
                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_HEALTH, 1L);

                            break;
                        case "教育":
                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_EDUCATION, 1L);

                            break;
                        case "军事":
                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_MILITARY, 1L);

                            break;
                        case "旅游":
                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_TRAVEL, 1L);

                            break;
                        case "体育":
                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_SPORT, 1L);

                            break;
                        case "新闻":
                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_NEWS, 1L);

                            break;
                        case "娱乐":
                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_ENTERTAINMENT, 1L);

                            break;
                        case "原创":
                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_ORIGINAL, 1L);

                            break;
                        case "直播":
                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_LIVE, 1L);

                            break;
                        case "综艺":
                            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_VARIETY, 1L);
                        }

                    }

                    // 输出收费类型-统计类型信息
                    context.write(reportKey, reportValue);
                    // 输出汇总-汇总信息
                    this.contextWrite("0", "0", cols, reportValue, context);
                    // 输出汇总-统计类型信息
                    this.contextWrite("0", userType, cols, reportValue, context);
                    // 输出收费类型-汇总信息
                    this.contextWrite(chargeType, "0", cols, reportValue, context);

                }
            } catch (Exception e) {
                LOG.info("***current record ERROR: " + inputFile + " : " + value.toString());
                System.err.println("Exception in getting columns:" + e.getMessage());
            }

        }

        /**
         * 
         * 输出结果
         * 
         */
        private void contextWrite(String chargeType, String userType, String[] cols, AvroMap reportValue,
                Context context) throws IOException, InterruptedException {
            ReportKey reportKey = new ReportKey();
            // 设置报表的维度
            // 日期
            reportKey.put(VideoPlayReprotHeader.STATIS_DAY, cols[BaseTable4Video.STATIS_DAY]);
            // 终端产品ID
            reportKey.put(VideoPlayReprotHeader.TERMINAL_ID, cols[BaseTable4Video.TERMINAL_ID]);
            // 版本号
            reportKey.put(VideoPlayReprotHeader.VERSION_ID, cols[BaseTable4Video.VERSION_ID]);
            // 是否收费
            reportKey.put(VideoPlayReprotHeader.CHARGE_TYPE, chargeType);
            // 统计类型
            reportKey.put(VideoPlayReprotHeader.USER_TYPE, userType);
            // 输出汇总-汇总信息
            context.write(reportKey, reportValue);
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

        public AvroMap getZeroAvroMap() {
            AvroMap reportValue = new AvroMap(new HashMap<CharSequence, Object>());
            reportValue.getData().put(VideoPlayReprotHeader.TIMES_TOTAL, 0L);
            reportValue.getData().put(VideoPlayReprotHeader.TIMES_0_1_MIN, 0L);
            reportValue.getData().put(VideoPlayReprotHeader.TIMES_1_3_MIN, 0L);
            reportValue.getData().put(VideoPlayReprotHeader.TIMES_3_5_MIN, 0L);
            reportValue.getData().put(VideoPlayReprotHeader.TIMES_5_10_MIN, 0L);
            reportValue.getData().put(VideoPlayReprotHeader.TIMES_10_N_MIN, 0L);
            reportValue.getData().put(VideoPlayReprotHeader.TIMES_LIVE_PLAY, 0L);
            reportValue.getData().put(VideoPlayReprotHeader.TIMES_REQUEST_PLAY, 0L);
            reportValue.getData().put(VideoPlayReprotHeader.TIMES_DOWNLOAD, 0L);
            reportValue.getData().put(VideoPlayReprotHeader.TIMES_YXTX, 0L);

            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_CARTOON, 0L);
            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_EDUCATION, 0L);
            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_ENTERTAINMENT, 0L);
            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_FINANCE, 0L);
            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_HEALTH, 0L);
            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_LIVE, 0L);

            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_MILITARY, 0L);
            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_MOVIE, 0L);
            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_NEWS, 0L);
            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_ORIGINAL, 0L);
            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_RECORD, 0L);
            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_SPORT, 0L);
            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_TRAVEL, 0L);
            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_TV, 0L);
            reportValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_VARIETY, 0L);
            return reportValue;
        }

    }

    public static class R1 extends Reducer<ReportKey, AvroMap, ReportKey, AvroMap> {

        @Override
        protected void reduce(ReportKey key, Iterable<AvroMap> values, Context context)
                throws IOException, InterruptedException {

            Long times_total = 0L;
            Long times_0_1_min = 0L;
            Long times_1_3_min = 0L;
            Long times_3_5_min = 0L;
            Long times_5_10_min = 0L;
            Long times_10_n_min = 0L;
            Long times_live_play = 0L;
            Long times_request_play = 0L;
            Long times_download = 0L;
            Long times_yxtx = 0L;
            Long times_channel_cartoon = 0L;
            Long times_channel_education = 0L;
            Long times_channel_entertainment = 0L;
            Long times_channel_finance = 0L;
            Long times_channel_health = 0L;
            Long times_channel_live = 0L;
            Long times_channel_military = 0L;
            Long times_channel_movie = 0L;
            Long times_channel_news = 0L;
            Long times_channel_original = 0L;
            Long times_channel_record = 0L;
            Long times_channel_sport = 0L;
            Long times_channel_travel = 0L;
            Long times_channel_tv = 0L;
            Long times_channel_variety = 0L;

            for (AvroMap reportValue : values) {
                times_total += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_TOTAL);
                times_0_1_min += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_0_1_MIN);
                times_1_3_min += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_1_3_MIN);
                times_3_5_min += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_3_5_MIN);
                times_5_10_min += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_5_10_MIN);
                times_10_n_min += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_10_N_MIN);
                times_live_play += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_LIVE_PLAY);
                times_request_play += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_REQUEST_PLAY);
                times_download += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_DOWNLOAD);
                times_yxtx += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_YXTX);

                times_channel_cartoon += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_CHANNEL_CARTOON);
                times_channel_education += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_CHANNEL_EDUCATION);
                times_channel_entertainment += (Long) reportValue.getData()
                        .get(VideoPlayReprotHeader.TIMES_CHANNEL_ENTERTAINMENT);
                times_channel_finance += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_CHANNEL_FINANCE);
                times_channel_health += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_CHANNEL_HEALTH);
                times_channel_live += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_CHANNEL_LIVE);

                times_channel_military += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_CHANNEL_MILITARY);
                times_channel_movie += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_CHANNEL_MOVIE);
                times_channel_news += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_CHANNEL_NEWS);
                times_channel_original += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_CHANNEL_ORIGINAL);
                times_channel_record += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_CHANNEL_RECORD);
                times_channel_sport += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_CHANNEL_SPORT);
                times_channel_travel += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_CHANNEL_TRAVEL);
                times_channel_tv += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_CHANNEL_TV);
                times_channel_variety += (Long) reportValue.getData().get(VideoPlayReprotHeader.TIMES_CHANNEL_VARIETY);

            }

            AvroMap reportOutValue = new AvroMap(new HashMap<CharSequence, Object>());
            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_TOTAL, times_total);
            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_0_1_MIN, times_0_1_min);
            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_1_3_MIN, times_1_3_min);
            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_3_5_MIN, times_3_5_min);
            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_5_10_MIN, times_5_10_min);
            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_10_N_MIN, times_10_n_min);
            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_LIVE_PLAY, times_live_play);
            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_REQUEST_PLAY, times_request_play);
            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_DOWNLOAD, times_download);
            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_YXTX, times_yxtx);

            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_CARTOON, times_channel_cartoon);
            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_EDUCATION, times_channel_education);
            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_ENTERTAINMENT, times_channel_entertainment);
            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_FINANCE, times_channel_finance);
            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_HEALTH, times_channel_health);
            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_LIVE, times_channel_live);

            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_MILITARY, times_channel_military);
            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_MOVIE, times_channel_movie);
            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_NEWS, times_channel_news);
            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_ORIGINAL, times_channel_original);
            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_RECORD, times_channel_record);
            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_SPORT, times_channel_sport);
            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_TRAVEL, times_channel_travel);
            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_TV, times_channel_tv);
            reportOutValue.getData().put(VideoPlayReprotHeader.TIMES_CHANNEL_VARIETY, times_channel_variety);

            context.write(key, reportOutValue);

        }

    }

}
