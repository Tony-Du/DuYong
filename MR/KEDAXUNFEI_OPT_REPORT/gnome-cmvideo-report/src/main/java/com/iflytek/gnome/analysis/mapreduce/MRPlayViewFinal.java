package com.iflytek.gnome.analysis.mapreduce;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.iflytek.daplat.share.AvroMap;
import com.iflytek.daplat.share.ReportKey;
import com.iflytek.gnome.analysis.entity.TM_PLAY;
import com.iflytek.gnome.analysis.entity.ViewReportHeader;
import com.iflytek.gnome.analysis.entity.ViewReportHeader.VisitViewHeader;

/**
 * 
 * 用户使用规模、流量、时长报表
 * 
 * @author wtxu2
 * @version 1.0 2016/8/15 两步MR hadoop key 先去重UV指标
 */
public class MRPlayViewFinal {

    public static final Log LOG = LogFactory.getLog(MRPlayView.class);

    public static final String part = "\t";

    public static class M1 extends Mapper<LongWritable, Text, String, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] cols = value.toString().split("\t");

           
            String serv_number = cols[TM_PLAY.SERV_NUMBER];
            String use_flow = cols[TM_PLAY.USE_FLOW];
            String use_dur = cols[TM_PLAY.USE_DUR];
            // String opt_type = cols[TM_PLAY.OPT_TYPE];

            // 过滤条件 待确认! 过滤量较大 opt_type.equals("4")
            if (StringUtils.isNotBlank(serv_number)) {

                // String statis_day = cols[TM_PLAY.STATIS_DAY];
                // 数据源中每天的数据包含多日的日期数据，这里先统一强制设置为startDate
                String statis_day = context.getConfiguration().get("startDate");
                String dept_id = cols[TM_PLAY.DEPT_ID];
                String usr_type = cols[TM_PLAY.USER_TYPE];
                String TERM_PROD_TYPE_ID = cols[TM_PLAY.TERM_PROD_TYPE_ID];
                String TERM_PROD_CLASS_ID = cols[TM_PLAY.TERM_PROD_CLASS_ID];
                String term_prod_id = cols[TM_PLAY.TERM_PROD_ID];
                String chn_id_type = cols[TM_PLAY.CHN_ID_TYPE];
                String chn_id_new = cols[TM_PLAY.CHN_ID_NEW];
                String use_cnt = cols[TM_PLAY.USE_CNT];

                // 维度 部门ID 终端产品类型 终端产品 终端平台 渠道类型 渠道名称

                String Mapkey = statis_day + part + dept_id + part + TERM_PROD_TYPE_ID + part + term_prod_id + part
                        + TERM_PROD_CLASS_ID + part + chn_id_new + part + chn_id_type;

                // value
                String value1 = use_dur + part + use_flow + part + usr_type + part + use_cnt;

                // 兩步MR 去做UV 利用Hadoop key 去重
                context.write(serv_number + ":" + Mapkey, new Text(value1));

            }
        }
    }

    // shuffle 去重 serv_number ，把非UV字段先分组计算处理，第二个Reduce可以累加即可
    public static class R1 extends Reducer<String, Text, String, Text> {

        @Override
        protected void reduce(String key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // // value
            // String value1 = use_dur + part + use_flow
            // + part + usr_type + part + use_cnt;
            //
            Long userCount = 0L;
            double durSum = 0D;
            double flowSum = 0D;
            String usertype = "";

            String s[];
            for (Text value : values) {
                s = value.toString().split(part);
                usertype = s[2];
                userCount += Long.parseLong(s[3]);// 相当与部分PV
                durSum += Double.parseDouble(s[0]);
                flowSum += Double.parseDouble(s[1]);
            }
            String[] skey = key.split(":");

            // skey[1]是Map1的 MapKey
            context.write(skey[1] + ":" + skey[0] + part + usertype + part + userCount + part + durSum + part + flowSum,
                    new Text(""));

        }

    }

    public static class M2 extends Mapper<LongWritable, Text, String, Text> {

        // 读入的value就是上个reduce的输出 ，也就是reduce的key
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] values = line.split(":");

            // 输出 原有的维度 和 对于单个指标的值 values[0]:原来的维度， values[1]： 原来的指标
            context.write(values[0], new Text(values[1]));
        }
    }

    public static class R2 extends Reducer<String, Text, ReportKey, AvroMap> {

        @Override
        protected void reduce(String key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            ReportKey reportKey = new ReportKey();

            // reportKey 填冲报表入库的维度
            String[] keys = key.split("\t");

            if (keys.length != 7) {
                return;
            }

            reportKey.put(VisitViewHeader.STATIS_DAY, keys[0]);
            reportKey.put(VisitViewHeader.DEPT_ID, keys[1]);
            reportKey.put(VisitViewHeader.TERM_PROD_TYPE_ID, keys[2]);
            reportKey.put(VisitViewHeader.TERM_PROD_ID, keys[3]);
            reportKey.put(VisitViewHeader.TERM_PROD_CLASS_ID, keys[4]);
            reportKey.put(VisitViewHeader.CHN_ID_NEW, keys[5]);
            reportKey.put(VisitViewHeader.CHN_ID_TYPE, keys[6]);

            // 初始化
            AvroMap ValueMap = new AvroMap(new HashMap<CharSequence, Object>());

            double UserDurSum = 0D;
            double UserFlow = 0D;

            double TouristDurSum = 0D;
            double TouristFlow = 0D;

            Long UserUv = 0L;
            Long TourUv = 0L;
            Long UserPvSum = 0L;
            Long TouPvSum = 0L;

            String usrType;

            // skey [0] + part + usertype + part + userCount + part + durSum +
            // part + flowSum

            // 此时进来的是已经按serv_number 去重过的，可以直接count 个数 就是UV值
            for (Text value : values) {
                String[] vs = value.toString().split(part);
                usrType = vs[1].toString();
                if (usrType.equals("1")) {
                    ++UserUv;
                    UserDurSum += Double.parseDouble(vs[3].toString());
                    UserFlow += Double.parseDouble(vs[4].toString());
                    UserPvSum += Long.parseLong(vs[2].toString());
                }
                if (usrType.equals("2")) {
                    ++TourUv;
                    TouristDurSum += Double.parseDouble(vs[3].toString());
                    TouristFlow += Double.parseDouble(vs[4].toString());
                    TouPvSum += Long.parseLong(vs[2].toString());
                }
            }

            // 流量格式以KB为单位
            Double UserFlowSum = (double) (UserFlow / 1024);
            Double TourFlowSum = (double) (TouristFlow / 1024);

            // DecimalFormat df = new DecimalFormat("#.00");

            ValueMap.getData().put(ViewReportHeader.PlayViewHeader.VV_USER, UserPvSum);
            ValueMap.getData().put(ViewReportHeader.PlayViewHeader.PLAY_DUR_USER, UserDurSum);
            ValueMap.getData().put(ViewReportHeader.PlayViewHeader.PLAY_FLOW_USER, String.format("%.2f", UserFlowSum));
            ValueMap.getData().put(ViewReportHeader.PlayViewHeader.USER_UV, UserUv);

            ValueMap.getData().put(ViewReportHeader.PlayViewHeader.VV_TOURIST, TouPvSum);
            ValueMap.getData().put(ViewReportHeader.PlayViewHeader.PLAY_DUR_TOURIST, TouristDurSum);
            ValueMap.getData().put(ViewReportHeader.PlayViewHeader.PLAY_FLOW_TOURIST,
                    String.format("%.2f", TourFlowSum));
            ValueMap.getData().put(ViewReportHeader.PlayViewHeader.TOURIST_UV, TourUv);

            context.write(reportKey, ValueMap);

        }

    }

}
