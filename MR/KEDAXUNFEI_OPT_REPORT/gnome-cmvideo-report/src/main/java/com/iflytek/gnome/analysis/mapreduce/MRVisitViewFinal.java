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
import com.iflytek.gnome.analysis.entity.TM_VISIT;
import com.iflytek.gnome.analysis.entity.ViewReportHeader;
import com.iflytek.gnome.analysis.entity.ViewReportHeader.VisitViewHeader;

/**
 * @author wtxu2
 * @date 2016年8月15日
 */
public class MRVisitViewFinal {

    public static final Log LOG = LogFactory.getLog(MRVisitViewFinal.class);

    private static final String part = "\t";

    public static class M1 extends Mapper<LongWritable, Text, String, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String valueFill = value.toString() + "\t" + "tail";
            String[] cols = valueFill.toString().split(part);
            String serv_number = cols[TM_VISIT.SERV_NUMBER];
            String imei_new = cols[TM_VISIT.IMEI_NEW];

            if (StringUtils.isNotBlank(serv_number) || StringUtils.isNotBlank(imei_new)) {

                // 先获取中间表数据
                String statis_day = context.getConfiguration().get("startDate"); // 日期
                String dept_id = cols[TM_VISIT.DEPT_ID];// 部门
                String usr_type = cols[TM_VISIT.USER_TYPE];	// usr_type 1 2
                                                           	// 1代表号码用户，2 代表游客
                String TERM_PROD_TYPE_ID = cols[TM_VISIT.TERM_PROD_TYPE_ID];// 终端产品类型
                String TERM_PROD_CLASS_ID = cols[TM_VISIT.TERM_PROD_CLASS_ID];// 终端产品平台
                String term_prod_id = cols[TM_VISIT.TERM_PROD_ID];  // 终端产品
                String chn_id_type = cols[TM_VISIT.CHN_ID_TYPE]; // 渠道名称
                String chn_id_new = cols[TM_VISIT.CHN_ID_NEW];  // 渠道类型

                // String value1 = serv_number + part + imei_new + part +
                // usr_type ;

                // 维度 部门ID 终端产品类型 终端产品 终端平台 渠道类型 渠道名称

                String Mapkey = statis_day + part + dept_id + part + TERM_PROD_TYPE_ID + part + term_prod_id + part
                        + TERM_PROD_CLASS_ID + part + chn_id_new + part + chn_id_type;

                // 兩步MR 去做UV 利用Hadoop key 去重 后期考虑直接写组合key，在reduce端处理,避免两步MR
                context.write(serv_number + ":" + Mapkey, new Text(usr_type));
                context.write(imei_new + ":" + Mapkey, new Text("imei"));

            }
        }
    }

    public static class R1 extends Reducer<String, Text, String, Text> {

        protected void reduce(String key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            long userCount = 0L;
            long tourCount = 0L;

            boolean isImei = false;
            boolean isUser = false;
            for (Text value : values) {
                // imei
                if (value.toString().contains("imei")) {
                    isImei = true;
                }
                // serv_number 用户和 游客 不会 shuffle到一个reduce
                else if (value.toString().equals("1")) {
                    ++userCount;
                    isUser = true;
                } else {
                    ++tourCount;
                }

            }

            // 拆分key

            String[] s = key.split(":");
            if (StringUtils.isNotBlank(s[1])) {
                // key 带过来的是imei
                if (isImei) {
                    context.write(s[1] + ":" + s[0] + "\t" + "imei", new Text(""));
                }
                // 是 serv_number
                else if (isUser) {
                    context.write(s[1] + ":" + s[0] + "\t" + "user" + "\t" + userCount, new Text(""));
                } else {
                    context.write(s[1] + ":" + s[0] + "\t" + "tour" + "\t" + tourCount, new Text(""));
                }
                // 去重后 下一个MR可以正常 计数了
            }

        }
    }

    public static class M2 extends Mapper<LongWritable, Text, String, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] values = line.split(":");

            // 输出 原有的维度 和 对于单个指标的值
            context.write(values[0], new Text(values[1]));

        }
    }

    public static class R2 extends Reducer<String, Text, ReportKey, AvroMap> {

        @Override
        protected void reduce(String key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // 表示维度
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

            // 初始化 指标值 接着填充指标值
            AvroMap reportValue = getZeroAvroMap();

            Long UserPvSum = 0L;
            Long TouPvSum = 0L;
            Long imeiSum = 0L;

            Long UserUv = 0L;
            Long TourUv = 0L;

            for (Text value : values) {

                if (value.toString().contains("imei")) {
                    ++imeiSum;
                } else if (value.toString().contains("user")) {
                    UserPvSum += Long.parseLong(value.toString().split("\t")[2]);
                    ++UserUv;
                } else {
                    TouPvSum += Long.parseLong(value.toString().split("\t")[2]);
                    ++TourUv;
                }

            }

            reportValue.getData().put(ViewReportHeader.VisitViewHeader.PV_USER, UserPvSum);
            reportValue.getData().put(ViewReportHeader.VisitViewHeader.PV_TOURIST, TouPvSum);
            reportValue.getData().put(ViewReportHeader.VisitViewHeader.UV_USER, UserUv);
            reportValue.getData().put(ViewReportHeader.VisitViewHeader.UV_TOURIST, TourUv);
            reportValue.getData().put(ViewReportHeader.VisitViewHeader.UV_IMEI, imeiSum);

            // 输出
            context.write(reportKey, reportValue);

        }

        // 初始化 指标
        public AvroMap getZeroAvroMap() {
            AvroMap reportValue = new AvroMap(new HashMap<CharSequence, Object>());
            reportValue.getData().put(ViewReportHeader.VisitViewHeader.PV_USER, 0L);
            reportValue.getData().put(ViewReportHeader.VisitViewHeader.PV_TOURIST, 0L);
            reportValue.getData().put(ViewReportHeader.VisitViewHeader.UV_USER, 0L);
            reportValue.getData().put(ViewReportHeader.VisitViewHeader.UV_TOURIST, 0L);
            reportValue.getData().put(ViewReportHeader.VisitViewHeader.UV_IMEI, 0L);
            return reportValue;
        }

    }
}
