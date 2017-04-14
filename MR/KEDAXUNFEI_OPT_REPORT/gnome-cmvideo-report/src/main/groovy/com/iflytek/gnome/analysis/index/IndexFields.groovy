package com.iflytek.gnome.analysis.index

import com.google.common.collect.Lists
import com.google.gson.JsonObject
import com.iflytek.gnome.analysis.model.AdAnalysisModel
import com.iflytek.gnome.analysis.model.DspModel
import com.iflytek.gnome.dspextract.AdInfo
import com.iflytek.gnome.dspextract.PlatInfo
import com.iflytek.gnome.log.analysis.index.IIndex
import org.apache.avro.generic.GenericRecord
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.io.MapWritable

import java.text.SimpleDateFormat

class IndexFields implements IIndex {

    // 日志查询时间格式
    def FORMAT_OUTPUT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    // 广告请求的会话id，session_id
    def SID = "sid";

    // 媒体名称
    def APP_NAME = "app_name";
    // 媒体id
    def APP_ID = "app_id";
    // packagename
    def PACKAGE_NAME = "package_name";
    // android id
    def ADID = "adid";
    // android imei
    def IMEI = "imei";
    // android mac
    def MAC = "mac";

    def AAID = "aaid";
    // 手机操作系统
    def OS = "os";
    // 所有第三方平台请求
    def THIRD_PARTY_ID = "third_party_id";
    // 广告平台sdk版本号
    def SDK_VERSION = "sdk_version"

    def DATE = "date";

    def OPENUDID = "openudid";

    def LOCAL_IP = "local_ip";

    def IDFA = "idfa";

    def DUID = "duid";

    def OPERATOR = "operator";

    def IP = "ip";
    // 获胜第三方平台
    def WINNER_THIRD_PARTY_ID = "winner_id";

    def PRICE = "price";

    def DEVICE_TYPE = "device_type";

    def DEVICE_MODEL = "device_model";

    def IS_IMPRESSED = "is_impressed";

    def IS_CLICKED = "is_clicked";

    def AD_SHOW_TYPE = "ad_show_type";

    def INCOME_TYPE = "income_type";
    // 广告展示宽度
    def AD_SHOW_WIDTH = "ad_show_width";
    // 广告展示高度
    def AD_SHOW_HEIGHT = "ad_show_height";
    // 屏幕宽度
    def SCREEN_WIDTH = "screen_width";
    // 屏幕高度
    def SCREEN_HEIGHT = "screen_height";

    // requirements

    def RET = "ret";

    def ERRORMESSAGE = "error_message";

    def MEDIAID = "media_id";
    // local 平台的广告位
    def ADUNITSHOWID = "adunit_show_id";

    def AD_CONTENT_TYPE = "ad_content_type";
    // 物料形式
    def AD_URL = "ad_url";

    def IMPRESSED = "impressed";
    def CLICKED = "clicked";

    @Override
    JsonObject getAdxIndexFields(Object model) {
        if(null == model){
            return null;
        }
        JsonObject json = new JsonObject();

        String sid = StringUtils.isBlank(model.sid) ? "" : model.sid;
        json.addProperty(SID,sid);

        String appName = StringUtils.isBlank(model.mediaName) ? "" : model.mediaName;
        json.addProperty(APP_NAME,appName);

        String appId = StringUtils.isBlank(model.mediaShowId) ? "" : model.mediaShowId;
        json.addProperty(APP_ID,appId);

        String packageName = StringUtils.isBlank(model.pkgname) ? "" : model.pkgname;
        json.addProperty(PACKAGE_NAME,packageName);

        String adId = StringUtils.isBlank(model.dvcAndroidId) ? "" : model.dvcAndroidId;
        json.addProperty(ADID,adId);

        String imei = StringUtils.isBlank(model.dvcAndroidImei) ? "" : model.dvcAndroidImei;
        json.addProperty(IMEI,imei);

        String mac = StringUtils.isBlank(model.dvcMac) ? "" : model.dvcMac;
        json.addProperty(MAC,mac);

        String adunitId = model.adunitId.toString();
        json.addProperty("adunitId",adunitId);

        // 所有第三方平台
        if (null!=model.otherPlatRe) {
            String otherPlatformId = model.otherPlatRe.keySet().toListString();
            json.addProperty(THIRD_PARTY_ID,otherPlatformId);
        }

        // 下发物料
        if (null != model.otherPlatRe && model.otherPlatRe.size() > 0) {
            List<String> url = Lists.newArrayList();
			List<String> otherPlatSidList = Lists.newArrayList();
            for (String k : model.otherPlatRe.keySet()) {
                GenericRecord platInfo = model.otherPlatRe.get(k);
                String otherPlatSid = platInfo.get("otherPlatSid");
				if (otherPlatSid != null) {
					otherPlatSidList.add(otherPlatSid);
				Map<String, GenericRecord> adInfo = platInfo.get("adsMap");
                for (String innerKey : adInfo.keySet()) {
                    GenericRecord ai = adInfo.get(innerKey);
					String imgUrl = ai.get("imgUrl");
					String txtText = ai.get("txtText");
					if (imgUrl != null) {
						url.add(imgUrl);
					}
					if (txtText != null) {
						url.add(txtText);
					}
                }
            }
            json.addProperty(AD_URL,url.toListString());
			json.addProperty("otherPlatSid", otherPlatSidList.toListString());
        }

        // 获胜第三方平台
        String winerId = model.winPlatId.toString();
        json.addProperty(WINNER_THIRD_PARTY_ID,winerId);

        String idfa = StringUtils.isBlank(model.dvcIOSIdfa) ? "" : model.dvcIOSIdfa;
        json.addProperty(IDFA,idfa);

        String openUUID = StringUtils.isBlank(model.dvcIOSOpenUdid) ? "" : model.dvcIOSOpenUdid;
        json.addProperty(OPENUDID,openUUID);

        String adWidth = model.adunitWidth.toString();
        json.addProperty(AD_SHOW_WIDTH,adWidth);

        String adHeight = model.adunitHeight.toString();
        json.addProperty(AD_SHOW_HEIGHT,adHeight);

        String screenWidth = model.sWidth.toString();
        json.addProperty(SCREEN_WIDTH,screenWidth);

        String screenHeight = model.sHeight.toString();
        json.addProperty(SCREEN_HEIGHT,screenHeight);

        String mediaId = model.mediaId.toString();
        json.addProperty(MEDIAID,mediaId);

        String startTime = FORMAT_OUTPUT.format(new Date(model.startTime));
        json.addProperty(DATE,startTime);

        String impressed = model.uImpressNum.toString();
        json.addProperty(IMPRESSED,impressed);

        String clicked = model.uClickNum.toString();
        json.addProperty(CLICKED,clicked);

        return json;
    }

    @Override
    MapWritable getDspIndexFields(DspModel model) {
        return null
    }

}
