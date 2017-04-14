package com.iflytek.gnome.log.analysis.index;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.iflytek.daplat.share.SafeDate;
import com.iflytek.gnome.analysis.model.AdAnalysisModel;
import com.iflytek.gnome.analysis.model.AdInfo;
import com.iflytek.gnome.analysis.model.PlatInfo;
import com.iflytek.gnome.common.constants.BaseConstants;

class IndexFields implements IIndex {

	public JsonObject getAdxIndexFields(AdAnalysisModel model) {

		if (null == model) {
			return null;
		}

		JsonObject json = new JsonObject();

		json.addProperty("sid", StringUtils.isBlank(model.sid) ? "" : model.sid);
		json.addProperty("app_name", StringUtils.isBlank(model.mediaName) ? ""
				: model.mediaName);
		json.addProperty("app_id", StringUtils.isBlank(model.mediaShowId) ? ""
				: model.mediaShowId);
		json.addProperty("package_name",
				StringUtils.isBlank(model.pkgname) ? "" : model.pkgname);
		json.addProperty("adid", StringUtils.isBlank(model.dvcAndroidId) ? ""
				: model.dvcAndroidId);
		json.addProperty("imei", StringUtils.isBlank(model.dvcAndroidImei) ? ""
				: model.dvcAndroidImei);
		json.addProperty("mac", StringUtils.isBlank(model.dvcMac) ? ""
				: model.dvcMac);
		json.addProperty("adunitId", model.adunitId);
		json.addProperty("net", model.ntt);
		json.addProperty("adunitShowId", StringUtils
				.isBlank(model.adunitShowId) ? "" : model.adunitShowId);

		// 所有第三方平台
		if (null != model.otherPlatRe) {
			json.addProperty("third_party_id", model.otherPlatRe.keySet()
					.toString());
		}

		// 下发物料
		if (null != model.otherPlatRe && model.otherPlatRe.size() > 0) {
			List<String> url = Lists.newArrayList();
			List<String> otherPlatSidList = Lists.newArrayList();
			for (String k : model.otherPlatRe.keySet()) {
				PlatInfo platInfo = model.otherPlatRe
						.get(k);
				String otherPlatSid = platInfo.otherPlatSid;
				if (otherPlatSid != null) {
					otherPlatSidList.add(otherPlatSid);
				}
				Map<String, AdInfo> adInfo = platInfo.adsMap;
				if (adInfo !=null) {
					for (String innerKey : adInfo.keySet()) {
						AdInfo ai = adInfo.get(innerKey);
						String imgUrl = ai.imgUrl;
						String txtText = ai.txtText;
						if (imgUrl != null) {
							url.add(imgUrl);
						}
						if (txtText != null) {
							url.add(txtText);
						}
					}
				}

				json.addProperty("ad_url", url.toString());
				json.addProperty("otherPlatSid", otherPlatSidList.toString());
			}
		}

		// 获胜第三方平台
		json.addProperty("winner_id", model.winPlatId);
		json.addProperty("idfa", StringUtils.isBlank(model.dvcIOSIdfa) ? ""
				: model.dvcIOSIdfa);
		json.addProperty("openudid",
				StringUtils.isBlank(model.dvcIOSOpenUdid) ? ""
						: model.dvcIOSOpenUdid);
		json.addProperty("ad_show_width", model.adunitWidth);
		json.addProperty("ad_show_height", model.adunitHeight);

		json.addProperty("screen_width", model.sWidth);
		json.addProperty("screen_height", model.sHeight);
		json.addProperty("media_id", model.mediaId);

		json.addProperty("date", SafeDate.Date2Format(
				new Date(model.startTime), BaseConstants.STANDARD_DATE_FORMAT));
		json.addProperty("impressed", model.uImpressNum);
		json.addProperty("clicked", model.uClickNum);
		json.addProperty("show_type", model.adunitShowType);

		String localIp = model.localIp;
		if (org.apache.commons.lang3.StringUtils.isNotBlank(localIp)) {
			json.addProperty("local_ip", localIp);
		}
		return json;
	}
}
