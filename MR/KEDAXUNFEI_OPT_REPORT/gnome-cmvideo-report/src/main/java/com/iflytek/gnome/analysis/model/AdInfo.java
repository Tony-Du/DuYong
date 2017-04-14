package com.iflytek.gnome.analysis.model;

import org.apache.avro.reflect.Nullable;

public class AdInfo {
	
	public int height;
	public int width;
	
	@Nullable
	public String otherPlatAdType;
	/**
	 * 图片地址
	 */
	@Nullable
	public String imgUrl;
	
	@Nullable
	public String txtTitle;
	
	@Nullable
	public String txtText;
	
	@Nullable
	public String iconUrl;
	public int iconWidth;
	public int iconHeight;
	
	/**
	 * 落地页地址
	 */
	@Nullable
	public String ldpUrl;
	
	@Nullable
	public String impressUrl;
	
	@Nullable
	public String clickUrl;
	
	@Nullable
	public String downStartUrl;
	
	@Nullable
	public String downSuccUrl;
	
	@Nullable
	public String installSuccUrl;
	
	//物料标注
	@Nullable
	public String contentTopic;
}
