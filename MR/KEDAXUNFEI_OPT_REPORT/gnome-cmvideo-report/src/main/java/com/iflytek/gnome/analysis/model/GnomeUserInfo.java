package com.iflytek.gnome.analysis.model;

public class GnomeUserInfo
{
	public int mediaId;

	public long fRequestTime = Long.MAX_VALUE;
	public long lRequestTime = 0L;
	public long requestNum = 0L;

	public long fImpressTime = Long.MAX_VALUE;
	public long lImpressTime = 0L;
	public long impressNum = 0L;

	public long fClickTime = Long.MAX_VALUE;
	public long lClickTime = 0L;
	public long clickNum = 0L;

	/**
	 * androidId-imei-udid-idfa-mac
	 */
	public String uid;

	public void merge(GnomeUserInfo ui)
	{
		this.fRequestTime = Math.min(this.fRequestTime, ui.fRequestTime);
		this.lRequestTime = Math.max(this.lRequestTime, ui.lRequestTime);
		this.requestNum += ui.requestNum;

		this.fImpressTime = Math.min(this.fImpressTime, ui.fImpressTime);
		this.lImpressTime = Math.max(this.lImpressTime, ui.lImpressTime);
		this.impressNum += ui.impressNum;

		this.fClickTime = Math.min(this.fClickTime, ui.fClickTime);
		this.lClickTime = Math.max(this.lClickTime, ui.lClickTime);
		this.clickNum += ui.clickNum;
	}
}
