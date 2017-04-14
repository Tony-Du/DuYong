package com.iflytek.gnome.analysis.model;

import org.apache.avro.reflect.Nullable;

public class DmModel
{
	@Nullable
	public String openudid;
	@Nullable
	public String idfv;
	@Nullable
	public String idfa;
	@Nullable
	public String os;
	@Nullable
	public String osv;
	@Nullable
	public String imei;
	@Nullable
	public String mac;
	@Nullable
	public String cid;
	@Nullable
	public String ip;
	@Nullable
	public String adid;
	@Nullable
	public String token;
	@Nullable
	public String realIp;
	@Nullable
	public String aaid;
	public long timestamp;
	
	public int impress;
	public int click;
	
	public DmModel()
	{
		
	}
	public DmModel(DmModel other)
	{
		this.aaid = other.aaid;
		this.idfa = other.idfa;
		this.idfv = other.idfv;
		this.os = other.os;
		this.osv = other.osv;
		this.imei = other.imei;
		this.mac = other.mac;
		this.cid = other.cid;
		this.ip = other.ip;
		this.adid = other.adid;
		this.token = other.token;
		this.realIp = other.realIp;
		this.timestamp = other.timestamp;
		this.impress = other.impress;
		this.click = other.click;
	}
}
