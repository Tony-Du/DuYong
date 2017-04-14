package com.iflytek.gnome.analysis.mobile.dsp;

import java.util.List;
import java.util.Map;

import org.apache.avro.reflect.Nullable;

public class RequestProtocol {

  // auth id
  @Nullable
  public String authId;
  // token
  @Nullable
  public String token;
  // array of impression
  @Nullable
  public List<Imp> imp;
  // device info
  @Nullable
  public Device device;
  // App info
  @Nullable
  public App app;
  // User info
  @Nullable
  public User user;
  // extends info
  @Nullable
  public Map<String, String> ext;
  
  public static class Imp {
    // ad id
	@Nullable
    public String id;
    // ad type
    public int type;
    // action type
    public int actionType;
    @Nullable
    public Float bidFloor;
    @Nullable
    public String bidFloorCur;
    // banner ad
    @Nullable
    public Banner banner;
    // video ad
    @Nullable
    public Video video;
    // native ad
    @Nullable
    public NativeAd Native;
    // context
    @Nullable
    public Context context;
    // extends info
    @Nullable
    public Map<String, String> ext;
  }
  
  public static class Banner {
	@Nullable
    public Integer w;
	@Nullable
    public Integer h;
    
    public int mtype;
  }
  
  public static class Video {
	@Nullable
    public Integer w;
	@Nullable
    public Integer h;
    // play format
	@Nullable
    public String mimes;
    // min play time
	@Nullable
    public Integer minDuration;
    // max play time
	@Nullable
    public Integer maxDuration;
    //
	@Nullable
    public Integer startDelay;
    //
    public int combination;
  }
  
  public static class NativeAd {
    //
	@Nullable
    public Integer num;
    //
	@Nullable
	public Integer imageWidth;
    //
	@Nullable
	public Integer imageHeight;
    //
	@Nullable
    public Integer iconWidth;
    //
	@Nullable
    public Integer iconHeight;
    //
    @Nullable
    public Integer maxTitleLenth;
    //
    @Nullable
    public Integer maxDescLength;
  }
  
  public static class Context {
    //
    public ContextVideo video;
  }
  
  public static class ContextVideo {
    // 
	@Nullable
    public String title;
    //
	@Nullable
    public List<String> tags;
    //
	@Nullable
    public Integer duration;
    //
	@Nullable
    public Integer channelId;
  }
  
  public static class Device {
    // user-agent
	@Nullable
    public String ua;
    // geo
	@Nullable
    public Geo geo;
    // ip
    @Nullable
    public String ip;
    // device type
    @Nullable
    public Integer deviceType;
    // make
    @Nullable
    public String make;
    // model
    @Nullable
    public String model;
    // os
    @Nullable
    public String os;
    // os version
    @Nullable
    public String osv;
    // screen width
    @Nullable
    public Integer w;
    // screen height
    @Nullable
    public Integer h;
    // screen ppi
    @Nullable
    public Integer ppi;
    // orientation
    @Nullable
    public Integer orientation;
    // langage
    @Nullable
    public String language;
    //
    @Nullable
    public String ifa;
    //
    @Nullable
    public String did;
    //
    @Nullable
    public Integer didha;
    //
    @Nullable
    public String dpid;
    //
    @Nullable
    public Integer dpidha;
    //
    @Nullable
    public String mac;
    //
    @Nullable
    public Integer macha;
    // operator
    @Nullable
    public Integer carrier;
    // net type
    @Nullable
    public Integer connectionType;
    //
    @Nullable
    public Map<String, String> ext;
    
    @Nullable
    public String pnumber;
    
    public int pnumberha;
  }
  
  public static class Geo {
    //
	@Nullable
    public Float latitude;
    //
	@Nullable
	public Float longitude;
    //
	@Nullable
    public String country;
    //
	@Nullable
    public String province;
    //
	@Nullable
    public String city;
    //
	@Nullable
    public String district;
    //
	@Nullable
    public String street;
  }
  
  public static class App {
    // app id
	@Nullable
    public String id;
    // app package
	@Nullable
    public String bundle;
    // store url
	@Nullable
    public String storeUrl;
    // app category
	@Nullable
    public Integer catogery;
    // app keywords
	@Nullable
    public String keywords;
    // extends info
	@Nullable
    public Map<String, String> ext;
  }
  
  public static class User {
    // user id
	@Nullable
    public String id;
    // year of birth
	@Nullable
    public Integer yob;
    // gender
	@Nullable
    public Integer gender;
    // user keywords
	@Nullable
    public String keywords;
    // extends info
	@Nullable
    public Map<String, String> ext;
  }
}
