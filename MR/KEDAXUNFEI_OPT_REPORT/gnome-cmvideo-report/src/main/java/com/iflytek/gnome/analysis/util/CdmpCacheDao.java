package com.iflytek.gnome.analysis.util;

import java.sql.SQLException;
import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;

public class CdmpCacheDao {
	
	public static void main(String[] args){
		
		try {
			System.out.println(CdmpCacheDao.getChnIDType("200300010010001")); //C01 --false
			System.out.println(CdmpCacheDao.getDepartment("P0003126", "", ""));//D01
			System.out.println(CdmpCacheDao.getDepartment("", "", "101400010000006")); //D02
			System.out.println(CdmpCacheDao.getDepartment("", "B1000100600", "311600100000001"));//D04
			System.out.println(CdmpCacheDao.getDepartment("P0003126", "B1000100600", "311600100000001"));//D01
			System.out.println(CdmpCacheDao.getTermProdVideo("P0000023"));
			System.out.println(CdmpCacheDao.getTermProdClass("P0000023"));
			System.out.println(CdmpCacheDao.getTermProdVideo("P0000023"));
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//缓存cdmp维度数据的容器
	public static HashMap<String,String> deptTermProd = new HashMap<String,String>();
	public static HashMap<String,String> deptBusi = new HashMap<String,String>();
	public static HashMap<String,String> deptChn = new HashMap<String,String>();
	public static HashMap<String,String> chntype = new HashMap<String,String>();
	public static HashMap<String, String> termProdVideo = new HashMap<String,String>();
	public static HashMap<String, String> termProdClass = new HashMap<String,String>();
	public static HashMap<String, String> termProdType = new HashMap<String,String>();
	public static HashMap<String, String> tdim_chn_detail = new HashMap<String,String>();
	public static HashMap<String, String> tdim_prod_id = new HashMap<String,String>();
	
	public static HashMap<String, String> tdim_dept_id_copy = new HashMap<String,String>();
	public static HashMap<String, String> tdim_prod_type_id_copy = new HashMap<String,String>();
	public static HashMap<String, String> tdim_prod_id_copy = new HashMap<String,String>();
	public static HashMap<String, String> tdim_prod_class_id_copy = new HashMap<String,String>();
	public static HashMap<String, String> tdim_chn_type_copy = new HashMap<String,String>();
	public static HashMap<String, String> tdim_chn_detail_copy = new HashMap<String,String>();
	
	//根据渠道ID - chn_id_new 获取渠道类型；
	public static String getChnIDType(String chnIDNew) throws SQLException{
		  
		String chnTypeID = "C99"; 
		if(StringUtils.isNotBlank(chnIDNew)){
			
			if(StringUtils.isNotBlank(chntype.get(chnIDNew))){
				
				return chntype.get(chnIDNew);
				
			}else{
				return chnTypeID;
			}
		
		}else{
			return chnTypeID;
		}
	}

	//根据终端产品ID term_prod_id -获取TERM_PROD_TYPE_ID；
	public static String getTermProdType(String termProdID){
		
		String theTPType = "T9999999"; //T0000001
		if(StringUtils.isNotBlank(termProdID)){
			
			if(StringUtils.isNotBlank(termProdType.get(termProdID))){
				theTPType = termProdType.get(termProdID);
				return theTPType;
			}else{
				return theTPType;
			}
		
		}else{
			return theTPType;
		}
	}
	
	//根据终端产品ID term_prod_id -获取termProdVideo；
	public static String getTermProdVideo(String termProdID){
		
		String theTPVideo = "TV99999"; //TV00001
		if(StringUtils.isNotBlank(termProdID)){
			
			if(StringUtils.isNotBlank(termProdVideo.get(termProdID))){
				
				theTPVideo = termProdVideo.get(termProdID);
				return theTPVideo;
			
			}else{
				
				return theTPVideo;
			}
		}else{
			
			return theTPVideo;
		}
	}
	
	//根据终端产品ID term_prod_id -获取TermProdClass；
	public static String getTermProdClass(String termProdID){
		
		String theTPClass = "C9999999"; //C0000001
		if(StringUtils.isNotBlank(termProdID)){
			
			if(StringUtils.isNotBlank(termProdClass.get(termProdID))){
				
				theTPClass = termProdClass.get(termProdID);
				return theTPClass;
			
			}else{
				
				return theTPClass;
			}
		}else{
			
			return theTPClass;
		}
	}
	
	//根据终端产品ID/业务ID/渠道ID - 获取部门ID；
	public static String getDepartment(String termProdID, String businessID, String chnID){
		
		String deptDefault = "D99";
		
		if(StringUtils.isBlank(termProdID) && StringUtils.isBlank(businessID) && StringUtils.isBlank(chnID)){
			
			return deptDefault;//如果输入的参数全为空，则直接返回默认ID
			
		}else{
			
			
			if(StringUtils.equalsIgnoreCase(termProdID, "P0000521")|| StringUtils.equalsIgnoreCase(termProdID, "P0000922"))
			{
				//特殊终端产品，使用渠道判断部门
				if(StringUtils.isNotBlank(chnID) && StringUtils.isNotBlank(deptChn.get(chnID))){
					return deptChn.get(chnID);
				}else{
					return deptDefault;
				}
				
			}else{
				
				//其他终端产品采用正常的判断优先级
				if(StringUtils.isNotBlank(termProdID) && StringUtils.isNotBlank(deptTermProd.get(termProdID))){
					return deptTermProd.get(termProdID);
				}else{
					
					if(StringUtils.isNotBlank(businessID) && StringUtils.isNotBlank(deptBusi.get(businessID))){
						return deptBusi.get(businessID);
					}else{
						
						if(StringUtils.isNotBlank(chnID) && StringUtils.isNotBlank(deptChn.get(chnID))){
							return deptChn.get(chnID);
						}else{
							return deptDefault;
						}
					}
					
				}
				
			}
		}
	}
}
