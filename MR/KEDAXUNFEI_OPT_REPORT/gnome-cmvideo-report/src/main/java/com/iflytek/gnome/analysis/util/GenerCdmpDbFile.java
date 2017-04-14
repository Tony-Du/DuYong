package com.iflytek.gnome.analysis.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;

public class GenerCdmpDbFile {
	
	public static void main(String[] args){
	    
	    

        Connection connection = null;
        try{
            //这里要改成从配置文件读取信息
            Class.forName("oracle.jdbc.driver.OracleDriver");
            String user = "cdmpview";
            String password = "cmvb$cdmp";
            String url = "jdbc:oracle:thin:@172.16.12.201:1521:cdmpdb1";
            connection = DriverManager.getConnection(url,user,password);
            
            String sql1 = "select term_prod_id, DEPT_ID from tv_dim_term_prod";
            String sql2 = "select business_id, DEPT_ID from tv_xf_dim_busi";
            String sql3 = "select chn_id, DEPT_ID from tv_yxs_chn_cfg7";
            String sql4 = "select chn_id, chn_type_ID from tv_yxs_chn_cfg7";
            String sql5 = "SELECT distinct TERM_PROD_ID, TERM_PROD_TYPE_ID, TERM_PROD_CLASS_ID, TERM_VIDEO_TYPE_ID FROM CDMP_MK.TDIM_PROD_ID";
                    
            Statement stmt = connection.createStatement();
            ResultSet rs1 = stmt.executeQuery(sql1);
            
            while(rs1.next()){
                deptTermProd.put(rs1.getString(1),rs1.getString(2));
            }
            
            ResultSet rs2 = stmt.executeQuery(sql2);
            while(rs2.next()){
                deptBusi.put(rs2.getString(1),rs2.getString(2));
            }
            
            ResultSet rs3 = stmt.executeQuery(sql3);
            while(rs3.next()){
                deptChn.put(rs3.getString(1),rs3.getString(2));
            }
            
            ResultSet rs4 = stmt.executeQuery(sql4);
            while(rs4.next()){
                chntype.put(rs4.getString(1),rs4.getString(2));
            }
            
            ResultSet rs5 = stmt.executeQuery(sql5);
            while(rs5.next()){
                termProdType.put(rs5.getString(1),rs5.getString(2));
                termProdClass.put(rs5.getString(1),rs5.getString(3));
                termProdVideo.put(rs5.getString(1),rs5.getString(4));
            }
            
            
            
            
            
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    
	    
	    
	    
	    
	    
	    
		
		try {
			System.out.println(GenerCdmpDbFile.getChnIDType("200300010010001")); //C01 --false
			System.out.println(GenerCdmpDbFile.getDepartment("P0003126", "", ""));//D01
			System.out.println(GenerCdmpDbFile.getDepartment("", "", "101400010000006")); //D02
			System.out.println(GenerCdmpDbFile.getDepartment("", "B1000100600", "311600100000001"));//D04
			System.out.println(GenerCdmpDbFile.getDepartment("P0003126", "B1000100600", "311600100000001"));//D01
			System.out.println(GenerCdmpDbFile.getTermProdVideo("P0000023"));
			System.out.println(GenerCdmpDbFile.getTermProdClass("P0000023"));
			System.out.println(GenerCdmpDbFile.getTermProdVideo("P0000023"));
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//缓存cdmp维度数据的容器
	private static HashMap<String,String> deptTermProd = new HashMap<String,String>();
	private static HashMap<String,String> deptBusi = new HashMap<String,String>();
	private static HashMap<String,String> deptChn = new HashMap<String,String>();
	private static HashMap<String,String> chntype = new HashMap<String,String>();
	private static HashMap<String, String> termProdVideo = new HashMap<String,String>();
	private static HashMap<String, String> termProdClass = new HashMap<String,String>();
	private static HashMap<String, String> termProdType = new HashMap<String,String>();
	
	//将所需要的数据加载到缓存中的容器
	static{}
	
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
	
//	public String getDepartment(String termProdID, String businessID, String chnID) throws SQLException{
//		
//		String dept = "D99"; //无法判断部门时写为D99;
//		String sql1 = "select DEPT_ID from tv_dim_term_prod where term_prod_id = ? ";
//		String sql2 = "select DEPT_ID from tv_xf_dim_busi where business_id = ? ";
//		String sql3 = "select DEPT_ID from tv_yxs_chn_cfg7 where chn_id = ? ";
//		
//		//判断一下传入参数是否合法，不合法赋值为"-1"以免在jdbc中引起异常，如果传入值全不合法则直接返回部门默认值；
//		if(StringUtils.isBlank(termProdID) && StringUtils.isBlank(businessID) && StringUtils.isBlank(chnID)){
//			return dept;
//		}else{
//			
//			if(termProdID.equals("P0000521") || termProdID.equals("P0000922")){//特殊类型直接用渠道判断，判断不出合法结果则返回默认-1；
//				
//				PreparedStatement pstmt3 = this.connection.prepareStatement(sql3);
//				pstmt3.setString(1, chnID);
//				ResultSet rs3 = pstmt3.executeQuery();
//				
//				if(rs3.next()){
//					if(StringUtils.isNotBlank(rs3.getString(1))){
//						dept = rs3.getString(1);
//					}
//					return dept;
//				}else{
//					return dept;
//				}
//				
//			}else{ //正常类型的判断f按照优先级判断
//					
//				PreparedStatement pstmt = this.connection.prepareStatement(sql1);
//				pstmt.setString(1, termProdID);
//				ResultSet rs = pstmt.executeQuery();
//				
//				if(rs.next() && StringUtils.isNotBlank(rs.getString(1))){
//					
//					dept = rs.getString(1);
//					return dept;
//				
//				}else{
//					
//					PreparedStatement pstmt2 = this.connection.prepareStatement(sql2);
//					pstmt2.setString(1, businessID);
//					ResultSet rs2 = pstmt.executeQuery();
//					
//					if(rs2.next() && StringUtils.isNotBlank(rs2.getString(1))){
//						
//						dept = rs2.getString(1);
//						return dept;
//					
//					}else{
//						
//						PreparedStatement pstmt3 = this.connection.prepareStatement(sql3);
//						pstmt3.setString(1, chnID);
//						ResultSet rs3 = pstmt3.executeQuery();
//						
//						if(rs3.next() && StringUtils.isNotBlank(rs3.getString(1))){
//								
//								dept = rs3.getString(1);
//						}
//						
//						return dept;
//					}
//				}
//			}
//		}
//	}
//public String getcChnIDType(String chnID) throws SQLException{
//	
//	String chnTypeID = "-1"; //无法判断渠道
//	
//	if(StringUtils.isNotBlank(chnID)){
//		
//		String sql = "select chn_id_type from tv_yxs_chn_cfg7 where chn_id = ? ";
//		PreparedStatement pstmt = this.connection.prepareStatement(sql);
//		pstmt.setString(1, chnID);
//		ResultSet rs = pstmt.executeQuery();
//		
//		if(rs.next() && StringUtils.isNotBlank(rs.getString(1))){
//			
//			chnTypeID = rs.getString(1);
//			return chnTypeID;
//			
//		}else{
//			return chnTypeID;
//		}
//	}else{
//		return chnTypeID;
//	}
//	
//}
