package com.mganaly.cdmp.data.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import com.mganaly.conf.GlobalEv;

public class mysql_tbl_writer implements Writable, DBWritable 
{
	   //`regist_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间',
	protected int _colNum; // RIGIST_DAY is generate here
	protected String [] _columns = null;
	  
          
     public mysql_tbl_writer(String [] cols) throws IOException 
     { 
    	 _colNum = cols.length + 1;
    	 _columns = new String[_colNum];

         _columns[0] = GlobalEv.getCurTime();
         
         for (int i = 0; i < cols.length; ++i) {
         	_columns[i+1] = cols[i];
         }
     } 
     
     public String toString() 
     {  
     	String strCols = "";
     	for (String col : _columns) {
     		strCols += col +  ColParser.SEPRATOR;
     	}
     	return strCols.substring(0, strCols.length()-1);
     }
     
		public void readFields(ResultSet resultSet) throws SQLException {
			for (int i = 1; i < _colNum; ++i) {
				_columns[i] = resultSet.getString(i);
			}
		}
		
		public void write(DataOutput out) throws IOException {
			for (String col : _columns) {
				out.writeUTF(col);
			}
			
		}
		
		public void readFields(DataInput in) throws IOException {
         for (int i = 1; i < _colNum; ++i) {
         	_columns[i] = in.readUTF();
         }
		}
		
		public void write(PreparedStatement statement) throws SQLException {

	         for (int i = 0; i < _colNum; ++i) {
	         	statement.setString(i+1, _columns[i]);
	         }
         
		} 

}
