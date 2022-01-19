package th.co.ktb.fraud.monitas.tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class DatabaseHelper{

	
	
	public static void insert(Connection conn,String tableName, Map<String, Object> values) throws Exception {
		
		String[] columnArr = new String[values.keySet().size()];
		String[] valueArr = new String[values.keySet().size()];
		int i = 0;
		
		
		for(String key : values.keySet()) {
						
			columnArr[i] = key;
			valueArr[i] = "?";
			
			i++;
			
		}
		
		String columnsStr = String.join(",", columnArr);
		String valuesStr = String.join(",", valueArr);
		int paramIndex = 1;
		
		String sql = String.format("insert into %s (%s) values(%s)", tableName,columnsStr,valuesStr);
		
		try(PreparedStatement prst = conn.prepareStatement(sql)) {
			
			for(String key : values.keySet()) {
			
				prst.setObject(paramIndex++, values.get(key));
				
			}
			
			boolean effect = prst.execute();
			
			if(!effect) {
				
				throw new Exception("insert record not effect to database");
				
			}
			
		}catch(SQLException sqlex) {
			
			throw sqlex;
			
		}
		
	}
	
	public static void delete(Connection conn,String tableName,String condition) throws SQLException {
		
		String sql = String.format("delete from %s ", tableName);
		
		if(condition != null && !"".equals(condition)) {
			
			sql += condition;
			
		}
		
		try(PreparedStatement prst = conn.prepareStatement(sql)){
			
			prst.execute();
			
		}
		
	}
	
	public static ResultSet query(Connection conn,String sql,int page,int pageSize) throws SQLException {
		
		int offset = (page*pageSize) ;
		String querySql = String.format("%s limit %d offset %d", sql,pageSize,offset);
		
		try(PreparedStatement prst = conn.prepareStatement(sql)){
			
			return prst.executeQuery();
			
		}
		
	}
	
	public static int countTotalRecord(Connection conn,String tableName) throws SQLException {
		
		
		String sql = String.format("select count(*) from :s", tableName);
		
		try(PreparedStatement prst = conn.prepareStatement(sql)){
			
			ResultSet result = prst.executeQuery();
			
			result.next();
			return result.getInt(1);
			
		}
		
	}

	
	public static Connection getConnection(String url,String username,String password,String driverName) throws Exception{
		
		
		Connection conn = null;
		
		try{
			
			Class.forName(driverName);
			conn = DriverManager.getConnection(url, username, password);
			
		}catch(Exception ex){
			
			if(conn != null) {
				conn.close();
			}
			
		}
		
		return conn;
		
	}
	

}
