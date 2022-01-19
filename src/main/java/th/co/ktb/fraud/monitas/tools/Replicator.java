package th.co.ktb.fraud.monitas.tools;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class Replicator implements Closeable{

	Connection sourceConnection = null;
	Connection targetConnection = null;
	
	public Replicator(Connection souConnection,Connection tarConnection) {
		
		this.sourceConnection = souConnection;
		this.targetConnection = tarConnection;
		
	}
	
	public void deleteAndCopy(String tableName) throws Exception {
		
		try {
			
			targetConnection.setAutoCommit(false);
			
			DatabaseHelper.delete(targetConnection, tableName, null);
			
			int totalRecord = DatabaseHelper.countTotalRecord(sourceConnection, tableName);
			int page = 0;
			int pageSize = 1000;
			
			do {
				
				String sql = String.format("select * from %s",tableName);
				ResultSet result = DatabaseHelper.query(sourceConnection, tableName, page, pageSize);
				
				ResultSetMetaData meta = result.getMetaData();
				String[] columns = new String[meta.getColumnCount()];
				
				for(int i=0 ; i<meta.getColumnCount() ; i++) {
					
					columns[i] = meta.getColumnName(i+1);
					
				}
				
				while(result.next()) {
					
					Map<String,Object> values = new HashMap<String, Object>();
					
					for(String columnName : columns) {
						
						values.put(columnName, result.getObject(columnName));
						
					}
					
					DatabaseHelper.insert(targetConnection, tableName, values);
					
				}
				
				page++;
				
			}while(page*pageSize < totalRecord) ;
			
			
		
		}finally {
			
			targetConnection.commit();
			targetConnection.setAutoCommit(true);
			
		}
		
	}
	

	@Override
	public void close() throws IOException {
		
		if(sourceConnection != null) {
			
			try {
				sourceConnection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		if(targetConnection != null) {
			
			try {
				targetConnection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		
	}
	
}
