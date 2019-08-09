import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

class Table {
	private String OWNER;
	private String TABLE_NAME;
	private Connection oracon = null;
	private String InsertQuery;
	Table(String name){
		try {
			OWNER = name.substring(0, name.indexOf('.')).toUpperCase();
			TABLE_NAME = name.substring(name.indexOf('.')+1,name.length()).toUpperCase();
		}
		catch (Exception E) {
			System.out.println("Table Name Format ERROR");
		}
	}
	
	String getInsertQuery() {
		if (CheckTable()) {
			if (oracon!=null) {
				try {
					PreparedStatement pstmt = oracon.prepareStatement("select column_name,data_type,data_lenght from dba_tab_columns where owner=? and table_name=?");
					pstmt.setString(1, OWNER);
					pstmt.setString(2, TABLE_NAME);
					
					
				}
				catch(Exception E) {
					E.printStackTrace();
				}
				
			}
		}
		return null;
	}
	
	
	boolean CheckTable(){
		try {
			oracon = DBConnection.getOraConn();
			int count = 0 ;
			if (oracon!=null) {
				PreparedStatement pstmt = oracon.prepareStatement("select count(*) from dba_tables where owner=? and table_name=?");
				pstmt.setString(1, OWNER);
				pstmt.setString(2, TABLE_NAME);
				ResultSet rs = pstmt.executeQuery();
				while (rs.next()) {
					count = rs.getInt(1);
				}
				rs.close();
				pstmt.close();
				return count==1?true:false;
			}
			else {
				System.out.println("Could not get connection");
				return false;
			}
		}
		catch(Exception E) {
			return false;
		}
	}
	
	
	

}
