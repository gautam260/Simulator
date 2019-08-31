import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LoadTable {
	private String TableName;
	private String Owner;
	private boolean isValid = false;
	private ArrayList<String> columntypes = new ArrayList<>();
	private String InsertSQL;
	private ArrayList<Integer> Cardinality;
	LoadTable(String name,ArrayList<Integer> cardinality){
		try {
			Owner = name.substring(0, name.indexOf('.')).toUpperCase();
			TableName = name.substring(name.indexOf('.')+1,name.length()).toUpperCase();
			this.Cardinality = cardinality;
			CheckTable();
			getInsertStatement();
		}
		catch(Exception E) {
			System.out.println("TableName not proper");
		}
	}
	
	void loadData(int a) {
		try {
			if (isValid) {
				ExecutorService asd = Executors.newFixedThreadPool(10);
				int i = 0;
				while (i < a) {
					asd.submit(new LoadDataParallel());
					i++;
				}
				asd.shutdown();
				
			}
			else {
				System.out.println("Table not present");
			}
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	
	
	class LoadDataParallel implements Runnable{
		public void run() {
			try {
				System.out.println("Loading Data");
				Connection oraCon = DBConnection.getOraConn();
				PreparedStatement pstmt = oraCon.prepareStatement(InsertSQL);
				int i = 1;
				
				while (i < 1000000) {
					int j = 1;
					while (j<=columntypes.size()) {
						if (columntypes.get(j-1).equals("NUMBER")) {
																											
							pstmt.setInt(j, OraRandom.randomSkewInt(Cardinality.get(j - 1)));
						}
						else if (columntypes.get(j-1).equals("VARCHAR2")){
							pstmt.setString(j,OraRandom.randomString(20));
						}
						j++;
						
					}
					pstmt.addBatch();
					if (i%10000 == 0 ) {
						pstmt.executeBatch();
					}
					i++;
				}
				pstmt.executeBatch();
				pstmt.close();
				oraCon.close();
				
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
	
	
	
	
	private void getInsertStatement() {
		try {

			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			String SQL = " select column_name from dba_tab_columns where owner='"+ Owner + "' and  table_name='"+ TableName +"' order by column_id asc";
			ResultSet rs = stmt.executeQuery(SQL);
			StringBuilder InsertSQL = new StringBuilder("insert into " + Owner +"." + TableName +"(");
			StringBuilder tempSQL = new StringBuilder("(");
			while (rs.next()) {
				InsertSQL.append(rs.getString(1) +",");
				tempSQL.append("?,");
			}
			rs.close();
			stmt.close();
			oraCon.close();
			InsertSQL.delete(InsertSQL.length() -1 , InsertSQL.length());
			InsertSQL.append(") values ");
			tempSQL.delete(tempSQL.length() - 1, tempSQL.length());
			tempSQL.append(")");
			InsertSQL.append(tempSQL.toString());
			this.InsertSQL =  InsertSQL.toString();
		
		}
		catch(Exception E) {
			E.printStackTrace();
		}
		
	}
	
	private void CheckTable() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			String SQL = "select count(*) from dba_tables where owner='" + Owner + "' and table_name='" + TableName + "'";
			ResultSet rs = stmt.executeQuery(SQL);
			int result = 0;
			while(rs.next()) {
				result = rs.getInt(1);
			}
			rs.close();
			if (result == 1) {
				SQL = "select data_type from dba_tab_columns where owner='" + Owner + "' and table_name='" + TableName + "' order by column_id asc";
				rs = stmt.executeQuery(SQL);
				while (rs.next()) {
					columntypes.add(rs.getString(1));
				}
				isValid = true;
			}
			rs.close();
			stmt.close();
			oraCon.close();
			
		}
		catch(Exception E) {
			E.printStackTrace();
			isValid = false;
		}
	}
	
	
	
}
