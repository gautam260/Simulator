import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ClobData {
	
	void run() {
		
	}
	void updateLoad() {
		try {
			ExecutorService asd = Executors.newFixedThreadPool(30);
			int i = 0;
			while (i < 10) {
				asd.submit(new UpdateLoad());
				i++;
			}
			
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	void insertLoad() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			try {
				String SQL = "drop table clobdata";
				stmt.execute(SQL);
			}
			catch(Exception E) {
				
			}
			String SQL = "  create table clobdata (roll number primary key, name varchar2(20),details clob)  ";
			stmt.execute(SQL);

			System.out.println("Created Tables and indexes, Starting Load");
			ExecutorService asd = Executors.newFixedThreadPool(30);
			int i = 0;
			while (i < 10) {
				asd.submit(new InsertLoad());
				i++;
			}
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	
	class InsertLoad implements Runnable{
		public void run() {
			try {
				Connection oraCon = DBConnection.getOraConn();
				PreparedStatement pstmt = oraCon.prepareStatement("insert into clobdata(roll, name, details) values (?,?,?)");
				int i = 0 ;
				while (i < 1000000) {
					pstmt.setInt(1, oraSequence.nextVal());
					pstmt.setString(2, OraRandom.randomString(20));
					Clob temp = oraCon.createClob();
					temp.setString(1, OraRandom.randomString(OraRandom.randomInt(351201)));
					pstmt.setClob(3, temp);
					pstmt.executeUpdate();
					i++;
				}
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
	
	class SelectLoad implements Runnable{
		public void run() {
			try {
				Connection oraCon = DBConnection.getOraConn();
				String SQL = "select max(roll) from clobdata";
				Statement stmt = oraCon.createStatement();
				ResultSet rs = stmt.executeQuery(SQL);
				int maxvalue = 0;
				while (rs.next()) {
					maxvalue = rs.getInt(1);
				}
				PreparedStatement pstmt = oraCon.prepareStatement("select details from clobdata where roll = ? ");
				int i = 0 ;
				while (i < 1000000) {
					pstmt.setInt(1, OraRandom.randomUniformInt(maxvalue));
					Clob temp = oraCon.createClob();
					 rs = pstmt.executeQuery();
					while (rs.next()) {
						rs.getClob(1);
					}
					i++;
				}
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
	
	class UpdateLoad implements Runnable{
		public void run() {
			try {
				Connection oraCon = DBConnection.getOraConn();
				String SQL = "select max(roll) from clobdata";
				Statement stmt = oraCon.createStatement();
				ResultSet rs = stmt.executeQuery(SQL);
				int maxvalue = 0;
				while (rs.next()) {
					maxvalue = rs.getInt(1);
				}
				PreparedStatement pstmt = oraCon.prepareStatement("Update clobdata set details = ? where roll = ?");
				int i = 0 ;
				while (i < 1000000) {
					pstmt.setInt(2, OraRandom.randomUniformInt(maxvalue));
					Clob temp = oraCon.createClob();
					temp.setString(1, OraRandom.randomString(3512010));
					pstmt.setClob(1, temp);
					pstmt.executeUpdate();
					i++;
				}
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
	
}
