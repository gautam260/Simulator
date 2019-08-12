import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HugePages {
	void createTable() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			try {
				String SQL = "drop table HugePages";
				stmt.execute(SQL);
			}
			catch(Exception E) {
				
			}
			String SQL = " create table HugePages (roll number, details varchar2(3800)) ";
			stmt.execute(SQL);
			SQL = "create index idx on HugePages(roll) storage (buffer_pool keep)";
			stmt.execute(SQL);
			System.out.println("Created Tables and indexes, Starting Load");
			ExecutorService asd = Executors.newFixedThreadPool(400);
			int i = 0;
			while (i < 150) {
				asd.submit(new InsertLoad());
				i++;
			}
			i = 0 ;
			Thread.currentThread().sleep(5000);
			while (i < 2) {
				asd.submit(new DeleteLoad());
				asd.submit(new UpdateLoad());
				i++;
			}
			
			
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	void PageTable() {
		
	}
	class InsertLoad implements Runnable{
		public void run() {
			try {
				System.out.println("Staring Insert Thread -->" + Thread.currentThread().getName());
				Connection oraCon = DBConnection.getOraConn();
				PreparedStatement pstmt = oraCon.prepareStatement("insert into HugePages (roll, details) values (?,?)");
				int i = 0;
				while (i < 1000000) {
					pstmt.setInt(1 , oraSequence.nextVal());
					pstmt.setString(2, OraRandom.randomString(3800));
					pstmt.addBatch();
					if (i%10 == 0) {
						pstmt.executeBatch();
					}
					i++;
				}
				pstmt.close();
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
		
	}
	class UpdateLoad implements Runnable{
		public void run() {
			try {
				System.out.println("Staring Update Thread -->" + Thread.currentThread().getName());
				Connection oraCon = DBConnection.getOraConn();
				PreparedStatement pstmt = oraCon.prepareStatement("Update HugePages set details = ? where roll = ? ");
				int i = 0;
				while (i < 1000000) {
					pstmt.setInt(2 , OraRandom.randomUniformInt(oraSequence.getval()));
					pstmt.setString(1, OraRandom.randomString(3800));
					pstmt.executeUpdate();
					i++;
				}
				pstmt.close();
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
	class SelectLoad implements Runnable{
		public void run() {
			try {
				System.out.println("Staring Select  Thread -->" + Thread.currentThread().getName());
				Connection oraCon = DBConnection.getOraConn();
				PreparedStatement pstmt = oraCon.prepareStatement("select count(details) from  HugePages ");
				int i = 0;
				while (i < 1000000) {
				
					ResultSet rs = pstmt.executeQuery();
					while(rs.next()) {
						rs.getString(1);
					}
					i++;
				}
				pstmt.close();
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
	
	
	
	
	
	class DeleteLoad implements Runnable{
		public void run() {
			try {
				System.out.println("Staring Delete Thread -->" + Thread.currentThread().getName());
				Connection oraCon = DBConnection.getOraConn();
				PreparedStatement pstmt = oraCon.prepareStatement("delete from HugePages where roll = ? ");
				int i = 0;
				while (i < 1000000) {
					pstmt.setInt(1 , OraRandom.randomUniformInt(oraSequence.getval()));
					pstmt.executeUpdate();
					i++;
				}
				pstmt.close();
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
		
	}
}
