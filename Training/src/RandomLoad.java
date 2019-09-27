import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RandomLoad {
	void createTable() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			try {
				String SQL = "drop table RandomLoad";
				stmt.execute(SQL);
			}
			catch(Exception E) {
				
			}
			String SQL = "  create table randomload (roll number primary key, name varchar2(3500), mark1 number not null, mark2 number, mark3 number not null)  ";
			stmt.execute(SQL);

			System.out.println("Created Tables and indexes, Starting Load");
			ExecutorService asd = Executors.newFixedThreadPool(30);
			int i = 0;
			while (i < 30) {
				asd.submit(new InsertLoad());
				i++;
			}
			i = 0 ;
			System.out.println("Loading Data... Sleepin for 10 seconds");
			asd.shutdown();
			while(!asd.isShutdown()) {
				Thread.currentThread().sleep(1000);
			}
			asd.shutdownNow();
			asd = Executors.newFixedThreadPool(30);
			
			while (i < 30) {
				asd.submit(new SelectLoad());
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
				PreparedStatement pstmt = oraCon.prepareStatement("insert into RandomLoad (roll, name,mark1,mark2,mark3) values (?,?,?,?,?)");
				int i = 0;
				while (i < 2000000) {
					pstmt.setInt(1 , oraSequence.nextVal());
					pstmt.setString(2, OraRandom.randomString(3500));
					pstmt.setInt(3, OraRandom.randomSkewInt(100));
					pstmt.setInt(4,  OraRandom.randomSkewInt(100));
					pstmt.setInt(5,  OraRandom.randomSkewInt(500));
					
					pstmt.addBatch();
					if (i%10000 == 0) {
						pstmt.executeBatch();
						System.out.println("loaded " + oraSequence.getval());
					}
					i++;
				}
				pstmt.close();
				oraCon.close();
				  DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");  
				   LocalDateTime now = LocalDateTime.now();  
				   System.out.println(dtf.format(now));  
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
				PreparedStatement pstmt = oraCon.prepareStatement("Update RandomLoad set name = ? where roll = ? ");
				String SQL = "select max(roll) from randomload";
				Statement stmt = oraCon.createStatement();
				ResultSet rs = stmt.executeQuery(SQL);
				int maxvalue= 0;
				while(rs.next()) {
					maxvalue = rs.getInt(1);
				}
				rs.close();
				stmt.close();
				int i = 0;
				while (i < 1000000) {
					pstmt.setInt(2 , OraRandom.randomUniformInt(maxvalue));
					pstmt.setString(1, OraRandom.randomString(20));
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
				String SQL = "select max(roll) from randomload";
				Statement stmt = oraCon.createStatement();
				ResultSet rs = stmt.executeQuery(SQL);
				int maxvalue = 0;
				while (rs.next()) {
					maxvalue = rs.getInt(1);
				}
				PreparedStatement pstmt = oraCon.prepareStatement("select mark1 from  RandomLoad where roll >? and roll <?");

				int i = 0;
				while (i < 1000000) {
					int l = OraRandom.randomUniformInt(maxvalue);
					pstmt.setInt(1, l);
					pstmt.setInt(2, l+20000);
					 rs = pstmt.executeQuery();
					while(rs.next()) {
						rs.getInt(1);
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
				PreparedStatement pstmt = oraCon.prepareStatement("delete from RandomLoad where roll = ? ");
				String SQL = "select max(roll) from randomload";
				Statement stmt = oraCon.createStatement();
				ResultSet rs = stmt.executeQuery(SQL);
				int maxvalue= 0;
				while(rs.next()) {
					maxvalue = rs.getInt(1);
				}
				rs.close();
				stmt.close();
				int i = 0;
				while (i < 1000000) {
					pstmt.setInt(1 , OraRandom.randomUniformInt(maxvalue));
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
