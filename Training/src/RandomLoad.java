import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RandomLoad {
	void loadTable() throws InterruptedException {
		ExecutorService asd = Executors.newFixedThreadPool(30);
		int i = 0;
		while (i < 10) {
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
		
	}
	void updateLoad() {
		ExecutorService asd = Executors.newFixedThreadPool(50);
		int i = 0;
		while (i < 50) {
			asd.submit(new UpdateLoad());
			i++;
		}
		i = 0;
		while (i < 20) {
			asd.submit(new Update2());
			i++;
		}
		
	}
	void selectLoad() {
		ExecutorService asd = Executors.newFixedThreadPool(50);
		int i = 0;
		while (i < 50) {
			asd.submit(new SelectLoad());
			i++;
		}
	}
	void MixedLoad() {
		
	}
	
	
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
			String SQL = "  create table randomload (roll number , name varchar2(40),  mark1 number , mark2 number, mark3 number, mark4 number, mark5 number, mark6 number, primary key (roll,name)) ";
			stmt.execute(SQL);
			//SQL = " create index idx on randomload(name)";
			//stmt.execute(SQL);
			System.out.println("Created Tables and indexes, Starting Load");
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
				PreparedStatement pstmt = oraCon.prepareStatement("insert into RandomLoad (roll, name, mark1,mark2,mark3,mark4, mark5, mark6) values (ora.nextval,?,?,?,?,?,?,?)");
				int i = 0;
				while (i < 30099900) {
				/*	pstmt.setInt(1 , oraSequence.nextVal());
					pstmt.setString(2, OraRandom.randomString(40));
					pstmt.setInt(3, OraRandom.randomSkewInt(100));
					pstmt.setInt(4,  OraRandom.randomSkewInt(200));
					pstmt.setInt(5,  OraRandom.randomSkewInt(400));
					pstmt.setInt(6, OraRandom.randomSkewInt(800));
					pstmt.setInt(7,  OraRandom.randomUniformInt(1600));
					pstmt.setInt(8,  OraRandom.randomUniformInt(3200)); */
					
					pstmt.setString(1, OraRandom.randomString(40));
					pstmt.setInt(2, OraRandom.randomSkewInt(100));
					pstmt.setInt(3,  OraRandom.randomSkewInt(200));
					pstmt.setInt(4,  OraRandom.randomSkewInt(400));
					pstmt.setInt(5, OraRandom.randomSkewInt(800));
					pstmt.setInt(6,  OraRandom.randomUniformInt(1600));
					pstmt.setInt(7,  OraRandom.randomUniformInt(3200));
					//pstmt.execute();
					
					
					
					pstmt.addBatch();
					if (i%10 == 0) {
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
				PreparedStatement pstmt = oraCon.prepareStatement("Update RandomLoad set mark1 = ? where roll = ? ");
				String SQL = "select max(roll) from randomload";
				Statement stmt = oraCon.createStatement();
				ResultSet rs = stmt.executeQuery(SQL);
				int maxvalue= 0;
				while(rs.next()) {
					maxvalue = rs.getInt(1);
				}
				System.out.println(maxvalue);
				rs.close();
				stmt.close();
				int i = 0;
				while (i < 4000) {
					pstmt.setInt(2 , OraRandom.randomUniformInt(maxvalue));
					pstmt.setInt(1, OraRandom.randomSkewInt(100));
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
	class Update2 implements Runnable{
		public void run() {
			try {
				System.out.println("Staring Update Thread -->" + Thread.currentThread().getName());
				Connection oraCon = DBConnection.getOraConn();
				PreparedStatement pstmt = oraCon.prepareStatement("Update temp set mark1 = ? where roll = ? ");
				String SQL = "select max(roll) from temp";
				Statement stmt = oraCon.createStatement();
				ResultSet rs = stmt.executeQuery(SQL);
				int maxvalue= 0;
				while(rs.next()) {
					maxvalue = rs.getInt(1);
				}
				System.out.println(maxvalue);
				rs.close();
				stmt.close();
				int i = 0;
				while (i < 4000) {
					pstmt.setInt(2 , OraRandom.randomUniformInt(maxvalue));
					pstmt.setInt(1, OraRandom.randomSkewInt(100));
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
				PreparedStatement pstmt = oraCon.prepareStatement("select mark1 from  RandomLoad where roll =?");

				int i = 0;
				while (true) {
					while (i < 1000000) {
						int l = OraRandom.randomUniformInt(maxvalue);
						pstmt.setInt(1, l);
						 rs = pstmt.executeQuery();
						while(rs.next()) {
							rs.getInt(1);
						}
						i++;
					}
				}
	
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
