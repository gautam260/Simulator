import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class CBCPost {

	
	
	
	void createTable() {
		try {
			Connection oraCon = DBConnection.oraPostConn();
			Statement stmt = oraCon.createStatement();
			try {
				String SQL = "drop table RandomLoad";
				stmt.execute(SQL);
			}
			catch(Exception E) {
				System.out.println("Drop table failed or table doesnt exist");
			}
			String SQL = "  create table randomload(roll numeric primary key with (fillfactor=90), name varchar(20), mark1 numeric, mark2 numeric, mark3 numeric) with (fillfactor=90)";
			stmt.execute(SQL);
			System.out.println("Created Tables (ROLL PK) , Starting Load");	
		}
		catch(Exception E) {
			System.out.println("Error while Creating Table");
		}
	}
	
	
	
	
	@SuppressWarnings("static-access")
	void loadTable() {
		try {
			ExecutorService asd = Executors.newFixedThreadPool(42);
			int i = 0;
			while (i < 40) {
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
		}
		catch(Exception E) {
			System.out.println("Error while loading Data");
		}
	}
	
	class InsertLoad implements Runnable{
		public void run() {
			try {
				System.out.println("Staring Insert Thread -->" + Thread.currentThread().getName());
				Connection oraCon = DBConnection.oraPostConn();
				PreparedStatement pstmt = oraCon.prepareStatement("insert into RandomLoad (roll, name, mark1,mark2,mark3) values (?,?,?,?,?)");
				int i = 0;
				while (i < 1250000) {
					pstmt.setInt(1 , oraSequence.nextVal());
					pstmt.setString(2, OraRandom.randomString(20));
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
	
	void singleRowsMultiple(int a ) {
		try {
			ExecutorService asd = Executors.newFixedThreadPool(a+2);
			int i = 0;
			while (i < a) {
				asd.submit(new SingleStmt());
				i++;
			}
			i = 0 ;
			System.out.println("Loading Data... Sleepin for 10 seconds");
			asd.shutdown();
			while(!asd.isShutdown()) {
				Thread.currentThread().sleep(1000);
			}
			asd.shutdownNow();
		}
		catch(Exception E) {
			System.out.println("Error while loading Data");
		}
	}
	class SingleStmt implements Runnable{
		public void run() {

			try {
				System.out.println("Staring Select  Thread -->" + Thread.currentThread().getName());
				Connection oraCon = DBConnection.oraPostConn();
				String SQL = "select max(roll) from randomload";
				Statement stmt = oraCon.createStatement();
				ResultSet rs = stmt.executeQuery(SQL);
				int maxvalue = 0;
				while (rs.next()) {
					maxvalue = rs.getInt(1);
				}
				int randomValue = OraRandom.randomUniformInt(maxvalue); 
				int i = 0;
				while (i < 100) {
					PreparedStatement pstmt = oraCon.prepareStatement("select mark1 from  RandomLoad where roll =?");
					pstmt.setInt(1, randomValue);
					 rs = pstmt.executeQuery();
					while(rs.next()) {
						rs.getInt(1);
					}
					i++;
					pstmt.close();
				}
				
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
		
	}
	
	
	void singleRows1UPD(int a ) {
		try {
			ExecutorService asd = Executors.newFixedThreadPool(a+2);
			int i = 0;
			while (i < a) {
				asd.submit(new SingleStmt1Upd());
				i++;
			}
			Thread.currentThread().sleep(1000);
			asd.submit(new SingleStmt1Upd2());
			i = 0 ;
			System.out.println("Loading Data... Sleepin for 10 seconds");
			asd.shutdown();
			while(!asd.isShutdown()) {
				Thread.currentThread().sleep(1000);
			}
			asd.shutdownNow();
		}
		catch(Exception E) {
			System.out.println("Error while loading Data");
		}
	}
	static volatile int SingleStmt1Updval;
	class SingleStmt1Upd implements Runnable{
		public void run() {

			try {
				System.out.println("Staring Select  Thread -->" + Thread.currentThread().getName());
				Connection oraCon = DBConnection.oraPostConn();
				String SQL = "select max(roll) from randomload";
				Statement stmt = oraCon.createStatement();
				ResultSet rs = stmt.executeQuery(SQL);
				int maxvalue = 0;
				while (rs.next()) {
					maxvalue = rs.getInt(1);
				}
				int randomValue = OraRandom.randomUniformInt(maxvalue); 
				SingleStmt1Updval = randomValue;
				int i = 0;
				while (i < 1000000) {
					PreparedStatement pstmt = oraCon.prepareStatement("select mark1 from  RandomLoad where roll =?");
					pstmt.setInt(1, randomValue);
					 rs = pstmt.executeQuery();
					while(rs.next()) {
						rs.getInt(1);
					}
					i++;
					pstmt.close();
				}
				
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
		
	}
	
	class SingleStmt1Upd2 implements Runnable{
		public void run() {

			try {
				System.out.println("Staring Upload  Thread -->" + Thread.currentThread().getName());
				Connection oraCon = DBConnection.oraPostConn();
				
				int randomValue = SingleStmt1Updval;
				int i = 0;
				while (i < 1000000) {
					PreparedStatement pstmt = oraCon.prepareStatement("update randomload set mark1= ? where roll = " + randomValue);
					pstmt.setInt(1, OraRandom.randomSkewInt(100));
					pstmt.executeUpdate();
					pstmt.close();
				}
				
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
		
	}
	
	
	
	
	
	
	
}
