import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class CBC {

	
	
	
	void createTable() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			try {
				String SQL = "drop table RandomLoad";
				stmt.execute(SQL);
			}
			catch(Exception E) {
				System.out.println("Drop table failed or table doesnt exist");
			}
			String SQL = "  create table randomload (roll number primary key, name varchar2(20),  mark1 number not null, mark2 number, mark3 number not null) storage (initial 64m) ";
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
				Connection oraCon = DBConnection.getOraConn();
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
}
