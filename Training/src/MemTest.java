import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MemTest {
	void Start() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			String SQL;
			if (CheckTable.exists("MEM_TABLE")) {
				ExecutorService asd = Executors.newFixedThreadPool(10);
				asd.submit(new Loader());
			}
			else {
				SQL = "Create table MEM_TABLE(roll number primary key, name varchar2(20), address varchar2(200), location varchar2(200))";
				stmt.execute(SQL);
				SQL = "insert into MEM_TABLE(roll,name,address,location) values (?,?,?,?)";
				
				PreparedStatement pstmt = oraCon.prepareStatement(SQL);
				int i = 0;
				while (i < 2000000) {
					pstmt.setInt(1, oraSequence.nextVal());
					pstmt.setString(2, OraRandom.randomString(20));
					pstmt.setString(3, OraRandom.randomString(200));
					pstmt.setString(4, OraRandom.randomString(200));
					pstmt.addBatch();
					if (i%10000 == 0 ) {
						System.out.println("Loaded -> "+i + " rows");
						pstmt.executeBatch();
					}
					i++;
				}
				pstmt.executeBatch();
				pstmt.close();
				stmt.close();
				oraCon.close();
				ExecutorService asd = Executors.newFixedThreadPool(10);
				asd.submit(new Loader());
				asd.shutdown();
				asd.shutdownNow();
			}
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	class Loader implements Runnable{
		public void run() {
			try {
				System.out.println("Starting Load");
				long start_time = System.nanoTime();
				Connection oraCon = DBConnection.getOraConn();
				Statement stmt = oraCon.createStatement();
				String SQL = "select max(roll) from MEM_TABLE";
				ResultSet rs = stmt.executeQuery(SQL);
				int max = 0;
				while (rs.next()) {
					max = rs.getInt(1);
				}
				SQL = "select length(max(name)) from MEM_TABLE where roll > ? and roll < ?";
				PreparedStatement pstmt = oraCon.prepareStatement(SQL);
				int i = 0;
				while (i < 50000) {
					int temp = OraRandom.randomUniformInt(max);
					pstmt.setInt(1, temp);
					pstmt.setInt(2, temp + 1000);
					rs = pstmt.executeQuery();
					while(rs.next()) {
						rs.getInt(1);
					}
					i++;
				}
				System.out.println("Done Load");
				long end_time = System.nanoTime();
				System.out.println("Workload Took " + (end_time - start_time)/ 1e6);
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
}
