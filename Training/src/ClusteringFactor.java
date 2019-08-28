import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ClusteringFactor {
	
	
	
	void insertSession(String mode, int a) {
		try {
			try {
				Connection con = DBConnection.getOraConn();
				Statement stmt = con.createStatement();
				String SQL = "drop table randomload";
				try{
					stmt.execute(SQL);
				}
				catch(Exception E) {
					
				}
				SQL = "create table randomload(roll number, name varchar2(20), mark1 number, mark2 number, mark3 number)";
				stmt.execute(SQL);
				SQL = "create index idx on randomload(roll)";
				stmt.execute(SQL);
				stmt.close();
				con.close();
			}
			catch(Exception E) {
				E.printStackTrace();
			}
			
			
			if (mode.equals("SINGLE")) {
				ExecutorService asd = Executors.newCachedThreadPool();
				for (int i = 0 ; i < a ; i++ ) {
					asd.submit(new InsertNormal(10000000/a));
				}
				asd.shutdown();
			}
			else if (mode.equals("BATCH")) {
				ExecutorService asd = Executors.newCachedThreadPool();
				for (int i = 0 ; i < a ; i++ ) {
					asd.submit(new InsertBatch(10000000/a));
				}
				asd.shutdown();
			}
			
		}
		catch(Exception E){
			E.printStackTrace();
		}
	}
	class InsertNormal implements Runnable{
		private int numRows;
		InsertNormal(int a){
			this.numRows = a;
		}
		public void run() {
			try {
				System.out.println(Thread.currentThread().getName() + " ---> Starting Loading" );
				Connection con = DBConnection.getOraConn();
				PreparedStatement pstmt = con.prepareStatement("insert into randomload(roll,name,mark1, mark2, mark3) values (?,?,?,?,?)");
				int i = 0;
				while (i < numRows) {
					pstmt.setInt(1, oraSequence.getval());
					pstmt.setString(2, OraRandom.randomString(20));
					pstmt.setInt(3, OraRandom.randomSkewInt(100));
					pstmt.setInt(4, OraRandom.randomSkewInt(100));
					pstmt.setInt(5, OraRandom.randomSkewInt(100));
					pstmt.execute();
					i++;
				}
				pstmt.close();
				con.close();
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
	class InsertBatch implements Runnable{
		private int numRows;
		InsertBatch(int a){
			this.numRows = a;
		}
		public void run() {
			try {
				System.out.println(Thread.currentThread().getName() + " ---> Starting Loading" );
				Connection con = DBConnection.getOraConn();
				PreparedStatement pstmt = con.prepareStatement("insert into randomload(roll,name,mark1, mark2, mark3) values (?,?,?,?,?)");
				int i = 0;
				while (i < numRows) {
					pstmt.setInt(1, oraSequence.getval());
					pstmt.setString(2, OraRandom.randomString(20));
					pstmt.setInt(3, OraRandom.randomSkewInt(100));
					pstmt.setInt(4, OraRandom.randomSkewInt(100));
					pstmt.setInt(5, OraRandom.randomSkewInt(100));
					pstmt.addBatch();
					if (i%10000 == 0) {
						pstmt.executeBatch();
					}
				}
				pstmt.executeBatch();
				pstmt.close();
				con.close();
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
}
