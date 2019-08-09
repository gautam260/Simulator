import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataLoad {
	
	void run() {
		ExecutorService asd = Executors.newFixedThreadPool(30);
		for (int i = 0 ; i < 30; i++)
			asd.submit(new Loader());
		asd.shutdown();
	}
	class Loader implements Runnable {
		public void run() {
			try {
				Connection oraCon = DBConnection.getOraConn();
				if (oraCon!=null) {
					System.out.println(Thread.currentThread().getName() + " ----> Starting");
					PreparedStatement pstmt = oraCon.prepareStatement("insert into temp(t1,t2,t3,t4,t5,t6,t7) values (?,?,?,?,?,?,?)");
					int i = 1;
					while (i < 10000000) {
						pstmt.setInt(1,oraSequence.nextVal() );
						pstmt.setInt(2, OraRandom.randomSkewInt(100));
						pstmt.setInt(3, OraRandom.randomSkewInt(200));
						pstmt.setInt(4, OraRandom.randomSkewInt(400));
						pstmt.setInt(5, OraRandom.randomSkewInt(600));
						pstmt.setInt(6, OraRandom.randomSkewInt(1000));
						pstmt.setString(7, OraRandom.randomString(200));
						
						pstmt.addBatch();
						if (i%10000 == 0) {
							pstmt.executeBatch();
							System.out.println("Total inserted ---- " + oraSequence.getval() + " rows");
						}
						i++;
					}
					pstmt.executeBatch();
					pstmt.close();
					oraCon.close();
					System.out.println(Thread.currentThread().getName() + " ----> Complete");
				}
				else {
					System.out.println("Unable to get Connection --> " + Thread.currentThread().getName());
				}
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
		
	}

}
