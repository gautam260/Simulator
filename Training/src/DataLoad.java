import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataLoad {
	
	void run() {
		ExecutorService asd = Executors.newFixedThreadPool(10);
		for (int i = 0 ; i < 10; i++)
			asd.submit(new Loader());
		asd.shutdown();
	}
	class Loader implements Runnable {
		public void run() {
			try {
				Connection oraCon = DBConnection.getOraConn();
				if (oraCon!=null) {
					System.out.println(Thread.currentThread().getName() + " ----> Starting");
					PreparedStatement pstmt = oraCon.prepareStatement("insert into temp(roll,dept_id,year,sem_id,subject_id,mark1,mark2,mark3,finalmark) values (?,?,?,?,?,?,?,?,?)");
					int i = 1;
					while (i < 10000000) {
						pstmt.setInt(1,oraSequence.nextVal() );
						pstmt.setInt(2, OraRandom.randomSkewInt(8));
						pstmt.setInt(3, OraRandom.randomSkewInt(4));
						pstmt.setInt(4, OraRandom.randomSkewInt(8));
						pstmt.setInt(5, OraRandom.randomSkewInt(48));
						pstmt.setInt(6, OraRandom.randomSkewInt(100));
						pstmt.setInt(7, OraRandom.randomSkewInt(100));
						pstmt.setInt(8, OraRandom.randomSkewInt(100));
						pstmt.setInt(9, OraRandom.randomSkewInt(100));
						pstmt.addBatch();
						if (i%10000 == 0) {
							pstmt.executeBatch();
							System.out.println(Thread.currentThread().getName() + " ----  inserted ---- " + i + " rows");
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
