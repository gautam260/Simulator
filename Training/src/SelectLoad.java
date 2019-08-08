import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SelectLoad {
	public void run() {
		ExecutorService ad = Executors.newFixedThreadPool(10);
		for (int i = 0 ; i < 10 ; i ++){
			ad.submit(new Loader());
		}
		ad.shutdown();
	}
	class Loader implements Runnable {
		public void run() {
			try {
				System.out.println("Select Load --> " +Thread.currentThread().getName() + " --- Started");
				Connection oraCon = DBConnection.getOraConn();
				PreparedStatement pstmt = oraCon.prepareStatement("select avg(mark3) from temp where sem_id=?  and finalmark=?");
				int i = 0 ;
				while (i< 1000) {
					pstmt.setInt(1, OraRandom.randomSkewInt(8));
					pstmt.setInt(3, OraRandom.randomSkewInt(100));
					ResultSet rs = pstmt.executeQuery();
					while(rs.next()) {
						rs.getInt(1);
					}
					i++;
				}
				System.out.println("Select Load --> " +Thread.currentThread().getName() + " --- Finished");
			}
			catch(Exception E) {
				E.printStackTrace();
			}
			
		}
	}

}
