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
				PreparedStatement pstmt = oraCon.prepareStatement("select avg(totalmarks) from temp where roll < ?");
				int i = 0 ;
				while (i< 1000000) {
					pstmt.setInt(1, OraRandom.randomSkewInt(6000));
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
