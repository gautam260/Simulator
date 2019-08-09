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
<<<<<<< HEAD
				PreparedStatement pstmt = oraCon.prepareStatement("select sum(t4) from temp2 where t6=?");
=======
				PreparedStatement pstmt = oraCon.prepareStatement("select avg(totalmarks) from temp where roll < ?");
>>>>>>> branch 'master' of https://github.com/vishnusivathej/Simulator.git
				int i = 0 ;
<<<<<<< HEAD
				while (i< 100000) {
					pstmt.setInt(1, OraRandom.randomSkewInt(1000));
					
=======
				while (i< 1000000) {
					pstmt.setInt(1, OraRandom.randomSkewInt(6000));
>>>>>>> branch 'master' of https://github.com/vishnusivathej/Simulator.git
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
