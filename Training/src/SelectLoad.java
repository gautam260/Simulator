import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SelectLoad {
	

	void selectLoad() {
		ExecutorService asd = Executors.newFixedThreadPool(50);
		int i = 0;
		while (i < 50) {
			asd.submit(new Load());
			i++;
		}
	}
	
	
	class Load implements Runnable{
		public void run() {
			try {
				
				Connection oraCon = DBConnection.getOraConn();
				PreparedStatement pstmt = oraCon.prepareStatement("select ora.nextval from  RandomLoad");
				ResultSet rs;
				int i = 0;
				while (i < 1000000) {
						rs = pstmt.executeQuery();
						while(rs.next()) {
							System.out.println(Thread.currentThread().getName() + " ---- " + rs.getInt(1));
						}
						i++;
				}
	
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
	
	
	
}
