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
		while (i < 10) {
			asd.submit(new Load());
			i++;
		}
	}
	
	
	class Load implements Runnable{
		public void run() {
			try {
				
				Connection oraCon = DBConnection.getOraConn();
				PreparedStatement pstmt = oraCon.prepareStatement("select sum(mark1) from students where mark2=?");
				pstmt.setFetchSize(500);
				ResultSet rs;
				int i = 0;
				while (i < 100000) {
					pstmt.setInt(1, OraRandom.randomUniformInt(1600));
						rs = pstmt.executeQuery();
						while(rs.next()) {
							
						}
						i++;
				}
				System.out.println("Complete");
	
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
	
	
	
}
