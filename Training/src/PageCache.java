import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PageCache {
	
	void run() {
		ExecutorService asd = Executors.newFixedThreadPool(100);
		int i = 0;
		while (i < 100) {
			asd.submit(new Loader());
			i++;
		}
	}
	
	
	
	class Loader implements Runnable{
		public void run() {
			try {
				Connection oracon = DBConnection.getOraConn();
				String sql = "select 1 from dual";
				Statement stmt = oracon.createStatement();
				ResultSet rs; 
				while (true) {
					rs = stmt.executeQuery(sql);
					while (rs.next()) {
						rs.next();
					}
				}
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
}
