import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LibraryCacheLock {
	
	
	
	void HardParse() throws InterruptedException {
		ExecutorService asd = Executors.newFixedThreadPool(20);
		int i = 0;
		while (i < 20) {
			asd.submit(new HardParse());
			i++;
		}
	}
	
	
	class HardParse implements Runnable{
		public void run() {
			try {
				Connection oracon = DBConnection.getOraConn();
				Statement stmt = oracon.createStatement();
				String SQL = "select max(roll) from randomload";
				ResultSet rs = stmt.executeQuery(SQL);
				int maxvalue = 0;
				while (rs.next()) {
					maxvalue = rs.getInt(1);
				}
				SQL = "select mark1 from randomload where roll = ";
				while (true) {
					String sql;
					sql = SQL;
					sql = sql + OraRandom.randomUniformInt(maxvalue);
					rs = stmt.executeQuery(sql);
					while (rs.next()) {
						rs.getInt(1);
					}
				}
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
}
