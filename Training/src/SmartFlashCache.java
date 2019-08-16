import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SmartFlashCache {
	void run() {
		
		try {
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			System.out.println("Dropping any Existing Table");
			String SQL = "drop table smartcache";
			try {
				stmt.execute(SQL);
			}
			catch(Exception E) {
				
			}
			System.out.println("Creating Table smartcache");
			SQL = "create table smartcache (roll number, name varchar2(200), address varchar2(2000), details varchar2(2000))";
			stmt.execute(SQL);
			System.out.println("checking buffers");
			SQL = "select sum(buffers)*4   from v$buffeR_pool";
			ResultSet rs = stmt.executeQuery(SQL);
			int count = 0 ;
			while (rs.next()) {
				count = rs.getInt(1);
			}
			System.out.println("Loading Data");
			SQL = "insert into smartcache(roll, name,address,details) values (?,?,?,?)";
			PreparedStatement pstmt = oraCon.prepareStatement(SQL);
			int i = 0;
			while (i < count) {
				pstmt.setInt(1, oraSequence.nextVal());
				pstmt.setString(2, OraRandom.randomString(200));
				pstmt.setString(3, OraRandom.randomString(2000));
				pstmt.setString(4, OraRandom.randomString(2000));
				pstmt.addBatch();
				if (i%10000 == 0 ) {
					System.out.println("loaded -->" + oraSequence.getval() + " Rows" );
					pstmt.executeBatch();
				}
				i++;
			}
			pstmt.executeBatch();
			System.out.println("Data Load Complete... Creating Index");
			SQL = "create index smartcache_idx on smartcache(roll)";
			stmt.execute(SQL);
			System.out.println("Index Creation Complete.. Starting load for first 1000000 queries");
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Date date = new Date(); 
			System.out.println("Load Start Time " + dateFormat.format(date)); //2016/11/16 12:08:43*/
			ExecutorService asd = Executors.newFixedThreadPool(10);
			 i = 0;
			while (i < 10) {
				asd.submit(new Loader());
				i++;
			}
			asd.shutdown();
		}
		catch(Exception E) {
			E.printStackTrace();
		}
		
	}
	class Loader implements Runnable{
		public void run() {
			try {
				String SQL = "select substr(details,0,10) from smartcache where roll between ? and ? ";
				Connection oraCon = DBConnection.getOraConn();
				PreparedStatement pstmt = oraCon.prepareStatement(SQL);
				int i = 0 ;
				while (i < 100000) {
					int k = OraRandom.randomUniformInt(275232);
					pstmt.setInt(1, k);
					pstmt.setInt(2, k + 100);
					ResultSet rs = pstmt.executeQuery();
					while (rs.next()) {
						rs.getString(1);
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
