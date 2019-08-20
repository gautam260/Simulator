import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RandomLoad {
	void createTable() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			try {
				String SQL = "drop table RandomLoad";
				stmt.execute(SQL);
			}
			catch(Exception E) {
				
			}
			String SQL = " create table RandomLoad (roll number, name varchar2(20), mark1 number, mark2 number, mark3 number) ";
			stmt.execute(SQL);
			//SQL = "create index RandomLoad_idx on RandomLoad(roll) ";
			//stmt.execute(SQL);
			System.out.println("Created Tables and indexes, Starting Load");
			ExecutorService asd = Executors.newFixedThreadPool(400);
			int i = 0;
			while (i < 30) {
				asd.submit(new InsertLoad());
				i++;
			}
			i = 0 ;
			/*System.out.println("Loading Data... Sleepin for 10 seconds");
			Thread.currentThread().sleep(10000);
			while (i < 5) {
				asd.submit(new DeleteLoad());
				asd.submit(new UpdateLoad());
				i++;
			}
			while (i < 10) {
				asd.submit(new SelectLoad());
				i++;
			}
			*/
			
			
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	void PageTable() {
		
	}
	class InsertLoad implements Runnable{
		public void run() {
			try {
				System.out.println("Staring Insert Thread -->" + Thread.currentThread().getName());
				Connection oraCon = DBConnection.getOraConn();
				PreparedStatement pstmt = oraCon.prepareStatement("insert into RandomLoad (roll, name,mark1,mark2,mark3) values (?,?,?,?,?)");
				int i = 0;
				while (i < 10000000) {
					pstmt.setInt(1 , oraSequence.nextVal());
					pstmt.setString(2, OraRandom.randomString(20));
					pstmt.setInt(3, OraRandom.randomUniformInt(100));
					pstmt.setInt(4, OraRandom.randomUniformInt(100));
					pstmt.setInt(5, OraRandom.randomUniformInt(100));
					
					pstmt.addBatch();
					if (i%100000 == 0) {
						pstmt.executeBatch();
						System.out.println("loaded " + oraSequence.getval());
					}
					i++;
				}
				pstmt.close();
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
		
	}
	class UpdateLoad implements Runnable{
		public void run() {
			try {
				System.out.println("Staring Update Thread -->" + Thread.currentThread().getName());
				Connection oraCon = DBConnection.getOraConn();
				PreparedStatement pstmt = oraCon.prepareStatement("Update RandomLoad set name = ? where roll = ? ");
				int i = 0;
				while (i < 1000000) {
					pstmt.setInt(2 , OraRandom.randomUniformInt(oraSequence.getval()));
					pstmt.setString(1, OraRandom.randomString(20));
					pstmt.executeUpdate();
					i++;
				}
				pstmt.close();
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
	class SelectLoad implements Runnable{
		public void run() {
			try {
				System.out.println("Staring Select  Thread -->" + Thread.currentThread().getName());
				Connection oraCon = DBConnection.getOraConn();
				PreparedStatement pstmt = oraCon.prepareStatement("select mark2 from  RandomLoad where roll =?");
				int i = 0;
				while (i < 1000000) {
					pstmt.setInt(1, OraRandom.randomUniformInt(oraSequence.getval()));
					ResultSet rs = pstmt.executeQuery();
					while(rs.next()) {
						rs.getInt(1);
					}
					i++;
				}
				pstmt.close();
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
	
	
	
	
	
	class DeleteLoad implements Runnable{
		public void run() {
			try {
				System.out.println("Staring Delete Thread -->" + Thread.currentThread().getName());
				Connection oraCon = DBConnection.getOraConn();
				PreparedStatement pstmt = oraCon.prepareStatement("delete from HugePages where roll = ? ");
				int i = 0;
				while (i < 1000000) {
					pstmt.setInt(1 , OraRandom.randomUniformInt(oraSequence.getval()));
					pstmt.executeUpdate();
					i++;
				}
				pstmt.close();
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
		
	}
}
