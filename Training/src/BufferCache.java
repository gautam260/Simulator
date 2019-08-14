import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BufferCache {
	
	
	
	void bufferCBCtest() {
		
	}
	
	void bufferPinTest() {
		ExecutorService asd = Executors.newFixedThreadPool(10);
		int i = 0;
		while (i < 20) {
			asd.submit(new BufferPinTest());
		}
	}
	class BufferPinTest implements Runnable{
		public void run() {
			try {
				Connection oraCon = DBConnection.getOraConn();
				Statement stmt = oraCon.createStatement();
				String SQL = "select * from temp where rowid='AAASLLAAHAAAAGvACM'";
				while (true) {
					ResultSet rs = stmt.executeQuery(SQL);
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
	
	void checkWorkingSet() {


		String lruChains= "select NXT_REPL,NXT_REPLAX,NXT_WRITE,NXT_WRITEAX,cold_hd from x$kcbwds where CNUM_SET>0";
		String BufferCache="select prv_repl,nxt_repl,tch,decode(state,0,'free',1,'xcur',2,'scur',3,'cr', 4,'read',5,'mrec',6,'irec',7,'write',8,'pi', 9,'memory',10,'mwrite',11,'donated', 12,'protected',  13,'securefile', 14,'siop',15,'recckpt', 16, 'flashfree',  17, 'flashcur', 18, 'flashna') STATE from x$bh";

		try {
			Connection oraCon = DBConnection.getOraSysCon();
			Statement stmt = oraCon.createStatement();
			ResultSet rs = stmt.executeQuery(BufferCache);
			rs.setFetchSize(100000);
			Statement stmt2 = oraCon.createStatement();
			ResultSet rs2 = stmt2.executeQuery(lruChains);
			rs2.setFetchSize(100000);
			String MidPoint ;
			HashMap<String,String> mainlruqueue ;
			HashMap<String,String> auxlruqueue;
			HashMap<String,String> wmainlruqueue ;
			HashMap<String,String> wauxlruqueue;
			int MidPoint_position = 0;
			HashMap<String,String> wholeQueue = new HashMap<>();
			HashMap<String,Integer> tchcounts = new HashMap<>();
			HashMap<String,String> bufstates = new HashMap<>();
			HashMap<String,Integer> mainstates = new HashMap<>();
			HashMap<String,Integer> auxstates = new HashMap<>();
			HashMap<String,Integer> wmainstates = new HashMap<>();
			HashMap<String,Integer> wauxstates = new HashMap<>();
			int i = 0;
			while (rs.next()) {
				wholeQueue.put(rs.getString(1),rs.getString(2));
				bufstates.put(rs.getString(1), rs.getString(4));
				tchcounts.put(rs.getString(1), rs.getInt(3));
				i++;
			}
			System.out.println("Got Details");
			HashMap<String,Integer> totalbufagg= new HashMap<>();
			for (String temp : bufstates.keySet()) {
				int temp1 = totalbufagg.get(bufstates.get(temp))==null?0:totalbufagg.get(bufstates.get(temp));
				totalbufagg.put(bufstates.get(temp),1+temp1);
			}
			System.out.println("Total Buffers in Buffer cache --> " + i);
			System.out.print ("Total Buffer States in Buffer cache:"+ " States ");
			for (String temp : totalbufagg.keySet() ) {
				System.out.print("   " +temp + " --> " + totalbufagg.get(temp));
			}
			System.out.println("");
			while (rs2.next()) {
				mainlruqueue = new HashMap<>();
				auxlruqueue = new HashMap<>();
				wmainlruqueue = new HashMap<>();
				wauxlruqueue = new HashMap<>();
				String mainptr = rs2.getString(1);
				String auxptr = rs2.getString(2);
				String wmainptr = rs2.getString(3);
				String wauxptr = rs2.getString(4);
				MidPoint = rs2.getString(5);
				//System.out.print(MidPoint);
				while (mainptr!=wholeQueue.get(mainptr)) {
					if (MidPoint.equals(mainptr)) {
						MidPoint_position = mainlruqueue.size();
					}
					mainlruqueue.put(mainptr, wholeQueue.get(mainptr));
					mainptr = wholeQueue.get(mainptr);
					
				}
				while (auxptr!=wholeQueue.get(auxptr)) {
					auxlruqueue.put(auxptr, wholeQueue.get(auxptr));
					auxptr = wholeQueue.get(auxptr);
				}
				
				while (wmainptr!=wholeQueue.get(wmainptr)) {
					wmainlruqueue.put(wmainptr, wholeQueue.get(wmainptr));
					wmainptr = wholeQueue.get(wmainptr);
				}
				while (wauxptr!=wholeQueue.get(wauxptr)) {
					wauxlruqueue.put(wauxptr, wholeQueue.get(wauxptr));
					wauxptr = wholeQueue.get(wauxptr);
				}
				
				System.out.print("Total Buffers in LRU --> " + mainlruqueue.keySet().size() + " Midpoint Position --> " + MidPoint_position + " States ");
				for (String temp : mainlruqueue.keySet()) {
					int temp1 = mainstates.get(bufstates.get(temp))==null?0:mainstates.get(bufstates.get(temp));
					mainstates.put(bufstates.get(temp),1+temp1);
				}
				for (String temp : mainstates.keySet() ) {
					System.out.print("  " +temp + " --> " + mainstates.get(temp));
				}
				System.out.println("");
				System.out.print("Total Buffers in AUX LRU --> " + auxlruqueue.keySet().size()+ " States ");
				for (String temp : auxlruqueue.keySet()) {
					int temp1 = auxstates.get(bufstates.get(temp))==null?0:auxstates.get(bufstates.get(temp));
					auxstates.put(bufstates.get(temp),1+temp1);
				}
				for (String temp : auxstates.keySet() ) {
					System.out.print("  " +temp + " --> " + auxstates.get(temp));
				}
				
				
				
				System.out.println("");
				System.out.print("Total Buffers in WLRU --> " + wmainlruqueue.keySet().size() + " States ");
				for (String temp : wmainlruqueue.keySet()) {
					int temp1 = wmainstates.get(bufstates.get(temp))==null?0:wmainstates.get(bufstates.get(temp));
					wmainstates.put(bufstates.get(temp),1+temp1);
				}
				for (String temp : wmainstates.keySet() ) {
					System.out.print("  " +temp + " --> " + wmainstates.get(temp));
				}
				System.out.println("");
				System.out.print("Total Buffers in WAUX LRU --> " + wauxlruqueue.keySet().size()+ " States ");
				for (String temp : wauxlruqueue.keySet()) {
					int temp1 = wauxstates.get(bufstates.get(temp))==null?0:wauxstates.get(bufstates.get(temp));
					wauxstates.put(bufstates.get(temp),1+temp1);
				}
				for (String temp : wauxstates.keySet() ) {
					System.out.print("  " +temp + " --> " + wauxstates.get(temp));
				}
				
			}
			rs.close();
			rs2.close();
			stmt.close();
			stmt2.close();
			oraCon.close();
			
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	
	
	}
}
