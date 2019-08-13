import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

public class BufferCache {
	void checkWorkingSet() {
		String lruChains= "select NXT_REPL,NXT_REPLAX from x$kcbwds where CNUM_SET>0";
		String BufferCache="select prv_repl,nxt_repl,tch,decode(state,0,'free',1,'xcur',2,'scur',3,'cr', 4,'read',5,'mrec',6,'irec',7,'write',8,'pi', 9,'memory',10,'mwrite',11,'donated', 12,'protected',  13,'securefile', 14,'siop',15,'recckpt', 16, 'flashfree',  17, 'flashcur', 18, 'flashna') STATE from x$bh";

		try {
			Connection oraCon = DBConnection.getOraSysCon();
			Statement stmt = oraCon.createStatement();
			ResultSet rs = stmt.executeQuery(BufferCache);
			Statement stmt2 = oraCon.createStatement();
			ResultSet rs2 = stmt2.executeQuery(lruChains);
			HashMap<String,String> mainlruqueue ;
			HashMap<String,String> auxlruqueue;
			HashMap<String,String> wholeQueue = new HashMap<>();
			HashMap<String,Integer> tchcounts = new HashMap<>();
			HashMap<String,String> bufstates = new HashMap<>();
			HashMap<String,Integer> mainstates = new HashMap<>();
			HashMap<String,Integer> auxstates = new HashMap<>();
			int i = 0;
			while (rs.next()) {
				wholeQueue.put(rs.getString(1),rs.getString(2));
				bufstates.put(rs.getString(1), rs.getString(4));
				tchcounts.put(rs.getString(1), rs.getInt(3));
				i++;
			}
			HashMap<String,Integer> totalbufagg= new HashMap<>();
			for (String temp : bufstates.keySet()) {
				int temp1 = totalbufagg.get(bufstates.get(temp))==null?0:totalbufagg.get(bufstates.get(temp));
				totalbufagg.put(bufstates.get(temp),1+temp1);
			}
			System.out.println("Total Buffers in Buffer cache --> " + i);
			System.out.println("Total Buffer States in Buffer cache:");
			for (String temp : totalbufagg.keySet() ) {
				System.out.println(temp + " --> " + totalbufagg.get(temp));
			}
			while (rs2.next()) {
				mainlruqueue = new HashMap<>();
				auxlruqueue = new HashMap<>();
				String mainptr = rs2.getString(1);
				String auxptr = rs2.getString(2);
				while (mainptr!=wholeQueue.get(mainptr)) {
					mainlruqueue.put(mainptr, wholeQueue.get(mainptr));
					mainptr = wholeQueue.get(mainptr);
				}
				while (auxptr!=wholeQueue.get(auxptr)) {
					auxlruqueue.put(auxptr, wholeQueue.get(auxptr));
					auxptr = wholeQueue.get(auxptr);
				}
				System.out.println("Total Buffers in Main LRU --> " + mainlruqueue.keySet().size());
				
				for (String temp : mainlruqueue.keySet()) {
					int temp1 = mainstates.get(bufstates.get(temp))==null?0:mainstates.get(bufstates.get(temp));
					mainstates.put(bufstates.get(temp),1+temp1);
				}
				for (String temp : mainstates.keySet() ) {
					System.out.println(temp + " --> " + mainstates.get(temp));
				}
				System.out.println("Total Buffers in AUX LRU --> " + auxlruqueue.keySet().size());
				for (String temp : auxlruqueue.keySet()) {
					int temp1 = auxstates.get(bufstates.get(temp))==null?0:auxstates.get(bufstates.get(temp));
					auxstates.put(bufstates.get(temp),1+temp1);
				}
				for (String temp : auxstates.keySet() ) {
					System.out.println(temp + " --> " + auxstates.get(temp));
				}
				
			}
			
			
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
}
