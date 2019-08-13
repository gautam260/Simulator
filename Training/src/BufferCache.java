import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

public class BufferCache {
	void checkWorkingSet() {
		String MainChain= "select NXT_REPL from x$kcbwds where CNUM_SET>0";
		String AuxChain= "select NXT_REPLAX from x$kcbwds where CNUM_SET>0";
		String FindAddrs="select prv_repl,nxt_repl from x$bh";
		String tchCount = "select prv_repl,tch from x$bh";
		try {
			Connection oraCon = DBConnection.getOraSysCon();
			Statement stmt = oraCon.createStatement();
			ResultSet rs = stmt.executeQuery(MainChain);
			Statement pstmt = oraCon.createStatement();
			ResultSet rs2 = pstmt.executeQuery(FindAddrs);
			int mainQueue = 0;
			int auxQueue = 0;
			HashMap<String,String> asd = new HashMap<>();
			
			while (rs2.next()) {
				asd.put(rs2.getString(1), rs2.getString(2));
			}
			HashMap<String,Integer> tchcounts = new HashMap<>();
			rs2 = pstmt.executeQuery(tchCount);
			while (rs2.next()) {
				tchcounts.put(rs2.getString(1), rs2.getInt(2));
			}
			
			int mainQueuetch = 0;
			int auxQueuetch = 0;
			
			
			while (rs.next()) {
				String ChainStart = rs.getString(1);
				int i = 0;
				while (asd.get(ChainStart)!=ChainStart) {
					ChainStart = asd.get(ChainStart);
					mainQueuetch = mainQueuetch + (tchcounts.get(ChainStart)==null?0:tchcounts.get(ChainStart));
					i++;
				}
				mainQueue  = i;
				System.out.println(rs.getString(1) + " --> Main Queue blocks --> "  + i +" --> TCH Count => " + mainQueuetch);
			}
			rs.close();
			rs = stmt.executeQuery(AuxChain);
			while (rs.next()) {
				String ChainStart = rs.getString(1);
				
				int i = 0;
				while (asd.get(ChainStart)!=ChainStart) {
					ChainStart = asd.get(ChainStart);
					auxQueuetch = auxQueuetch + (tchcounts.get(ChainStart)==null?0:tchcounts.get(ChainStart));
					i++;
				}
				auxQueue  = i;
				System.out.println(rs.getString(1) + " --> Aux Queue blocks --> "  + i+" --> TCH Count => " + auxQueuetch);
			}
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
}
