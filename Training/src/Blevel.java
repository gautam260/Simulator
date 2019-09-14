import java.sql.Statement;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class Blevel {
	
	
	void start() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			String SQL;
			try {
				SQL = "drop table blevel";
				stmt.execute(SQL);
			}
			catch(Exception E) {
			}
			try {
				SQL = "create table blevel (roll number)";
				stmt.execute(SQL);
				SQL = "create index blevel_idx on blevel(roll)";
				stmt.execute(SQL);
			}
			catch(Exception E) {
			}
			SQL = "insert into blevel (roll) values (?)";
			PreparedStatement pstmt = oraCon.prepareStatement(SQL);
			Long i = (long) 0 ;
			System.out.println("Inserting Data");
			while (i < Long.MAX_VALUE) {
				pstmt.setLong(1, i);
				pstmt.addBatch();
				if (i%100000 == 0 ) {
					pstmt.executeBatch();
					if (i%1000000 == 0 )
						System.out.println("Inserted " + i + " rows" );
				}
				i++;
			}
			pstmt.executeBatch();
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
}
