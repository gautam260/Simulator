import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class HugePages {
	void createTable() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			String SQL = " create table cached (roll number, details varchar2(6000)) storage (buffer_pool keep)";
			Statement stmt = oraCon.createStatement();
			stmt.execute(SQL);
			PreparedStatement pstmt = oraCon.prepareStatement("Insert into cached (roll,details) values (?,?)");
			int i = 0 ;
			System.out.println("loading data");
			while (i < 8388608) {
				pstmt.setInt(1, oraSequence.nextVal());
				pstmt.setString(2, OraRandom.randomString(6000));
				pstmt.addBatch();
				if (i %100000 == 0)
					pstmt.executeBatch();
				i++;
			}
			pstmt.executeBatch();
			System.out.println("loaded data");
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
}
