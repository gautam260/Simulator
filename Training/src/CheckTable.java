import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class CheckTable {
	static boolean exists(String a) {
		try {
			Connection oraCon = DBConnection.getOraConn();
			Statement stmt = oraCon.createStatement();
			String SQL = "select count(*) from user_tables where table_name='" + a.toUpperCase() + "'";
			ResultSet rs = stmt.executeQuery(SQL);
			int result = 0;
			while(rs.next()) {
				result = rs.getInt(1);
			}
			rs.close();
			stmt.close();
			oraCon.close();
			if (result == 0) {
				return false;
			}
			else {
				return true;
			}
		}
		catch(Exception E) {
			System.out.println("Error while checking table");
			E.printStackTrace();
			return false;
			
		}
	}

}
