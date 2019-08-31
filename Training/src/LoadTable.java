import java.sql.Connection;
import java.sql.DriverManager;

public class LoadTable {
	String TableName;
	String Owner;
	LoadTable(String name){
		try {
			Owner = name.substring(0, name.indexOf('.')).toUpperCase();
			TableName = name.substring(name.indexOf('.')+1,name.length()).toUpperCase();
		}
		catch(Exception E) {
			System.out.println("TableName not proper");
		}
	}
	
	
	static Connection getOraConn() {
        try {
                Class.forName("oracle.jdbc.driver.OracleDriver");
                return DriverManager.getConnection("jdbc:oracle:thin:@10.10.1.20:1521/noncdb.vishnu.com","vishnu","oracle");
        }
        catch(Exception E) {
                if (E.toString().contains("ClassNotFoundException")) {
                        System.out.println("Java Driver not found");
                        System.out.println("Please Download Oracle JDBC driver(ojdbc10.jar) and place in $JRE_HOME/lib/ext");
                }
                E.printStackTrace();
                return null;
        }
	}
	
	
	
	
}
