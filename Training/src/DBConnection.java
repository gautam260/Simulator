import java.sql.Connection;
import java.sql.DriverManager;

class DBConnection {
        static Connection getOraConn() {
                try {
                        Class.forName("oracle.jdbc.driver.OracleDriver");
                        return DriverManager.getConnection("jdbc:oracle:thin:@192.168.0.22:1521:noncdb","vishnu","oracle");
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
        static Connection getOraSysCon() {
        	 try {
                 Class.forName("oracle.jdbc.driver.OracleDriver");
                 return DriverManager.getConnection("jdbc:oracle:thin:@192.168.0.20:1521:noncdb","sys as sysdba","oracle");
        	 }
        	 catch(Exception E) {
                 if (E.toString().contains("ClassNotFoundException")) {
                         System.out.println("Java Driver not found");
                         System.out.println("Please Download Oracle JDBC driver(ojdbc10.jar) and place in $JRE_HOME/lib/ext");
                 }
                 return null;
         	}
        }
}
