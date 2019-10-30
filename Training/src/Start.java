import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Start {
	public static void main(String[] args) throws InterruptedException {
		 DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");  
		   LocalDateTime now = LocalDateTime.now();  
		   System.out.println(dtf.format(now));
		CBC a = new CBC();
		a.createTable();
		a.loadTable();
	}
}
