import java.util.ArrayList;

public class Start {
	public static void main(String[] args) throws InterruptedException {
		ArrayList<Integer> asd = new ArrayList<>();
		asd.add(50000000);
		asd.add(5000);
		asd.add(60);
		asd.add(50);
		asd.add(50000);
		asd.add(1800);
		asd.add(1800);
		asd.add(1800);
		System.out.println(asd.get(0));
		
		LoadTable a = new LoadTable("vishnu.temp",asd);
		a.loadData(40);
	}
}
