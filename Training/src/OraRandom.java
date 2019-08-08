import java.util.Random;

public class OraRandom {
	static int randomSkewInt(int a) {
		return Math.abs(Math.round((new Random().nextInt()/(new Random().nextInt()/4))%a)); //increasing 7 reduces the skew.
	}
	
	static int randomUniformInt(int a) {
		return Math.abs(new Random().nextInt()%a);
	}
	
	 static String randomString(int a) {
	    int leftLimit = 97;
	    int rightLimit = 122;
	    Random random = new Random();
	    StringBuilder buffer = new StringBuilder(a);
	    for (int i = 0; i < a; i++) {
	        int randomLimitedInt = leftLimit + (int) 
	          (random.nextFloat() * (rightLimit - leftLimit + 1));
	        buffer.append((char) randomLimitedInt);
	    }
	    return buffer.toString();
	}
}
