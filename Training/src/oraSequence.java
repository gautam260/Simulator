
public class oraSequence {
	static volatile int value = 0;
	synchronized static int nextVal() {
		value++;
		return value;
	}
	synchronized static int getval() {
		return value;
	}
}
