package si;

import java.util.Random;

public class MyRandomlySlowService{
	Random random = new Random();

	public String echo(String value) throws Exception{
		Thread.sleep(random.nextInt(3000));
		return value;
	}
}
