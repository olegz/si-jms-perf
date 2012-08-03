package si;

import java.util.concurrent.atomic.AtomicLong;

import org.springframework.util.ErrorHandler;

public class CustomErrorHandler implements ErrorHandler {

	private final AtomicLong count = new AtomicLong();

	@Override
	public void handleError(Throwable t) {
		long n = count.incrementAndGet();
		System.out.println("error handler received error " + n + ": " + t.getMessage());
		//throw (t instanceof RuntimeException) ? (RuntimeException) t : new RuntimeException(t);
	}

	public long getErrorCount() {
		return this.count.get();
	}

}
