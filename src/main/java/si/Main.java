package si;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.integration.Message;
import org.springframework.integration.MessageDeliveryException;
import org.springframework.integration.gateway.RequestReplyExchanger;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.util.StopWatch;

public class Main {

	private final AtomicLong timeouts = new AtomicLong();
	private final AtomicLong matches = new AtomicLong();
	private final AtomicLong mismatches = new AtomicLong();
	private final AtomicLong failures = new AtomicLong();

	public static void main(String[] args) throws Exception {
		new Main().testPerformanceTempReply(10, 3, 10);
	}

	public void testPerformanceTempReply(int numThreads, int numBatches, int batchSize) throws Exception {
		Executor executor = Executors.newFixedThreadPool(numThreads);
		ClassPathXmlApplicationContext brokerContext = new ClassPathXmlApplicationContext("broker.xml", Main.class);
		ClassPathXmlApplicationContext consumerContext = new ClassPathXmlApplicationContext("consumer.xml", Main.class);
		ClassPathXmlApplicationContext producerContext =
				new ClassPathXmlApplicationContext("producer-temp-reply.xml", Main.class);

		RequestReplyExchanger gateway = producerContext.getBean(RequestReplyExchanger.class);
		System.out.println("\n==== Started SI Request/Reply performance test with Temporary Reply Queue ====\n");
		for (int i = 0; i < numBatches; i++) {
			testBatchOfMessagesSync(gateway, executor, batchSize, i);
		}
		System.out.println("===================================== DONE =====================================");
		System.out.println("matches=" + matches.get() + ", mismatches=" + mismatches.get()
				+ ", timeouts=" + timeouts.get() + ", failures=" + failures.get());
		producerContext.stop();
		consumerContext.stop();
		brokerContext.stop();
	}

	private void testBatchOfMessagesSync(final RequestReplyExchanger gateway, Executor executor, int batchSize, final int batchCount) throws Exception{
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		final CountDownLatch latch = new CountDownLatch(batchSize);
		for (int i = 0; i < batchSize; i++) {
			final int loopCount = i;
			executor.execute(new Runnable() {
				public void run() {
					try {
						String requestPayload = batchCount + "." + loopCount;
						Message<?> reply = gateway.exchange(MessageBuilder.withPayload(requestPayload).build());
						if (reply == null) {
							timeouts.incrementAndGet();
						}
						else if (reply.getPayload().equals(requestPayload)) {
							matches.incrementAndGet();
						}
						else {
							mismatches.incrementAndGet();
						}
					}
					catch (Exception e) {
						if (e instanceof MessageDeliveryException) {
							timeouts.incrementAndGet();
						}
						else {
							failures.incrementAndGet();
						}
					}
					finally {
						latch.countDown();
					}
				}
			});
		}
		if (!latch.await(300, TimeUnit.SECONDS)) {
			throw new RuntimeException("Batch " + batchCount + " failed to complete within 5 minutes; Stopping.");
		}
		stopWatch.stop();
		System.out.println(batchCount + ". Exchanged " + batchSize + "  messages in " + stopWatch.getTotalTimeMillis());
	}

}
