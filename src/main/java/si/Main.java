package si;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.integration.Message;
import org.springframework.integration.MessageDeliveryException;
import org.springframework.integration.gateway.RequestReplyExchanger;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.jmx.access.MBeanProxyFactoryBean;
import org.springframework.jmx.support.MBeanServerFactoryBean;
import org.springframework.util.StopWatch;

public class Main {

	private final AtomicLong nulls = new AtomicLong();
	private final AtomicLong timeouts = new AtomicLong();
	private final AtomicLong matches = new AtomicLong();
	private final AtomicLong mismatches = new AtomicLong();
	private final AtomicLong failures = new AtomicLong();
	private volatile CustomErrorHandler errorHandler;

	public static void main(String[] args) throws Exception {
		new Main().testPerformanceTempReply(12, 5, 1000);
	}

	public void testPerformanceTempReply(int numThreads, int numBatches, int batchSize) throws Exception {
		Executor executor = Executors.newFixedThreadPool(numThreads);
		ClassPathXmlApplicationContext brokerContext = new ClassPathXmlApplicationContext("broker.xml", Main.class);
		ClassPathXmlApplicationContext consumerContext = new ClassPathXmlApplicationContext("consumer.xml", Main.class);
		ClassPathXmlApplicationContext producerContext =
				new ClassPathXmlApplicationContext("producer-temp-reply.xml", Main.class);
		this.errorHandler = consumerContext.getBean(CustomErrorHandler.class);
		RequestReplyExchanger gateway = producerContext.getBean(RequestReplyExchanger.class);
		System.out.println("\n==== Started SI Request/Reply performance test with Temporary Reply Queue ====\n");
		System.out.println("Running with " + numThreads + " threads: " + numBatches + " batches of " + batchSize + " messages");		
		for (int i = 0; i < numBatches; i++) {
			testBatchOfMessagesSync(gateway, executor, batchSize, i);
		}
		waitForCompletion(numBatches, batchSize);
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
							nulls.incrementAndGet();
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
		if (!latch.await(1, TimeUnit.HOURS)) {
			throw new RuntimeException("Batch " + batchCount + " failed to complete within 1 hour; Stopping.");
		}
		stopWatch.stop();
		System.out.println((batchCount + 1) + ": Exchanged " + batchSize + "  messages in " + stopWatch.getTotalTimeMillis());
	}

	private void waitForCompletion(int numBatches, int batchSize) throws Exception {
		MBeanServerFactoryBean mbeanServerFactory = new MBeanServerFactoryBean();
		mbeanServerFactory.setLocateExistingServerIfPossible(true);
		mbeanServerFactory.afterPropertiesSet();
		MBeanProxyFactoryBean brokerProxyFactory = new MBeanProxyFactoryBean();
		brokerProxyFactory.setServer(mbeanServerFactory.getObject());
		brokerProxyFactory.setObjectName("org.apache.activemq:BrokerName=localhost,Type=Broker");
		brokerProxyFactory.setProxyInterface(Broker.class);
		brokerProxyFactory.afterPropertiesSet();
		Broker broker = (Broker) brokerProxyFactory.getObject();
		List<ObjectName> queueObjectNames = Arrays.asList(broker.getQueues());
		List<ObjectName> tempQueueObjectNames = Arrays.asList(broker.getTemporaryQueues());
		List<ObjectName> allQueueObjectNames = new ArrayList<ObjectName>(queueObjectNames);
		allQueueObjectNames.addAll(tempQueueObjectNames);
		Map<String,Queue> queues = new HashMap<String, Queue>();
		for (ObjectName objectName : allQueueObjectNames) {
			String queueName = objectName.getKeyProperty("Destination");
			MBeanProxyFactoryBean queueProxyFactory = new MBeanProxyFactoryBean();
			queueProxyFactory.setServer(mbeanServerFactory.getObject());
			queueProxyFactory.setObjectName(objectName);
			queueProxyFactory.setProxyInterface(Queue.class);
			queueProxyFactory.afterPropertiesSet();
			Queue queue = (Queue) queueProxyFactory.getObject();
			queues.put(queueName, queue);
		}
		AtomicLong remainders = new AtomicLong();
		AtomicLong requestsEnqueued = new AtomicLong();
		AtomicLong requestsDequeued = new AtomicLong();
		AtomicLong repliesEnqueued = new AtomicLong();
		AtomicLong repliesDequeued = new AtomicLong();
		long numReplyQueues = queues.size() - 1;
		long totalExpected = numBatches * batchSize;
		System.out.print("Waiting for completion on " + numReplyQueues + " reply queues");
		int count = 0;
		while (!(totalExpected == requestsEnqueued.get() && requestsEnqueued.get() == requestsDequeued.get()
				&& numReplyQueues == repliesEnqueued.get() && repliesEnqueued.get() == repliesDequeued.get() + timeouts.get())) {
			updateCounters(queues, totalExpected, remainders, requestsEnqueued, requestsDequeued, repliesEnqueued, repliesDequeued);
			Thread.sleep(1000);
			System.out.print(".");
			if (++count > 300) {
				System.out.println("exiting after 5 minutes");
				break;
			}
		}
		System.out.println("");
		System.out.println("===================================== DONE =====================================");
		System.out.println("matches:                 " + matches.get());
		System.out.println("mismatches:              " + mismatches.get());
		System.out.println("timeouts:                " + timeouts.get());
		System.out.println("nulls:                   " + nulls.get());
		System.out.println("failures:                " + failures.get());
		System.out.println("JMS listener exceptions: " + errorHandler.getErrorCount());
		System.out.println("================================================================================");
		System.out.println("total remainders: " + remainders + " (on " + numReplyQueues + " queues)");
		System.out.println("total requests enqueued: " + requestsEnqueued);
		System.out.println("total requests dequeued: " + requestsDequeued);
		System.out.println("total replies enqueued:  " + repliesEnqueued);
		System.out.println("total replies dequeued:  " + repliesDequeued);
		System.out.println("================================================================================");
	}

	private void updateCounters(Map<String, Queue> queues, long totalExpected, AtomicLong remainders,
			AtomicLong requestsEnqueued, AtomicLong requestsDequeued, AtomicLong repliesEnqueued, AtomicLong repliesDequeued) {
		remainders.set(0);
		requestsEnqueued.set(0);
		requestsDequeued.set(0);
		repliesEnqueued.set(0);
		repliesDequeued.set(0);
		for (Map.Entry<String, Queue> entry : queues.entrySet()) {
			String queueName = entry.getKey();
			Queue queue = entry.getValue();
			CompositeData[] dataArray = queue.browse();
			long enqueued = queue.getEnqueueCount();
			long dequeued = queue.getDequeueCount();
			if ("siOutQueue".equals(queueName)) {
				requestsEnqueued.addAndGet(enqueued);
				requestsDequeued.addAndGet(dequeued);
			}
			else {
				remainders.addAndGet(dataArray.length);
				repliesEnqueued.addAndGet(enqueued);
				repliesDequeued.addAndGet(dequeued);
			}
		}
	}

	public static interface Broker {
		ObjectName[] getQueues();
		ObjectName[] getTemporaryQueues();
	}

	public static interface Queue {
		CompositeData[] browse();
		long getEnqueueCount();
		long getDequeueCount();
	}

}
