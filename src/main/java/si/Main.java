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
import org.springframework.util.CollectionUtils;
import org.springframework.util.StopWatch;

public class Main {

	private final AtomicLong nulls = new AtomicLong();
	private final AtomicLong timeouts = new AtomicLong();
	private final AtomicLong matches = new AtomicLong();
	private final AtomicLong mismatches = new AtomicLong();
	private final AtomicLong failures = new AtomicLong();

	public static void main(String[] args) throws Exception {
		new Main().testPerformanceTempReply(25, 10, 1000);
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
		waitForCompletion(numBatches, batchSize);
		producerContext.stop();
		consumerContext.stop();
		brokerContext.stop();
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
		Map<String, Queue> queuesToCheck = queues;
		long numQueues = queuesToCheck.size();
		while (!CollectionUtils.isEmpty(queuesToCheck)) {
			System.out.println("waiting 10 seconds for processing to complete (" + queuesToCheck.size() + " queues to check)");
			queuesToCheck = updateStatistics(queuesToCheck, numBatches * batchSize, remainders,
					requestsEnqueued, requestsDequeued, repliesEnqueued, repliesDequeued);
			Thread.sleep(10 * 1000);
		}
		System.out.println("===================================== DONE =====================================");
		System.out.println("matches:    " + matches.get());
		System.out.println("mismatches: " + mismatches.get());
		System.out.println("timeouts:   " + timeouts.get());
		System.out.println("nulls:      " + nulls.get());
		System.out.println("failures:   " + failures.get());
		System.out.println("================================================================================");
		System.out.println("total remainders: " + remainders + " (on " + (numQueues - 1) + " queues)");
		System.out.println("total requests enqueued: " + requestsEnqueued);
		System.out.println("total requests dequeued: " + requestsDequeued);
		System.out.println("total replies enqueued:  " + repliesEnqueued);
		System.out.println("total replies dequeued:  " + repliesDequeued);
		System.out.println("================================================================================");
	}

	private Map<String, Queue> updateStatistics(Map<String, Queue> queuesToCheck, long totalExpected, AtomicLong remainders,
			AtomicLong requestsEnqueued, AtomicLong requestsDequeued, AtomicLong repliesEnqueued, AtomicLong repliesDequeued) {
		Map<String, Queue> incompleteQueues = new HashMap<String, Queue>();
		for (Map.Entry<String, Queue> entry : queuesToCheck.entrySet()) {
			String queueName = entry.getKey();
			Queue queue = entry.getValue();
			CompositeData[] dataArray = queue.browse();
			long enqueued = queue.getEnqueueCount();
			long dequeued = queue.getDequeueCount();
			if ("siOutQueue".equals(queueName)) {
				if (enqueued == dequeued) {
					requestsEnqueued.addAndGet(enqueued);
					requestsDequeued.addAndGet(dequeued);
				}
				else {
					incompleteQueues.put(queueName, queue);
				}
			}
			else {
				remainders.addAndGet(dataArray.length);
				if (enqueued > 0) {
					repliesEnqueued.addAndGet(enqueued);
					repliesDequeued.addAndGet(dequeued);
				}
				else {
					incompleteQueues.put(queueName, queue);
				}
			}
		}
		return incompleteQueues; 
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
		if (!latch.await(300, TimeUnit.SECONDS)) {
			throw new RuntimeException("Batch " + batchCount + " failed to complete within 5 minutes; Stopping.");
		}
		stopWatch.stop();
		System.out.println(batchCount + ". Exchanged " + batchSize + "  messages in " + stopWatch.getTotalTimeMillis());
	}

}
