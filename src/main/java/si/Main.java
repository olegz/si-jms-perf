package si;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

	private final AtomicLong timeouts = new AtomicLong();
	private final AtomicLong matches = new AtomicLong();
	private final AtomicLong mismatches = new AtomicLong();
	private final AtomicLong failures = new AtomicLong();

	public static void main(String[] args) throws Exception {
		new Main().testPerformanceTempReply(25, 5, 100);
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

		MBeanServerFactoryBean mbeanServerFactory = new MBeanServerFactoryBean();
		mbeanServerFactory.setLocateExistingServerIfPossible(true);
		mbeanServerFactory.afterPropertiesSet();
		
		MBeanProxyFactoryBean brokerProxyFactory = new MBeanProxyFactoryBean();
		brokerProxyFactory.setServer(mbeanServerFactory.getObject());
		brokerProxyFactory.setObjectName("org.apache.activemq:BrokerName=localhost,Type=Broker");
		brokerProxyFactory.setProxyInterface(Broker.class);
		brokerProxyFactory.afterPropertiesSet();
		Broker broker = (Broker) brokerProxyFactory.getObject();
		List<ObjectName> queues = Arrays.asList(broker.getQueues());
		List<ObjectName> tempQueues = Arrays.asList(broker.getTemporaryQueues());
		List<ObjectName> objectNames = new ArrayList<ObjectName>(queues);
		objectNames.addAll(tempQueues);
		long remainders = 0;
		for (ObjectName objectName : objectNames) {
			if ("siOutQueue".equals(objectName.getKeyProperty("Destination"))) {
				// we only count remainders on REPLY queues
				continue;
			}
			MBeanProxyFactoryBean browserProxyFactory = new MBeanProxyFactoryBean();
			browserProxyFactory.setServer(mbeanServerFactory.getObject());
			browserProxyFactory.setObjectName(objectName);
			browserProxyFactory.setProxyInterface(QueueBrowser.class);
			browserProxyFactory.afterPropertiesSet();
			QueueBrowser browser = (QueueBrowser) browserProxyFactory.getObject();
			CompositeData[] dataArray = browser.browse();
			remainders += dataArray.length;
		}
		System.out.println("total remainders: " + remainders + " (on " + (objectNames.size() - 1) + " queues)");

		producerContext.stop();
		consumerContext.stop();
		brokerContext.stop();
	}

	public static interface Broker {
		ObjectName[] getQueues();
		ObjectName[] getTemporaryQueues();
	}

	public static interface QueueBrowser {
		CompositeData[] browse();
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
