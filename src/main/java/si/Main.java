package si;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.integration.gateway.RequestReplyExchanger;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.util.StopWatch;

public class Main {


	public static void main(String[] args) throws Exception{
		testPerformanceTempReply();
	}

	public static void testPerformanceTempReply() throws Exception{
		new ClassPathXmlApplicationContext("broker.xml", Main.class);
		new ClassPathXmlApplicationContext("consumer.xml", Main.class);
		ClassPathXmlApplicationContext context =
				new ClassPathXmlApplicationContext("producer-temp-reply.xml", Main.class);

		RequestReplyExchanger gateway = context.getBean(RequestReplyExchanger.class);
		System.out.println("\n==== Started SI Request/Reply performance test with Temporary Reply Queue ====\n");
		for (int i = 0; i < 30; i++) {
			testBatchOfMessagesSync(gateway, 20000, i);
		}
		System.out.println("******* The END *******");
	}

	private static void testBatchOfMessagesSync(final RequestReplyExchanger gateway, int number, int batch) throws Exception{
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		for (int i = 0; i < number; i++) {
			gateway.exchange(MessageBuilder.withPayload(String.valueOf(i)).build());
		}
		stopWatch.stop();
		System.out.println(batch + ". Exchanged " + number + "  messages in " + stopWatch.getTotalTimeMillis());

	}
}
