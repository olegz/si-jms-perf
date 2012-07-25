package si;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.integration.gateway.RequestReplyExchanger;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.util.StopWatch;

public class SiJmsRequestReply {


	public static void main(String[] args) throws Exception{
		testPerformanceTempReply();
	}

	public static void testPerformanceTempReply() throws Exception{
		new ClassPathXmlApplicationContext("broker.xml", SiJmsRequestReply.class);
		new ClassPathXmlApplicationContext("consumer.xml", SiJmsRequestReply.class);
		ClassPathXmlApplicationContext context =
				new ClassPathXmlApplicationContext("producer-temp-reply.xml", SiJmsRequestReply.class);

		RequestReplyExchanger gateway = context.getBean(RequestReplyExchanger.class);
		for (int i = 0; i < 30; i++) {
			testBatchOfMessagesSync(gateway, 20000, i);
		}
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
