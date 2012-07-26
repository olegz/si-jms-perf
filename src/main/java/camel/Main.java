/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package camel;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.StopWatch;

public final class Main {

    public static void main(String args[]) throws Exception {
    	new ClassPathXmlApplicationContext("broker.xml", Main.class);
    	new ClassPathXmlApplicationContext("consumer.xml", Main.class);
    	ClassPathXmlApplicationContext producer =
    			new ClassPathXmlApplicationContext("producer-temp-reply.xml", Main.class);
    	CamelContext producerContext = producer.getBean(CamelContext.class);

    	ProducerTemplate producerTemplate = producerContext.createProducerTemplate();

    	producerTemplate.setDefaultEndpointUri("direct:invokeCamelOutQueue");

    	for (int i = 0; i < 50; i++) {
			testBatchOfMessages(producerTemplate, 20000, i);
		}
    }

    private static void testBatchOfMessages(final ProducerTemplate template, int number, int batch) throws Exception{
    	StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		for (int i = 0; i < number; i++) {
			template.requestBody(String.valueOf(i));
		}
		stopWatch.stop();
		System.out.println(batch + ". Exchanged " + number + "  messages in " + stopWatch.getTotalTimeMillis());
	}
}
