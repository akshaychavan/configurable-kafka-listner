package com.configurable.kafka.listner;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;

public class CustomMessageListener implements MessageListener<String, String> {

	@Override
	public void onMessage(ConsumerRecord<String, String> data) {

		System.err.println("offset="+data.offset());
		System.err.println("value="+data.value());
		
	}

}
