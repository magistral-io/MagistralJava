package io.magistral.client;

import java.util.Properties;

public class Configs {
	
	private static Properties producer;
	
	static {
		producer = new Properties();
		
		producer.put("acks", "1");
		producer.put("retries", "5");
		producer.put("batch.size", "65536");
		producer.put("linger.ms", 5);
		producer.put("request.timeout.ms", 60000);
		producer.put("max.in.flight.requests.per.connection", 3);
				
		producer.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		producer.put("compression.type", "gzip");
	}

	public static Properties producer() {		
		return producer;
	}
}
