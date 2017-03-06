package io.magistral.client;

import java.util.Properties;

public class Configs {
	
	private static Properties producer;
	
	static {
		producer = new Properties();
		producer.put("producer.type", "async");
		producer.put("acks", "0");
		producer.put("retries", "10");
		producer.put("batch.size", "512");
		producer.put("linger.ms", 5);
		producer.put("request.timeout.ms", 60000);
		producer.put("max.in.flight.requests.per.connection", 1);
				
		producer.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer.put("value.serializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		producer.put("compression.type", "gzip");
	}

	public static Properties producer() {		
		return producer;
	}
}
