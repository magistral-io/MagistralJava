package io.magistral.client.sub;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;

import javax.crypto.Cipher;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import io.magistral.Message;
import io.magistral.client.MagistralException;

public class MagistralConsumer {	
	
	private Cipher cipher;
	
	@SuppressWarnings("unused")
	private String pubKey, subKey, authKey;
	
	private final static int HISTORY_DATA_FETCH_SIZE_LIMIT = 100000;
	
	private Properties props; {
		
		String home = System.getProperty("user.home");			
		File magistralDir = new File(home + "/magistral");
		
		props = new Properties();
		props.put("enable.auto.commit", "false");
		props.put("session.timeout.ms", "30000");
		props.put("fetch.min.bytes", "65536");
		props.put("max.partition.fetch.bytes", "1048576");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		
		props.put("security.protocol", "SSL");
		props.put("ssl.truststore.location", magistralDir.getAbsolutePath() + "/ts");
		props.put("ssl.truststore.password", "magistral");
		
		props.put("ssl.keystore.location",  magistralDir.getAbsolutePath() + "/ks");
		props.put("ssl.keystore.password", "magistral");
		props.put("ssl.key.password", "magistral");
	}

	public MagistralConsumer(String pubKey, String subKey, String sKey, String bootstrapServers, Cipher dCipher) {
		props.put("bootstrap.servers", bootstrapServers);
		
		this.pubKey = pubKey;
		this.subKey = subKey;
		this.authKey = sKey;
		
		this.cipher = dCipher;
	}
	
	public List<Message> history(String topic, int channel, int records) throws MagistralException {	
		
		KafkaConsumer<byte[], byte[]> consumer = null;
		try {
			if (records > HISTORY_DATA_FETCH_SIZE_LIMIT) records = HISTORY_DATA_FETCH_SIZE_LIMIT;
			
			List<Message> out = new ArrayList<>(records);
			
			String kfkTopic = subKey + "." + topic;
			
			TopicPartition x = new TopicPartition(kfkTopic, channel);
			
			List<TopicPartition> tpL = Arrays.asList(x);
			
			props.put("fetch.min.bytes", "4096");
			props.setProperty("max.partition.fetch.bytes", records * 65536 + "");
			consumer = new KafkaConsumer<>(props);
			consumer.assign(tpL);
						
			long offset = consumer.position(x);
			
			consumer.seek(x, offset > records ? offset - records : 0);
			ConsumerRecords<byte[], byte[]> data = consumer.poll(5000);
			
			for (ConsumerRecord<byte[], byte[]> r : data) {			    
				Message msg = new Message();
				msg.setTopic(topic);
				msg.setChannel(channel);
								
				byte[] body = r.value();
				
				if (cipher != null) {
					byte[] encrypted = Base64.getDecoder().decode(body);    			
					msg.setBody(cipher.doFinal(encrypted));
				} else {
					msg.setBody(body);
				}
				
				msg.setBody((cipher == null) ? body : cipher.doFinal(body));
					
				byte[] b = r.key();		
				String key = new String(b, StandardCharsets.UTF_8);
				
				if (key.matches("\\d+")) {
					msg.setTimestamp(Long.parseLong(key));
				} else {
					msg.setTimestamp(0);
				}
				out.add(msg);
			}
			
			return out;
		} catch (Exception e) {
			throw new MagistralException(e);
		} finally {
			if (consumer != null) consumer.close();
		}
	}	
	
	public List<Message> history(String topic, int channel, long start, int count) throws MagistralException {		
		
		KafkaConsumer<byte[], byte[]> consumer = null;
		try {
			
			if (count > HISTORY_DATA_FETCH_SIZE_LIMIT) count = HISTORY_DATA_FETCH_SIZE_LIMIT;
			
			List<Message> out = new ArrayList<>(count);
			
			String kfkTopic = subKey + "." + topic;
			
			TopicPartition x = new TopicPartition(kfkTopic, channel);
			
			List<TopicPartition> tpL = Arrays.asList(x);
			
			props.put("fetch.min.bytes", "16384");
			props.put("max.partition.fetch.bytes", "16384"); // TODO adjust with AVGs
			consumer = new KafkaConsumer<>(props);
			consumer.assign(tpL);
			
			boolean found = false;
			long position = consumer.position(x) - count;
			
			while (!found) {
				consumer.seek(x, position);
				ConsumerRecords<byte[], byte[]> data = consumer.poll(2000);
				
				if (data.count() == 0) break;
				ConsumerRecord<byte[], byte[]> r = data.iterator().next();
				
				byte[] b = r.key();		
				String key = new String(b, StandardCharsets.UTF_8);

				if (Long.parseLong(key) < start) found = true;
				
				position = position - count;
			}
			consumer.close();
			
			props.put("fetch.min.bytes", "65536");
			props.put("max.partition.fetch.bytes", 65536 * count);
			consumer = new KafkaConsumer<>(props);
			consumer.assign(tpL);
			
			consumer.seek(x, position);
			
			ConsumerRecords<byte[], byte[]> data = consumer.poll(10000);
			
			int counter = 0;
			for (ConsumerRecord<byte[], byte[]> r : data) {				
				if (counter == count) return out;
				
				byte[] b = r.key();		
				String key = new String(b, StandardCharsets.UTF_8);
				
				long timestamp = Long.parseLong(key);
				if (timestamp < start) continue;
				
				Message msg = new Message();
				msg.setTopic(topic);
				msg.setChannel(channel);
				
				if (key.matches("\\d+")) {
					msg.setTimestamp(timestamp);
				} else {
					msg.setTimestamp(0);
				}
											
				byte[] body = r.value();
				
				if (cipher != null) {
					byte[] encrypted = Base64.getDecoder().decode(body);    			
					msg.setBody(cipher.doFinal(encrypted));
				} else {
					msg.setBody(body);
				}
				
				out.add(msg);				
				counter++;
			}
			
			return out;
		} catch (Exception e) {
			e.printStackTrace();
			throw new MagistralException(e);
		} finally {
			if (consumer != null) consumer.close();
		}
	}
	
	public List<Message> historyForTimePeriod(String topic, int channel, long start, long end) throws MagistralException {
		
		KafkaConsumer<byte[], byte[]> consumer = null;
		try {			
			List<Message> out = new ArrayList<>();
			
			String kfkTopic = subKey + "." + topic;
			
			TopicPartition x = new TopicPartition(kfkTopic, channel);
			
			List<TopicPartition> tpL = Arrays.asList(x);
			
			props.put("fetch.min.bytes", "16384");
			props.put("max.partition.fetch.bytes", "16384"); // TODO adjust with AVGs
			consumer = new KafkaConsumer<>(props);
			consumer.assign(tpL);
			
			boolean found = false;
			long position = consumer.position(x) - 2000;
			
			while (!found) {
				consumer.seek(x, position);
				ConsumerRecords<byte[], byte[]> data = consumer.poll(5000);
				
				if (data.count() == 0) break;
				ConsumerRecord<byte[], byte[]> r = data.iterator().next();
				
				byte[] b = r.key();		
				String key = new String(b, StandardCharsets.UTF_8);

				if (Long.parseLong(key) < start) found = true;
				
				position = position - 2000;
			}
			consumer.close();
			
			props.put("fetch.min.bytes", "65536");
			props.put("max.partition.fetch.bytes", 65536 * 20000);
			consumer = new KafkaConsumer<>(props);
			consumer.assign(tpL);
			
			consumer.seek(x, position);
			
			ConsumerRecords<byte[], byte[]> data = consumer.poll(20000);
						
			for (ConsumerRecord<byte[], byte[]> r : data) {
				
				byte[] b = r.key();		
				String key = new String(b, StandardCharsets.UTF_8);
				
				long timestamp = Long.parseLong(key);
				if (timestamp < start) continue;
				
				Message msg = new Message();
				msg.setTopic(topic);
				msg.setChannel(channel);
				
				if (key.matches("\\d+")) {
					msg.setTimestamp(timestamp);
				} else {
					msg.setTimestamp(0);
				}
				
//				System.out.println(new Date(timestamp));
				
				if (timestamp > end) break;
											
				byte[] body = r.value();
				
				if (cipher != null) {
					byte[] encrypted = Base64.getDecoder().decode(body);    			
					msg.setBody(cipher.doFinal(encrypted));
				} else {
					msg.setBody(body);
				}
				
				out.add(msg);
			}
			
			return out;
		} catch (Exception e) {
			e.printStackTrace();
			throw new MagistralException(e);
		} finally {
			if (consumer != null) consumer.close();
		}
	}
}
