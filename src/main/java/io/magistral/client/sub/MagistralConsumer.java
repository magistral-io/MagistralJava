package io.magistral.client.sub;

import java.io.File;
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
		
		props = new Properties();
		props.put("enable.auto.commit", "false");
		props.put("session.timeout.ms", "15000");
		
		props.put("fetch.min.bytes", "64");
		props.put("max.partition.fetch.bytes", "65565");
		props.put("fetch.max.wait.ms", "176");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		
	}

	public MagistralConsumer(String pubKey, String subKey, String sKey, String token, String bootstrapServers, Cipher dCipher) {
		
		this.pubKey = pubKey;
		this.subKey = subKey;
		this.authKey = sKey;
		
		this.cipher = dCipher;
		
		String home = System.getProperty("user.home");			
		File dir = new File(home + "/magistral" + "/" + token);
		
		props.put("bootstrap.servers", bootstrapServers);
		
		props.put("security.protocol", "SSL");
		props.put("ssl.truststore.location", dir.getAbsolutePath() + "/ts");
		props.put("ssl.truststore.password", "magistral");
		
		props.put("ssl.keystore.location",  dir.getAbsolutePath() + "/ks");
		props.put("ssl.keystore.password", "magistral");
		props.put("ssl.key.password", "magistral");
	}
	
	public List<Message> history(String topic, int channel, int records) throws MagistralException {	
		
		KafkaConsumer<byte[], byte[]> consumer = null;
		try {
			if (records > HISTORY_DATA_FETCH_SIZE_LIMIT) records = HISTORY_DATA_FETCH_SIZE_LIMIT;
			
			List<Message> out = new ArrayList<>(records);
						
			TopicPartition x = new TopicPartition(subKey + "." + topic, channel);
			
			props.put("fetch.min.bytes", "64");	
			props.put("fetch.max.wait.ms", "176");	
			
			consumer = new KafkaConsumer<>(props);			
			consumer.assign(Arrays.asList(x));
			
			Map<TopicPartition, Long> offsets = consumer.endOffsets(consumer.assignment());			
			
			long last = offsets.get(x);			
			long pos = (last - records < 0) ? consumer.beginningOffsets(consumer.assignment()).get(x) : (last - records);
			
			consumer.seek(x, pos);
			ConsumerRecords<byte[], byte[]> data = consumer.poll(200);
			
			spitz : while (!data.isEmpty() && out.size() < records) {
				for (ConsumerRecord<byte[], byte[]> r : data) {
					if (out.size() >= records) break spitz;
					
					Message m = new Message();
					m.setTopic(topic);
					m.setIndex(r.offset());
					m.setChannel(r.partition());
					m.setTimestamp(r.timestamp());	
					m.setBody(cipher != null ? cipher.doFinal(Base64.getDecoder().decode(r.value())) : r.value());
					
					out.add(m);
				}
				
				data = consumer.poll(200);
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
			
			TopicPartition x = new TopicPartition(subKey + "." + topic, channel);
						
			props.put("fetch.min.bytes", "64");
			props.put("fetch.max.wait.ms", "176");
			
			props.put("max.partition.fetch.bytes", "65565"); // TODO adjust with AVGs
			
			consumer = new KafkaConsumer<>(props);
			consumer.assign(Arrays.asList(x));
			
			boolean found = false;
			
			Map<TopicPartition, Long> offsets = consumer.endOffsets(consumer.assignment());			
			long last = offsets.get(x);
			
			long position = last - count < 0 ? consumer.beginningOffsets(consumer.assignment()).get(x) : last - count;
			
			while (!found) {				
				consumer.seek(x, position);
				ConsumerRecords<byte[], byte[]> data = consumer.poll(200);
				
				if (data.count() == 0) break;
				ConsumerRecord<byte[], byte[]> r = data.iterator().next();
				
				if (r.timestamp() <= start) {
					position = r.offset(); found = true;				
				}
				if (position == 0) break;
			}

			consumer.close();
			
			consumer = new KafkaConsumer<>(props);
			consumer.assign(Arrays.asList(x));
			
			consumer.seek(x, position);
			
			int counter = 0;
			
			ConsumerRecords<byte[], byte[]> data = consumer.poll(200);
			spitz : while (!data.isEmpty() && counter < count) {
				
				for (ConsumerRecord<byte[], byte[]> r : data) {
					
					if (r.timestamp() <= start) continue;
					if (out.size() >= counter) break spitz;
					
					Message m = new Message();
					m.setTopic(topic);
					m.setIndex(r.offset());
					m.setChannel(r.partition());
					m.setTimestamp(r.timestamp());					
					m.setBody(cipher != null ? cipher.doFinal(Base64.getDecoder().decode(r.value())) : r.value());					
					out.add(m);				
					counter++;
				}
				
				data = consumer.poll(200);
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
			
			TopicPartition x = new TopicPartition(subKey + "." + topic, channel);
						
			props.put("fetch.min.bytes", "64");
			props.put("fetch.max.wait.ms", "176");			
			props.put("max.partition.fetch.bytes", "65565");
			
			consumer = new KafkaConsumer<>(props);
			consumer.assign(Arrays.asList(x));
			
			boolean found = false;
			int hop = 1000;
			
			Map<TopicPartition, Long> offsets = consumer.endOffsets(consumer.assignment());			
			long last = offsets.get(x);
			
			long position = last - hop < 0 ? consumer.beginningOffsets(consumer.assignment()).get(x) : last - hop;
			
			while (!found) {
				consumer.seek(x, position);
				ConsumerRecords<byte[], byte[]> data = consumer.poll(200);
				
				if (data.count() == 0) break;
				ConsumerRecord<byte[], byte[]> r = data.iterator().next();
			
				if (r.timestamp() < start) {
					position = r.offset(); found = true;
				}
				
				if (position == 0) break;				
				position =- hop;
			}
			consumer.close();
			
			
			consumer = new KafkaConsumer<>(props);
			consumer.assign(Arrays.asList(x));
			
			consumer.seek(x, position);
			
			ConsumerRecords<byte[], byte[]> data = consumer.poll(200);						
			spitz : while (!data.isEmpty()) {
				
				for (ConsumerRecord<byte[], byte[]> r : data) {
					
					if (r.timestamp() <= start) continue;
					if (r.timestamp() >= end || out.size() == HISTORY_DATA_FETCH_SIZE_LIMIT) break spitz;
					
					Message m = new Message();
					m.setTopic(topic);
					m.setIndex(r.offset());
					m.setChannel(r.partition());
					m.setTimestamp(r.timestamp());					
					m.setBody(cipher != null ? cipher.doFinal(Base64.getDecoder().decode(r.value())) : r.value());
					out.add(m);
				}
				
				data = consumer.poll(200);
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
