package io.magistral.client.sub;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import io.magistral.Message;
import io.magistral.client.MagistralException;
import io.magistral.client.MessageEvent;
import io.magistral.client.perm.PermMeta;

@SuppressWarnings("unused")
public class GroupConsumer implements Runnable {
	
	private String group;

	private final AtomicBoolean isAlive = new AtomicBoolean(true);
	
	private KafkaConsumer<byte[], byte[]> consumer;
	
	private String subKey;
	private Cipher cipher;	
	
	private Map<String, Map<Integer, NetworkListener>> map = new ConcurrentHashMap<>();
	
	private List<PermMeta> permissions;
	
	private Map<String, Map<Integer, Long>> offsets = new HashMap<>();
	
	private static Properties createConsumerConfig(String bootstrapServers, String groupId, String token) {
		
		String home = System.getProperty("user.home");			
		File dir = new File(home + "/magistral/" + token);
		
		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrapServers);
		props.put("group.id", groupId);
		
		props.put("heartbeat.interval.ms", "2000");		
		props.put("metadata.max.age.ms", "180000");
		
		props.put("enable.auto.commit", "false");
		props.put("auto.commit.interval.ms", "2000");
		props.put("max.poll.records", "250");
		
		props.put("session.timeout.ms", "20000");
		props.put("fetch.min.bytes", "64");
		props.put("fetch.wait.max.ms", "96");		
		props.put("max.partition.fetch.bytes", "65565");
		
		props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		
		props.put("security.protocol", "SSL");
		props.put("ssl.truststore.location", dir.getAbsolutePath() + "/ts");
		props.put("ssl.truststore.password", "magistral");
		
		props.put("ssl.keystore.location",  dir.getAbsolutePath() + "/ks");
		props.put("ssl.keystore.password", "magistral");
		props.put("ssl.key.password", "magistral");
		
		return props;
	}
	
	public GroupConsumer(String sKey, String bootstrapServers, String groupId, String token, List<PermMeta> permissions) {
		this(sKey, bootstrapServers, groupId, token, null, permissions);
	}

	public GroupConsumer(String sKey, String bootstrapServers, String groupId, String token, Cipher _cipher, List<PermMeta> permissions) {
		this.group = groupId;
		this.subKey = sKey;
		
		consumer = new KafkaConsumer<byte[], byte[]>(createConsumerConfig(bootstrapServers, groupId, token));
		this.cipher = _cipher;
		
		this.permissions = permissions;
	}

	public void shutdown() {
		 isAlive.set(false);
         consumer.wakeup();
	}	

	public void subscribe(String topic, int channel, NetworkListener callback) throws MagistralException {
		if (channel < -1) return;
		
		String etopic = subKey + "." + topic;
		
		List<PartitionInfo> pif = consumer.partitionsFor(etopic);
		if (pif == null || pif.isEmpty() || channel >= pif.size()) {
			
			Map<String, List<PartitionInfo>> ltis = consumer.listTopics();
			if (ltis == null || !ltis.keySet().contains(etopic)) {
				MagistralException mex = new MagistralException("Topic [" + topic + "] does not exist.");
				callback.error(mex);
				throw mex;
			}
		}
		
		Collection<Integer> channels = new HashSet<>(channel == -1 ? pif.size() : 1);
		if (channel == -1) {
			for (PartitionInfo pi : pif) channels.add(pi.partition());
		} else {
			channels.add(channel);
		}
		
		subscribe(topic, channels, callback);
	}
	
	public void subscribe(String topic, Collection<Integer> channels, NetworkListener callback) throws MagistralException {		
		if (channels == null || channels.size() == 0) return;
				
		if (channels.size() > 1 && channels.contains(-1)) channels.remove(-1);
		
		String etopic = subKey + "." + topic;
		
		List<PartitionInfo> pis = consumer.partitionsFor(etopic);
		if (pis == null || pis.size() == 0) {
			
			Map<String, List<PartitionInfo>> ltis = consumer.listTopics();
			if (ltis == null || !ltis.keySet().contains(etopic)) {
				MagistralException mex = new MagistralException("Topic [" + topic + "] does not exist.");
				callback.error(mex);
				throw mex;
			}
		}
				
		System.out.println("Subscribe -> " + topic + ":" + channels + " // " + subKey);
		
		if (this.permissions == null || this.permissions.size() == 0) {
			MagistralException mex = new MagistralException("User has no permissions for topic [" + topic + "].");
			mex.printStackTrace();
			callback.error(mex);
			throw mex;
		}
		
		for (PermMeta pm : permissions) {
			if (!pm.topic().equals(topic)) continue;			
			
			for (Iterator<Integer> it = channels.iterator(); it.hasNext();) {
				int ch = it.next();
				if (!pm.channels().contains(ch) || !pm.readable(ch)) it.remove();
			}
		}
		
		String npgex = "No permissions for topic [" + topic + "] granted";
		
		if (channels.isEmpty()) {
			MagistralException mex = new MagistralException(npgex);
			mex.printStackTrace();
			callback.error(mex);
			throw mex;
		}
		
		if (!map.containsKey(etopic)) map.put(etopic, new HashMap<Integer, NetworkListener>());
		
		// Assign Topic-partition pairs to listen
		
		List<TopicPartition> tpas = new ArrayList<>(channels.size());
		for (int ch : channels) {
			tpas.add(new TopicPartition(etopic, ch));
//			Add Listener
			map.get(etopic).put(ch, callback);
		}
		consumer.assign(tpas);
	}
	
	public void unsubscribe(String topic) {
		consumer.unsubscribe();
		map.remove(topic);
		
		List<TopicPartition> tpas = new ArrayList<>();
		for (String t : map.keySet()) {
			Map<Integer, NetworkListener> chm = map.get(t);
			for (Integer p : chm.keySet()) tpas.add(new TopicPartition(t, p));
		}
		consumer.assign(tpas);
	}

	public void run() {		
		try {			
			while (isAlive.get()) {			
				ConsumerRecords<byte[], byte[]> records = consumer.poll(128);
				if (records.count() == 0) continue;
				
				for (ConsumerRecord<byte[], byte[]> record : records) {							
					handle(record);
				}
				
				consumer.commitAsync();
			}
		} catch(Exception ex) {
			ex.printStackTrace();
		} finally {
			consumer.close();
		}
	}
	
	public void handle(ConsumerRecord<byte[], byte[]> record) {
		String topic = record.topic();
		int channel = record.partition();
		long timestamp = record.timestamp();
		
		try {	        			        		
    		byte[] decrypted = record.value();
    		if (cipher != null) {
    			byte[] encrypted = Base64.getDecoder().decode(decrypted);    			
    			decrypted = cipher.doFinal(encrypted);
    		}
    		
    		if (!map.containsKey(topic)) return;	        		
    		if (!map.get(topic).keySet().contains(channel)) return;
    		
    		String t = topic.substring(topic.indexOf('.') + 1);
    		
    		map.get(topic).get(channel).messageReceived(new MessageEvent(t, channel, decrypted, record.offset(), timestamp));
    		
    	} catch (IllegalBlockSizeException e) {
    		String m = e.getMessage(); 
			if (m.startsWith("Input length must be multiple") && m.endsWith("when decrypting with padded cipher")) {						
				String t = topic.substring(topic.indexOf('.') + 1);
				
				byte[] decrypted = record.value();						
				map.get(topic).get(channel).messageReceived(new MessageEvent(t, record.partition(), decrypted, record.offset(), timestamp));
			} else {
				e.printStackTrace();
			}
		} catch (BadPaddingException e) {
			String m = e.getMessage(); 
			if (m.startsWith("Given final block not properly padded")) {
				String t = topic.substring(topic.indexOf('.') + 1);
				
				byte[] decrypted = record.value();						
				map.get(topic).get(channel).messageReceived(new MessageEvent(t, record.partition(), decrypted, record.offset(), timestamp));
			} else {
				e.printStackTrace();
			}
		} catch (WakeupException e) {
        	if (isAlive.get()) throw e;
        }
	}

	public void unsubscribe(String string, int channel) {
		
	}
}
