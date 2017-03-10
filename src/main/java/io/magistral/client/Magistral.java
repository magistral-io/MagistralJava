package io.magistral.client;

import java.io.File;
import java.nio.charset.StandardCharsets;

import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.*;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.magistral.Message;
import io.magistral.client.data.History;
import io.magistral.client.java.util.REST;
import io.magistral.client.java.util.PrimitiveFactory;
import io.magistral.client.java.util.TimedMap;
import io.magistral.client.java.util.TimedMap.ExpiredObject;
import io.magistral.client.java.util.TimedMapEvictionListener;
import io.magistral.client.perm.PermMeta;
import io.magistral.client.pub.Callback;
import io.magistral.client.pub.PubMeta;
import io.magistral.client.sub.*;
import io.magistral.client.topics.TopicMeta;

public class Magistral implements IMagistral {
	
	private Logger logger = LoggerFactory.getLogger(Magistral.class);
	
	private String mqttBrokerAdrress = "ssl://app.magistral.io:8883";
	
	private final static String UUID_REGEX 		= "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";	
	private final static String PUB_KEY_REGEX 	= "pub-" + UUID_REGEX;
	private final static String SUB_KEY_REGEX 	= "sub-" + UUID_REGEX;
	private final static String SKEY_REGEX 		= "s-"   + UUID_REGEX;
	
//	private final static String SKEY_REGEX_EXT  = SKEY_REGEX + "-[0-9a-z]{6}";
	
	private String clientId;
	private String pubKey, subKey, secretKey;
	private boolean _ssl;
	
	private SecureRandom random = new SecureRandom();
	
	private Map<String, List<Properties>> settings = new HashMap<>();
	
	private Map<String, KafkaProducer<String, byte[]>> pM = new ConcurrentHashMap<String, KafkaProducer<String,byte[]>>();
	private Map<String, Map<String, GroupConsumer>> consumerMap = new ConcurrentHashMap<String, Map<String, GroupConsumer>>();
	
	private Map<String, MqttClient> mqttMap = new HashMap<String, MqttClient>();
	
	private TimedMap<String, ERROR> learnedErrors = new TimedMap<>();
	
	private Key aesKey;	
	private Cipher _cipher = null;
	
	private List<PermMeta> permissions = new ArrayList<>();
	
	private volatile boolean alive = true;
		
	private int randomInteger(int min, int max) {
	    int randomNum = random.nextInt((max - min) + 1) + min;
	    return randomNum;
	}
	
	public Magistral(String pubKey, String subKey, String secretKey) {
		this(pubKey, subKey, secretKey, null);
	}
		
	private class MqttExFeed extends Observable {
		public void exception(JSONObject obj) {
			setChanged();
			notifyObservers(obj);
		}
	}
	
	private MqttExFeed mqttExFeed = new MqttExFeed();
	
	public Magistral(String pubKey, String subKey, String secretKey, String cipher) {
		
		if (pubKey == null || !pubKey.matches(PUB_KEY_REGEX)) {
			throw new IllegalArgumentException("Publish key is not provided or has invalid format!");
		}
		
		if (subKey == null || !subKey.matches(SUB_KEY_REGEX)) {
			throw new IllegalArgumentException("Subscribe key is not provided or has invalid format!");
		}
		
		if (secretKey == null || !secretKey.matches(SKEY_REGEX)) {
			throw new IllegalArgumentException("Secret key is not provided or has invalid format!");
		}
		
		if (cipher != null && cipher.length() < 16) {
			throw new IllegalArgumentException("Minimal length of cipher key is 16 symbols!");
		}
		
		if (cipher != null && cipher.length() > 16) cipher = cipher.substring(0, 15);
		
		String home = System.getProperty("user.home");
		
		File magistralDir = new File(home + "/magistral");
		if (!magistralDir.exists()) magistralDir.mkdir();		

		this.pubKey = pubKey;
		this.subKey = subKey;
		this.secretKey = secretKey;
		
		this.clientId = secretKey;
		
		this._ssl = false;
		
		try {
			settings = REST.instance.getConnectionSettings(pubKey, subKey, secretKey, true);
		} catch (MagistralException me) {
			me.printStackTrace();
			System.exit(0);
		}
		
		try {
			if (cipher != null) {
				aesKey = new SecretKeySpec(cipher.getBytes(StandardCharsets.UTF_8), "AES");
				_cipher = Cipher.getInstance("AES/ECB/NoPadding");
				_cipher.init(Cipher.ENCRYPT_MODE, aesKey);
			}
		} catch(InvalidKeyException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (NoSuchPaddingException e) {
			e.printStackTrace();
		} 
		
		try {
			logger.info("Properties {}", settings.get("pub"));
			
			for (Properties properties : settings.get("pub")) {
				String token = settings.get("meta").get(0).getProperty("token");
				
				if (_ssl && properties.containsKey("bootstrap.servers.ssl")) {
					properties.setProperty("bootstrap.servers", properties.get("bootstrap.servers.ssl").toString());
					properties.remove("bootstrap.servers.ssl");
				}
				
				logger.info("Publisher created {}", properties.get("bootstrap.servers"));
				
				properties.setProperty("client.id", clientId);
				
				KafkaProducer<String, byte[]> p = new KafkaProducer<String, byte[]>(properties, new StringSerializer(), new ByteArraySerializer());						
				pM.put(token, p);
				
//				<MQTT>
				MqttClient mqtt = new MqttClient(mqttBrokerAdrress, token, new MemoryPersistence());
				final MqttConnectOptions connOpts = new MqttConnectOptions();
				connOpts.setUserName(pubKey);
				connOpts.setPassword(secretKey.toCharArray());
				connOpts.setKeepAliveInterval(30);
				
				connOpts.setWill("presence/" + pubKey + "/" + token, new byte[] { 0 }, 2, true);
				
				Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
					public void run() {
						try {
							mqtt.publish("presence/" + pubKey + "/" + token, new byte[] { 0 }, 2, true);
						} catch (MqttPersistenceException e) {
							e.printStackTrace();
						} catch (MqttException e) {
							e.printStackTrace();
						}
					}
				}));
				
				mqtt.connect(connOpts);
				
				mqtt.setCallback(new MqttCallback() {	
					
					public void messageArrived(String arg0, MqttMessage msg) throws Exception {								
						JSONObject json = (JSONObject) new JSONParser().parse(new String(msg.getPayload(), StandardCharsets.UTF_8));
						mqttExFeed.exception(json);
					}
					
					public void deliveryComplete(IMqttDeliveryToken arg0) {								
					}	
					
					public void connectionLost(Throwable arg0) {
						arg0.printStackTrace();
						
						try {
							mqtt.connect(connOpts);
						} catch (MqttSecurityException e) {
							e.printStackTrace();
						} catch (MqttException e) {
							e.printStackTrace();
						}
					}
				});
				
				mqtt.subscribe("exceptions");						
				mqtt.publish("presence/" + pubKey + "/" + token, new byte[] { 1 }, 1, true);						
//				<MQTT/>

				mqttMap.put(token, mqtt);
				
				permissions.addAll(REST.instance.permissions(pubKey, subKey, secretKey, null));
			}
				        
		} catch (MqttException e) {					
			e.printStackTrace();
		} catch (MagistralException e) {			
			e.printStackTrace();
		}
		
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				for (KafkaProducer<String, byte[]> p : pM.values()) {
					p.close();
				}
				pM.clear();
				for (String group : consumerMap.keySet()) {
					for (GroupConsumer gc : consumerMap.get(group).values()) gc.shutdown();
				}
				alive = false;
			}
		}));
	}
	
	public void setClientId(String clientId) {
		this.clientId = clientId;
	}
		
	public Future<SubMeta> subscribe(String topic, NetworkListener listener, io.magistral.client.sub.Callback callback) throws MagistralException {
		return subscribe(topic, "default", -1, listener, callback); 
	}
	
	public Future<SubMeta> subscribe(String topic, int channel, NetworkListener listener, io.magistral.client.sub.Callback callback) throws MagistralException {
		return subscribe(topic, "default", channel, listener, callback);
	}
	
	public Future<SubMeta> subscribe(String topic, String group, NetworkListener listener, io.magistral.client.sub.Callback callback) throws MagistralException {
		return subscribe(topic, group, -1, listener, callback);
	}
	
	public Future<SubMeta> subscribe(final String topic, final String group, final int channel, NetworkListener listener, io.magistral.client.sub.Callback callback) throws MagistralException {		
		
		CompletableFuture<SubMeta> future = new CompletableFuture<SubMeta>();
		try {			
			
			Map<String, GroupConsumer> cm;
			if (!consumerMap.containsKey(group)) {
				cm = new HashMap<String, GroupConsumer>(settings.get("sub").size());
				consumerMap.put(group, cm);
			}
			
			cm = consumerMap.get(group);
						
			if (cm.size() < settings.get("sub").size()) {
				
				Cipher dCipher = null;
				if (aesKey != null) {
					dCipher = Cipher.getInstance("AES/ECB/NoPadding");
					dCipher.init(Cipher.DECRYPT_MODE, aesKey);
				}
				
				final Cipher cph = (dCipher == null) ? null : dCipher;
				
				for (Properties properties : settings.get("sub")) {
					
					final String bs = properties.getProperty("bootstrap.servers");
					if (cm.containsKey(bs)) continue;
					
					GroupConsumer c = new GroupConsumer(subKey, bs, group, cph, permissions);					
					consumerMap.get(group).put(bs, c);
				}
			}
			
			SubMeta meta = new SubMeta(); 
			meta.setGroup(group);
			meta.setTopic(topic);
			meta.setChannel(channel);
			
			for (String bs : consumerMap.get(group).keySet()) {	
				GroupConsumer gc = consumerMap.get(group).get(bs);
				gc.subscribe(topic, channel, listener);
				Executors.newSingleThreadExecutor().execute(gc);
				
				if (meta.getEndPoints() == null) {
					meta.setEndPoints(new ArrayList<String>(consumerMap.get(group).keySet()));
				}
				meta.getEndPoints().add(bs);
			}			

			logger.debug("Connected to {}", meta.getEndPoints());
			listener.connected(topic);
			
			future.complete(meta);
			if (callback != null) callback.success(meta);
		} catch (Exception e) {
			listener.error(new MagistralException(e));			
			if (callback != null) callback.error(e);
			future.completeExceptionally(e);
		}		
		return future;
	}

	private class SingleMqttExFeedbacker implements Observer {		
		
		private TimedMap<String, SimpleEntry<CompletableFuture<PubMeta>, Callback>> map = new TimedMap<>();
		
		public SingleMqttExFeedbacker() {
			mqttExFeed.addObserver(this);
			
			map.addEvictionListener(new TimedMapEvictionListener() {
				@Override
				public void evicted(Object eo) {
					
					@SuppressWarnings("unchecked")
					SimpleEntry<String, Object> evicted = (SimpleEntry<String, Object>) eo;
					
					String[] args = evicted.getKey().split("\\^");
										
					PubMeta meta = PrimitiveFactory.getInstance().createPubMeta(args[0], Integer.parseInt(args[1]));
					
					@SuppressWarnings("rawtypes")
					ExpiredObject expObj = (ExpiredObject) evicted.getValue();
					@SuppressWarnings("unchecked")
					SimpleEntry<CompletableFuture<PubMeta>, Callback> val = (SimpleEntry<CompletableFuture<PubMeta>, Callback>) expObj.getValue();
					
					if (val.getValue() != null) val.getValue().success(meta);
					val.getKey().complete(meta);
				}
			});
		}
		
		public void completeWithin(CompletableFuture<PubMeta> completableFuture, Callback callback, String topic, int partition, long offset, long timeout) {
			map.put(topic + "^" + partition + "^" + offset, new SimpleEntry<CompletableFuture<PubMeta>, Callback>(completableFuture, callback), timeout);
		}

		@Override
		public void update(Observable o, Object arg) {
			JSONObject json = (JSONObject) arg;
			
			String t = json.get("topic").toString();
			long channel = (long) json.get("channel");
			long off = (long) json.get("offset");
			long errorCode = (long) json.get("code");
			
			if (!learnedErrors.containsKey(t + "^" + channel)) {

				learnedErrors.put(t + "^" + channel, ERROR.getByCode((int)errorCode), 20000);
				
				for (String key : map.keySet()) {
					if (!key.startsWith(t + "^" + channel)) continue;
					
					MagistralException exc = new MagistralException(ERROR.getByCode((int)errorCode).message());

					SimpleEntry<CompletableFuture<PubMeta>, Callback> se = map.remove(key);
					if (se == null) return;
					
					if (se.getValue() != null) se.getValue().error(exc);
					se.getKey().completeExceptionally(exc);
				}
			} 
			
			if (map.containsKey(t + "^" + channel + "^" + off)) {
				MagistralException exc = new MagistralException(ERROR.getByCode((int)errorCode).message());

				SimpleEntry<CompletableFuture<PubMeta>, Callback> se = map.remove(t + "^" + channel + "^" + off);
				if (se == null) return;
				
				if (se.getValue() != null) se.getValue().error(exc);
				se.getKey().completeExceptionally(exc);
			}
		}
	}
	
	private SingleMqttExFeedbacker smef = new SingleMqttExFeedbacker();
	
	public Future<PubMeta> publish(String topic, byte[] body, io.magistral.client.pub.Callback callback) throws MagistralException {
		
		try {			
			String token = pM.size() == 1 ? pM.keySet().iterator().next() : new ArrayList<String>(pM.keySet()).get(randomInteger(0, pM.size() - 1));			
			KafkaProducer<String, byte[]> p = pM.get(token);
			
			String realTopic = pubKey + "." + topic;
			byte[] encrypted = body;			
			
			if (_cipher != null) {
				int pad = 16 - body.length % 16;
				byte[] padded = new byte[body.length + pad];
				
				System.arraycopy(body, 0, padded, 0, body.length);
				for (int i=body.length;i<body.length + pad; i++) {
					padded[i] = 0x00;
				}
				
				encrypted = Base64.getEncoder().encode(_cipher.doFinal(padded));
			}
			
			ProducerRecord<String, byte[]> msg = new ProducerRecord<String, byte[]>(realTopic, secretKey + "-" + token, encrypted);
			
			CompletableFuture<PubMeta> completableFuture = new CompletableFuture<PubMeta>();
			
			p.send(msg, new org.apache.kafka.clients.producer.Callback() {
				
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					
					String topic = metadata.topic().replaceAll("pub-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\.", "");
					long offset = metadata.offset();
					int partition = metadata.partition();
					
					if (exception != null) {
						exception.printStackTrace();
						
						if (callback != null) callback.error(new MagistralException(exception));
						completableFuture.completeExceptionally(exception);
						return;
					}
					
					if (learnedErrors.containsKey(topic + "^" + partition)) {
						MagistralException magEx = new MagistralException(learnedErrors.get(topic + "^" + partition).message());
						if (callback != null) callback.error(magEx);
						completableFuture.completeExceptionally(magEx);
						return;
					}
					
					smef.completeWithin(completableFuture, callback, topic, partition, offset, 2000);
				}
			});
			
			return completableFuture;		
		} catch (IllegalBlockSizeException e) {
			throw new MagistralException(e);
		} catch (BadPaddingException e) {
			throw new MagistralException(e);
		}
	}
	
	public Future<PubMeta> publish(String topic, int channel, byte[] body, io.magistral.client.pub.Callback callback) throws MagistralException {		
		try {
			if (pM.size() == 0) {				
				throw new MagistralException("Unnable to publish message - client is not connected to the service");
			}
			
			CompletableFuture<PubMeta> completableFuture = new CompletableFuture<PubMeta>();
			
			if (learnedErrors.containsKey(topic + "^" + channel)) {
								
				MagistralException magEx = new MagistralException(learnedErrors.get(topic + "^" + channel).message());
				if (callback != null) callback.error(magEx);
				completableFuture.completeExceptionally(magEx);
				return completableFuture;
			}
			
			String token = pM.size() == 1 ? pM.keySet().iterator().next() : new ArrayList<String>(pM.keySet()).get(randomInteger(0, pM.size() - 1));			
			KafkaProducer<String, byte[]> p = pM.get(token);
			
			String realTopic = pubKey + "." + topic;		
			byte[] encrypted = body;			
			
			if (_cipher != null) {
				int pad = 16 - body.length % 16;
				byte[] padded = new byte[body.length + pad];
				
				System.arraycopy(body, 0, padded, 0, body.length);
				for (int i=body.length;i<body.length + pad; i++) {
					padded[i] = 0x00;
				}
				
				encrypted = Base64.getEncoder().encode(_cipher.doFinal(padded));
			}
	        
			ProducerRecord<String, byte[]> msg = new ProducerRecord<String, byte[]>(realTopic, channel, secretKey + "-" + token, encrypted);		
			p.send(msg, new org.apache.kafka.clients.producer.Callback() {
				
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					
					if (exception != null) {
						exception.printStackTrace();
						callback.error(new MagistralException(exception));
						completableFuture.completeExceptionally(exception);
						return;
					}
					
					String topic = metadata.topic().replaceAll("pub-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\.", "");
					long offset = metadata.offset();
					int partition = metadata.partition();
										
					smef.completeWithin(completableFuture, callback, topic, partition, offset, 2000);
				}
			});
			
			return completableFuture;
		} catch (IllegalBlockSizeException e) {
			throw new MagistralException(e);
		} catch (BadPaddingException e) {
			throw new MagistralException(e);
		}
	}

	@Override
	public Future<List<PermMeta>> permissions() throws MagistralException {		
		CompletableFuture<List<PermMeta>> future = new CompletableFuture<List<PermMeta>>();
		List<PermMeta> perms = REST.instance.permissions(pubKey, subKey, secretKey, null);		
		future.complete(perms);	
		return future;
	}

	@Override
	public Future<List<PermMeta>> permissions(String topic) throws MagistralException {
		return permissions(topic, null);
	}

	@Override
	public Future<List<PermMeta>> grant(String user, String topic, boolean read, boolean write) throws MagistralException {	
		return grant(user, topic, read, write, null);
	}

	@Override
	public Future<List<PermMeta>> grant(String user, String topic, boolean read, boolean write, int ttl) throws MagistralException {		
		return grant(user, topic, read, write, ttl, null);
	}

	@Override
	public Future<List<PermMeta>> grant(String user, String topic, int channel, boolean read, boolean write) throws MagistralException {		
		return grant(user, topic, channel, read, write, null);
	}

	@Override
	public Future<List<PermMeta>> grant(String user, String topic, int channel, boolean read, boolean write, int ttl) throws MagistralException {
		return grant(user, topic, channel, read, write, ttl, null);
	}
	
	@Override
	public Future<List<PermMeta>> grant(String user, String topic, boolean read, boolean write, io.magistral.client.perm.Callback callback) throws MagistralException {
		try {
			CompletableFuture<List<PermMeta>> future = new CompletableFuture<List<PermMeta>>();
			List<PermMeta> perms = REST.instance.grant(pubKey, subKey, secretKey, user, topic, null, null, read, write);
			if (callback != null) callback.success(perms);
			future.complete(perms);	
			return future;
		} catch (MagistralException e) {
			if (callback != null) callback.error(e);
			throw new MagistralException(e);
		}
	}

	@Override
	public Future<List<PermMeta>> grant(String user, String topic, boolean read, boolean write, int ttl, io.magistral.client.perm.Callback callback) throws MagistralException {
		try {
			CompletableFuture<List<PermMeta>> future = new CompletableFuture<List<PermMeta>>();
			List<PermMeta> perms = REST.instance.grant(pubKey, subKey, secretKey, user, topic, null, ttl, read, write);
			if (callback != null) callback.success(perms);
			future.complete(perms);	
			return future;
		} catch (MagistralException e) {
			if (callback != null) callback.error(e);
			throw new MagistralException(e);
		}
	}

	@Override
	public Future<List<PermMeta>> grant(String user, String topic, int channel, boolean read, boolean write, io.magistral.client.perm.Callback callback) throws MagistralException {
		try {
			CompletableFuture<List<PermMeta>> future = new CompletableFuture<List<PermMeta>>();
			List<PermMeta> perms = REST.instance.grant(pubKey, subKey, secretKey, user, topic, channel, null, read, write);
			if (callback != null) callback.success(perms);
			future.complete(perms);	
			return future;
		} catch (MagistralException e) {
			if (callback != null) callback.error(e);
			throw new MagistralException(e);
		}
	}

	@Override
	public Future<List<PermMeta>> grant(String user, String topic, int channel, boolean read, boolean write, int ttl, io.magistral.client.perm.Callback callback) throws MagistralException {
		try {
			CompletableFuture<List<PermMeta>> future = new CompletableFuture<List<PermMeta>>();
			List<PermMeta> perms = REST.instance.grant(pubKey, subKey, secretKey, user, topic, channel, ttl, read, write);
			if (callback != null) callback.success(perms);
			future.complete(perms);	
			return future;
		} catch (MagistralException e) {
			if (callback != null) callback.error(e);
			throw new MagistralException(e);
		}
	}
	
	@Override
	public Future<List<PermMeta>> revoke(String user, String topic) throws MagistralException {		
		CompletableFuture<List<PermMeta>> future = new CompletableFuture<List<PermMeta>>();
		List<PermMeta> perms =  REST.instance.revoke(pubKey, subKey, secretKey, user, topic, null);
		future.complete(perms);	
		return future;
	}

	@Override
	public Future<List<PermMeta>> revoke(String user, String topic, int channel) throws MagistralException {
		CompletableFuture<List<PermMeta>> future = new CompletableFuture<List<PermMeta>>();
		List<PermMeta> perms =  REST.instance.revoke(pubKey, subKey, secretKey, user, topic, channel);
		future.complete(perms);	
		return future;
	}
	
	@Override
	public Future<List<PermMeta>> revoke(String user, String topic, io.magistral.client.perm.Callback callback) throws MagistralException {
		CompletableFuture<List<PermMeta>> future = new CompletableFuture<List<PermMeta>>();
		List<PermMeta> perms = REST.instance.revoke(pubKey, subKey, secretKey, user, topic, null);
		future.complete(perms);	
		return future;
	}

	@Override
	public Future<List<PermMeta>> revoke(String user, String topic, int channel, io.magistral.client.perm.Callback callback) throws MagistralException {
		CompletableFuture<List<PermMeta>> future = new CompletableFuture<List<PermMeta>>();
		List<PermMeta> perms = REST.instance.revoke(pubKey, subKey, secretKey, user, topic, channel);
		future.complete(perms);	
		return future;
	}
	
	@Override
	public Future<SubMeta> unsubscribe(String topic) throws MagistralException {
		CompletableFuture<SubMeta> future = new CompletableFuture<SubMeta>();
		SubMeta subMeta = new SubMeta();
		
		for (String groupName : consumerMap.keySet()) {
			
			for (String conString : consumerMap.get(groupName).keySet()) {
				GroupConsumer gc = consumerMap.get(groupName).get(conString);
				gc.unsubscribe(subKey + "." + topic);
				
				subMeta.setTopic(topic);
				List<String> aL = new ArrayList<>(1); aL.add(conString);
				subMeta.setEndPoints(aL);
				subMeta.setGroup(groupName);
			}
		}
		
		future.complete(subMeta);	
		return future;
	}

	@Override
	public Future<SubMeta> unsubscribe(String topic, int channel) throws MagistralException {
		CompletableFuture<SubMeta> future = new CompletableFuture<SubMeta>();
		SubMeta subMeta = new SubMeta();
		
		for (String groupName : consumerMap.keySet()) {
			
			for (String conString : consumerMap.get(groupName).keySet()) {
				GroupConsumer gc = consumerMap.get(groupName).get(conString);
				gc.unsubscribe(subKey + "." + topic, channel);
				
				subMeta.setTopic(topic);
				List<String> aL = new ArrayList<>(1); aL.add(conString);
				subMeta.setEndPoints(aL);
				subMeta.setGroup(groupName);
			}
		}
		
		future.complete(subMeta);	
		return future;
	}

	@Override
	public Future<SubMeta> subscribe(String topic, NetworkListener listener) throws MagistralException {
		return subscribe(topic, listener, null);
	}

	@Override
	public Future<SubMeta> subscribe(String topic, int channel, NetworkListener listener) throws MagistralException {
		return subscribe(topic, channel, listener, null);
	}

	@Override
	public Future<SubMeta> subscribe(String topic, String group, NetworkListener listener) throws MagistralException {
		return subscribe(topic, group, listener, null);
	}

	@Override
	public Future<SubMeta> subscribe(String topic, String group, int channel, NetworkListener listener) throws MagistralException {
		return subscribe(topic, group, channel, listener, null);
	}

	@Override
	public Future<PubMeta> publish(String topic, byte[] msg) throws MagistralException {
		return publish(topic, msg, null);
	}

	@Override
	public Future<PubMeta> publish(String topic, int channel, byte[] msg) throws MagistralException {
		return publish(topic, channel, msg, null);
	}

	@Override
	public Future<List<TopicMeta>> topics(io.magistral.client.topics.Callback callback) throws MagistralException {
		CompletableFuture<List<TopicMeta>> future = new CompletableFuture<>();		
		List<TopicMeta> res = new ArrayList<TopicMeta>();
		
		try {
			Future<List<PermMeta>> perms = permissions();
			for (PermMeta pm : perms.get()) {
				TopicMeta tm = new TopicMeta();
				tm.setTopicName(pm.topic());
				tm.setСhannels(pm.channels());
				res.add(tm);
			}
			
			if (callback != null) callback.success(res);
			future.complete(res);
			return future;
		} catch (Exception e) {
			MagistralException ex = new MagistralException(e);
			if (callback != null) callback.error(ex);
			future.completeExceptionally(ex);
			return future;
		}
	}

	@Override
	public Future<TopicMeta> topic(String topic) throws MagistralException {
		return topic(topic, null);
	}

	@Override
	public Future<TopicMeta> topic(String topic, io.magistral.client.topics.Callback callback) throws MagistralException {
		CompletableFuture<TopicMeta> future = new CompletableFuture<>();		
		TopicMeta res = new TopicMeta();
		res.setTopicName(topic);
		res.setСhannels(new HashSet<>());
		
		try {
			Future<List<PermMeta>> perms = permissions(topic);
			
			for (PermMeta pm : perms.get()) {
				if (!pm.topic().equals(topic)) continue;				
				res.getСhannels().addAll(pm.channels());
			}
			
			if (callback != null) {
				List<TopicMeta> tml = new ArrayList<TopicMeta>(1);
				tml.add(res);
				callback.success(tml);
			}
			future.complete(res);
			return future;
		} catch (Exception e) {
			MagistralException ex = new MagistralException(e);
			if (callback != null) callback.error(ex);
			future.completeExceptionally(ex);
			return future;
		}
	}

	@Override
	public Future<List<TopicMeta>> topics() throws MagistralException {
		return topics(null);
	}

	@Override
	public Future<History> history(String topic, int channel, int count) throws MagistralException {
		return history(topic, channel, count, null);
	}

	@Override
	public Future<History> history(String topic, int channel, long start, int count) throws MagistralException {
		return history(topic, channel, start, count, null);
	}

	@Override
	public Future<History> history(String topic, int channel, long start, long end) throws MagistralException {
		return history(topic, channel, start, end, null);
	}

	@Override
	public Future<History> history(String topic, int channel, int count, io.magistral.client.data.Callback callback) throws MagistralException {
		CompletableFuture<History> future = new CompletableFuture<>();
		try {
			
			Cipher dCipher = null;
			if (aesKey != null) {
				dCipher = Cipher.getInstance("AES/ECB/NoPadding");
				dCipher.init(Cipher.DECRYPT_MODE, aesKey);
			}
			
			List<Message> messages = new ArrayList<>(count);		
			for (Properties properties : settings.get("sub")) {
				String bs = properties.get("bootstrap.servers").toString();
				MagistralConsumer consumer = new MagistralConsumer(pubKey, subKey, secretKey, bs, dCipher);				
				messages.addAll(consumer.history(topic, channel, count));			
			}
			
			History h = new History();
			h.setMessages(messages);
			
			future.complete(h);
			if (callback != null) callback.success(h);			
		} catch (Exception e) {
			e.printStackTrace();
			if (callback != null) callback.error(new MagistralException(e));
			future.completeExceptionally(e);			
		}
		return future;
	}

	@Override
	public Future<History> history(String topic, int channel, long start, int count, io.magistral.client.data.Callback callback) throws MagistralException {
		
		CompletableFuture<History> future = new CompletableFuture<>();
		try {
			Cipher dCipher = null;
			if (aesKey != null) {
				dCipher = Cipher.getInstance("AES/ECB/NoPadding");
				dCipher.init(Cipher.DECRYPT_MODE, aesKey);
			}
			
			List<Message> messages = new ArrayList<>(count);		
			for (Properties properties : settings.get("sub")) {
				String bs = properties.get("bootstrap.servers").toString();
				MagistralConsumer consumer = new MagistralConsumer(pubKey, subKey, secretKey, bs, dCipher);				
				messages.addAll(consumer.history(topic, channel, start, count));			
			}
			
			History h = new History();
			h.setMessages(messages);
			future.complete(h);
			if (callback != null) callback.success(h);			
		} catch (Exception e) {
			e.printStackTrace();
			if (callback != null) callback.error(new MagistralException(e));
			future.completeExceptionally(e);			
		}
		return future;
	}

	@Override
	public Future<History> history(String topic, int channel, long start, long end, io.magistral.client.data.Callback callback) throws MagistralException {
		CompletableFuture<History> future = new CompletableFuture<>();

		if (start >= end) {
			MagistralException e = new MagistralException("Start time should not exceed end time");
			if (callback != null) callback.error(new MagistralException(e));
			future.completeExceptionally(e);
			return future;
		}
		
		if (end - start > 86400000) {
			end = start + 86400000; 
		}
		
		try {
			Cipher dCipher = Cipher.getInstance("AES/ECB/NoPadding");
			dCipher.init(Cipher.DECRYPT_MODE, aesKey);
			
			List<Message> messages = new ArrayList<>();		
			for (Properties properties : settings.get("sub")) {
				String bs = properties.get("bootstrap.servers").toString();
				MagistralConsumer consumer = new MagistralConsumer(pubKey, subKey, secretKey, bs, dCipher);				
				messages.addAll(consumer.historyForTimePeriod(topic, channel, start, end));			
			}
			
			History h = new History();
			h.setMessages(messages);
			future.complete(h);
			if (callback != null) callback.success(h);			
		} catch (Exception e) {
			e.printStackTrace();
			if (callback != null) callback.error(new MagistralException(e));
			future.completeExceptionally(e);			
		}
		return future;
	}

	@Override
	public Future<List<PermMeta>> permissions(io.magistral.client.perm.Callback callback) throws MagistralException {		
		CompletableFuture<List<PermMeta>> future = new CompletableFuture<List<PermMeta>>();
		List<PermMeta> perms = REST.instance.permissions(pubKey, subKey, secretKey, null);		
		future.complete(perms);
		if (callback != null) callback.success(perms);		
		return future;
	}

	@Override
	public Future<List<PermMeta>> permissions(String topic, io.magistral.client.perm.Callback callback) throws MagistralException {
		CompletableFuture<List<PermMeta>> future = new CompletableFuture<List<PermMeta>>();
		List<PermMeta> perms = REST.instance.permissions(pubKey, subKey, secretKey, topic);		
		future.complete(perms);
		if (callback != null) callback.success(perms);
		return future;
	}

	@Override
	public void close() {
		System.exit(0);
	}
}
