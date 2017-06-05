package io.magistral.client.java.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.core.util.MultivaluedMapImpl;

import io.magistral.client.Configs;
import io.magistral.client.MagistralException;
import io.magistral.client.perm.PermMeta;

public enum REST {
	
	instance;
	
	private REST() {}	
	
	public void setCredentials(String user, String password) {
		RESTcommunicator.instance.setCredentials(user, password);
	}

	public Map<String, List<Properties>> getConnectionSettings(String host, int port, String pubKey, String subKey, String secretKey, boolean ssl) throws MagistralException {
		
		Map<String, List<Properties>> out = new HashMap<>(3);

		try {		
			String home = System.getProperty("user.home");			
			File magistralDir = new File(home + "/magistral");

			MultivaluedMap<String, String> parameters = new MultivaluedMapImpl();
			parameters.add("pubKey", pubKey);
			parameters.add("subKey", subKey);
			parameters.add("secretKey", secretKey);
			
			RESTcommunicator.instance.setCredentials(pubKey + "|" + subKey, secretKey);
			
			SimpleEntry<Integer, String> response = 
					RESTcommunicator.instance.get("https://" + host + ":" + port + "/api/magistral/net/connectionPoints", 
							parameters, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON);
			
			if (response.getKey() != 200) {
				if (response.getKey() == 401) 
					throw new MagistralException("Unnable to authorize user by provided keys");
				else
					throw new MagistralException(response.getValue());
			}
						
			JSONParser parser = new JSONParser();
			JSONArray arr = (JSONArray)parser.parse(response.getValue());
			
			for (int i=0;i<arr.size();i++) {
				
				JSONObject jo = (JSONObject) arr.get(i);
				
				String p = ssl ? jo.get("producer-ssl").toString() : jo.get("producer").toString();
				String c = ssl ? jo.get("consumer-ssl").toString() : jo.get("consumer").toString();
				
				String token = jo.get("token").toString();
				
				if (i == 0) {
					String ts = jo.get("ts").toString();
					String ks = jo.get("ks").toString();
					
					for (File file: magistralDir.listFiles()) if (!file.isDirectory()) file.delete();
					
					byte[] bts = Base64.getDecoder().decode(ts);
					byte[] bks = Base64.getDecoder().decode(ks);
					
					File dir = new File(magistralDir.getAbsolutePath() + "/" + token);
					dir.mkdirs(); dir.deleteOnExit();
					
					File tsFile = new File(dir.getAbsolutePath() + "/ts");
					File ksFile = new File(dir.getAbsolutePath() + "/ks");
					
					tsFile.deleteOnExit();
					ksFile.deleteOnExit();
					
					FileOutputStream fos = new FileOutputStream(tsFile.getAbsolutePath());
					fos.write(bts);
					fos.close();
					
					fos = new FileOutputStream(ksFile.getAbsolutePath());
					fos.write(bks);
					fos.close();
				}
				
				if (!out.containsKey("pub")) out.put("pub", new ArrayList<Properties>(arr.size()));
				if (!out.containsKey("sub")) out.put("sub", new ArrayList<Properties>(arr.size()));
				
				if (!out.containsKey("meta")) out.put("meta", new ArrayList<Properties>(1));
				
				Properties pp = Configs.producer();
				pp.put("bootstrap.servers", p);
				if (ssl) {
					pp.put("security.protocol", "SSL");
					pp.put("ssl.truststore.location", magistralDir.getAbsolutePath() + "/" + token + "/ts");
					pp.put("ssl.truststore.password", "magistral");
					
					pp.put("ssl.keystore.location",  magistralDir.getAbsolutePath() + "/" + token + "/ks");
					pp.put("ssl.keystore.password", "magistral");
					pp.put("ssl.key.password", "magistral");
				}
				
				
				if (jo.containsKey("producer-ssl")) pp.put("bootstrap.servers.ssl", jo.get("producer-ssl").toString());
				
				Properties sp = new Properties();
				sp.put("bootstrap.servers", c);
				
				if (jo.containsKey("consumer-ssl")) sp.put("bootstrap.servers.ssl", jo.get("consumer-ssl").toString());
				
				Properties pt = new Properties();
				pt.put("token", token);
				
				out.get("pub").add(pp);
				out.get("sub").add(sp);
				out.get("meta").add(pt);
			}
			
		} catch (ParseException e) {
			e.printStackTrace();
			throw new MagistralException(e.getMessage());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new MagistralException(e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			throw new MagistralException(e.getMessage());
		} 
		
		return out;
	}
	
	private List<PermMeta> JSONtoPermissions(String jsonString) throws ParseException {
		Map<String, PermMeta> permissions = new HashMap<>();
		JSONParser parser = new JSONParser();
		
		Object o = parser.parse(jsonString);
		if (o instanceof JSONObject) {
			
			JSONObject json = (JSONObject) o;
			
			Object pmsobj = json.get("permission");
			if (pmsobj instanceof JSONArray) {					
				JSONArray parr = (JSONArray)pmsobj;
				for (int i=0;i<parr.size();i++) {
					PermMeta permData = convertToPermData((JSONObject) parr.get(i));
					
					if (permissions.containsKey(permData.topic())) {
						for (int channel : permData.channels()) {
							permissions.get(permData.topic()).addPermission(channel, permData.readable(channel), permData.writable(channel));
						}
					} else {
						permissions.put(permData.topic(), permData);
					}
				}
			} else if (pmsobj instanceof JSONObject) {
				PermMeta permData = convertToPermData((JSONObject) pmsobj);
				
				if (permissions.containsKey(permData.topic())) {
					for (int channel : permData.channels()) {
						permissions.get(permData.topic()).addPermission(channel, permData.readable(channel), permData.writable(channel));
					}
				} else {
					permissions.put(permData.topic(), permData);
				}
			}
		}
		
		return new ArrayList<>(permissions.values());
	}
	
	public synchronized List<PermMeta> userPermissions(String host, int port, String pubKey, String subKey, String authKey, String userName) throws MagistralException {
		
		try {			
			MultivaluedMap<String, String> parameters = new MultivaluedMapImpl();
			if (userName != null) parameters.add("userName", userName);
						
			SimpleEntry<Integer, String> response = RESTcommunicator.instance.get("https://" + host + ":" + port + "/api/magistral/net/user_permissions", parameters);
			if (response.getKey() != 200) {
				if (response.getKey() == 401) 
					throw new MagistralException("Unnable to authorize user by provided keys");
				else
					throw new MagistralException(response.getValue());
			}
			
			return JSONtoPermissions(response.getValue());
		} catch (ParseException e) {
			throw new MagistralException(e.getMessage());
		}
	}
	
	public synchronized List<PermMeta> permissions(String host, int port, String pubKey, String subKey, String authKey, String topic) throws MagistralException {		
		Map<String, PermMeta> permissions = new HashMap<>();
		
		try {
			
			MultivaluedMap<String, String> parameters = new MultivaluedMapImpl();
			if (pubKey != null) parameters.add("pubKey", pubKey);
			if (subKey != null) parameters.add("subKey", subKey);
			if (authKey != null) parameters.add("authKey", authKey);
			if (topic != null) parameters.add("topic", topic);
						
			SimpleEntry<Integer, String> response = RESTcommunicator.instance.get("https://" + host + ":" + port + "/api/magistral/net/permissions", parameters);
			if (response.getKey() != 200) {
				if (response.getKey() == 401) 
					throw new MagistralException("Unnable to authorize user by provided keys");
				else
					throw new MagistralException(response.getValue());
			}
			
			
			JSONParser parser = new JSONParser();
			
			Object o = parser.parse(response.getValue());
			if (o instanceof JSONObject) {
				
				JSONObject json = (JSONObject) o;
				Object pmsobj = json.get("permission");
				
				if (pmsobj instanceof JSONArray) {					
					JSONArray parr = (JSONArray)pmsobj;
					for (int i=0;i<parr.size();i++) {
						PermMeta permData = convertToPermData((JSONObject) parr.get(i));
						
						if (permissions.containsKey(permData.topic())) {
							for (int channel : permData.channels()) {
								permissions.get(permData.topic()).addPermission(channel, permData.readable(channel), permData.writable(channel));
							}
						} else {
							permissions.put(permData.topic(), permData);
						}
					}
				} else if (pmsobj instanceof JSONObject) {
					PermMeta permData = convertToPermData((JSONObject) pmsobj);
					
					if (permissions.containsKey(permData.topic())) {
						for (int channel : permData.channels()) {
							permissions.get(permData.topic()).addPermission(channel, permData.readable(channel), permData.writable(channel));
						}
					} else {
						permissions.put(permData.topic(), permData);
					}
				}
			} 
		} catch (ParseException e) {
			throw new MagistralException(e.getMessage());
		}		
		return new ArrayList<>(permissions.values());
	}
	
	private PermMeta convertToPermData(JSONObject permJsonObj) {
		PermMeta p = new PermMeta();						
		p.setTopic(permJsonObj.get("topic").toString());
		
		Map<Integer, SimpleEntry<Boolean, Boolean>> pmap;		
		Object chObj = permJsonObj.get("channels");
		
		boolean readable = permJsonObj.get("read").toString().equals("true");
		boolean writable = permJsonObj.get("write").toString().equals("true");
		
		if (chObj instanceof JSONArray) {					
			JSONArray charr = (JSONArray)chObj;
			
			pmap = new HashMap<>(charr.size());
			for (int i=0;i<charr.size();i++) {			
				String sch = charr.get(i).toString();
				if (!sch.matches("\\d+")) continue;
				int chNo = Integer.parseInt(sch);				
				pmap.put(chNo, new SimpleEntry<>(readable, writable));
			}
			p.addPermissions(pmap);
		} else {
			pmap = new HashMap<>(1);

			String sch = chObj.toString();
			if (sch.matches("\\d+")) {
				int chNo = Integer.parseInt(sch);				
				pmap.put(chNo, new SimpleEntry<>(readable, writable));
			}
			p.addPermissions(pmap);
		}
		return p;
	}

	public synchronized List<PermMeta> grant(String host, int port, String pubKey, String subKey, String secretKey, String user, String topic, Integer channel, Integer ttl, boolean read, boolean write) throws MagistralException {		
		try {
			
			MultivaluedMap<String, String> parameters = new MultivaluedMapImpl();
			
			if (topic != null) parameters.add("topic", topic);		
			if (user != null) parameters.add("user", user);
			if (channel != null) parameters.add("channel", channel + "");
			if (ttl != null) parameters.add("ttl", ttl + "");
			
			parameters.add("read", read + "");
			parameters.add("write", write + "");

			SimpleEntry<Integer, String> result = RESTcommunicator.instance.put("https://" + host + ":" + port + "/api/magistral/net/grant", parameters);
			
			if (result.getKey() == Status.OK.getStatusCode() || result.getKey() == Status.CREATED.getStatusCode()) 
				return userPermissions(host, port, pubKey, subKey, secretKey, user);
			else 
				throw new MagistralException(result.getValue());
			
		} catch (Exception e) {
			throw new MagistralException(e); 
		}		
	}
	
	public synchronized List<PermMeta> revoke(String host, int port, String pubKey, String subKey, String secretKey, String user, String topic, Integer channel) throws MagistralException {		
		try {			
			MultivaluedMap<String, String> parameters = new MultivaluedMapImpl();
			
			if (topic != null) parameters.add("topic", topic);		
			if (user != null) parameters.add("user", user);
			if (channel != null) parameters.add("channel", channel + "");

			SimpleEntry<Integer, String> result = RESTcommunicator.instance.delete("https://" + host + ":" + port + "/api/magistral/net/revoke", parameters);
						
			if (result.getKey() == Status.OK.getStatusCode()) 
				return userPermissions(host, port, pubKey, subKey, secretKey, user);
			else 
				throw new MagistralException(result.getValue());
		} catch (Exception e) {
			throw new MagistralException(e); 
		}		
	}

}