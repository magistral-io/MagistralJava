package io.magistral.client.java.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import io.magistral.client.Configs;
import io.magistral.client.MagistralException;
import io.magistral.client.perm.PermMeta;

public class MagistralRESTi {
	
	private static String auth(String pubKey, String subKey, String secretKey) {
		String user = pubKey + "|" + subKey;
		String authStr = user + ":" + secretKey;
		
		String auth = Base64.getEncoder().encodeToString(authStr.getBytes(StandardCharsets.UTF_8));
		return auth;
	}

	public static Map<String, List<Properties>> getConnectionSettings(String pubKey, String subKey, String secretKey, boolean ssl) throws MagistralException {
		Map<String, List<Properties>> out = new HashMap<>(3);

		try {		
			String home = System.getProperty("user.home");			
			File magistralDir = new File(home + "/magistral");
			
			Map<String, String> reqProps = new HashMap<String, String>(1);
			reqProps.put("Accept", "text");
			reqProps.put("Authorization", "Basic " + auth(pubKey, subKey, secretKey));
			
			Map<String, Object> parameters = new HashMap<String, Object>(2);
			parameters.put("pubKey", pubKey);
			parameters.put("subKey", subKey);
			parameters.put("secretKey", secretKey);
			
			String host = "app.magistral.io";
			
			String result = "";
			try {
				result = RESTcommunicator.call("https", host, "api/magistral/net/connectionPoints", 
						"GET", reqProps, parameters, "Unnable to get connection points from [" + host + "]");				
			} catch (Exception e) {
				String ex = e.getMessage();
				if (ex.contains("HTTP") && ex.contains("401")) 
					throw new MagistralException("Unnable to authorize user by provided keys");
				else
					throw new MagistralException(ex);
			}
			
						
			JSONParser parser = new JSONParser();
			JSONArray arr = (JSONArray)parser.parse(result);
			
			for (int i=0;i<arr.size();i++) {
				
				JSONObject jo = (JSONObject) arr.get(i);
				
				String p = ssl ? jo.get("producer-ssl").toString() : jo.get("producer").toString();
				String c = ssl ? jo.get("consumer-ssl").toString() : jo.get("consumer").toString();
				
				if (i == 0) {
					String ts = jo.get("ts").toString();
					String ks = jo.get("ks").toString();
					
					for (File file: magistralDir.listFiles()) if (!file.isDirectory()) file.delete();
					
					byte[] bts = Base64.getDecoder().decode(ts);
					byte[] bks = Base64.getDecoder().decode(ks);
					
					FileOutputStream fos = new FileOutputStream(magistralDir.getAbsolutePath() + "/ts");
					fos.write(bts);
					fos.close();
					
					fos = new FileOutputStream(magistralDir.getAbsolutePath() + "/ks");
					fos.write(bks);
					fos.close();
				}
				
				String token = jo.get("token").toString();
				
				if (!out.containsKey("pub")) out.put("pub", new ArrayList<Properties>(arr.size()));
				if (!out.containsKey("sub")) out.put("sub", new ArrayList<Properties>(arr.size()));
				
				if (!out.containsKey("meta")) out.put("meta", new ArrayList<Properties>(1));
				
				Properties pp = Configs.producer();
				pp.put("bootstrap.servers", p);
				if (ssl) {
					pp.put("security.protocol", "SSL");
					pp.put("ssl.truststore.location", magistralDir.getAbsolutePath() + "/ts");
					pp.put("ssl.truststore.password", "magistral");
					
					pp.put("ssl.keystore.location",  magistralDir.getAbsolutePath() + "/ks");
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
			throw new MagistralException(e.getMessage());
		} catch (FileNotFoundException e) {
			throw new MagistralException(e.getMessage());
		} catch (IOException e) {
			throw new MagistralException(e.getMessage());
		} 
		
		return out;
	}
	
	public static List<PermMeta> userPermissions(String pubKey, String subKey, String authKey, String userName) throws MagistralException {
		Map<String, PermMeta> permissions = new HashMap<>();
		
		try {
			Map<String, String> reqProps = new HashMap<String, String>(1);
			reqProps.put("Accept", "application/json");
			reqProps.put("Authorization", "Basic " + auth(pubKey, subKey, authKey));
			
			Map<String, Object> parameters = new HashMap<String, Object>(4);
			if (pubKey != null) parameters.put("pubKey", pubKey);
			if (subKey != null) parameters.put("subKey", subKey);
			if (authKey != null) parameters.put("authKey", authKey);
			if (userName != null) parameters.put("userName", userName);
			
			String host = "app.magistral.io";
			String result = RESTcommunicator.call("https", host, "api/magistral/net/user_permissions", "GET", reqProps, parameters, "Unnable to get user permissions from [" + host + "]");
			
//			System.out.println("PERMISSIONS :: " + result);
			
			JSONParser parser = new JSONParser();
			
			Object o = parser.parse(result);
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
//		System.out.println("RETURN PERMS :: " + permissions.values());
		return new ArrayList<>(permissions.values());
	}
	
	public static List<PermMeta> permissions(String pubKey, String subKey, String authKey, String topic) throws MagistralException {		
		Map<String, PermMeta> permissions = new HashMap<>();
		
		try {
			Map<String, String> reqProps = new HashMap<String, String>(1);
			reqProps.put("Accept", "application/json");
			reqProps.put("Authorization", "Basic " + auth(pubKey, subKey, authKey));
			
			Map<String, Object> parameters = new HashMap<String, Object>(topic == null ? 3 : 4);
			if (pubKey != null) parameters.put("pubKey", pubKey);
			if (subKey != null) parameters.put("subKey", subKey);
			if (authKey != null) parameters.put("authKey", authKey);
			if (topic != null) parameters.put("topic", topic);
			
			String host = "app.magistral.io";
			String result = RESTcommunicator.call("https", host, "api/magistral/net/permissions", "GET", reqProps, parameters, "Unnable to get user permissions from [" + host + "]");
			
//			System.out.println("PERMISSIONS :: " + result);
			
			JSONParser parser = new JSONParser();
			
			Object o = parser.parse(result);
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
	
	private static PermMeta convertToPermData(JSONObject permJsonObj) {
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

	public static List<PermMeta> grant(String pubKey, String subKey, String secretKey, String user, String topic, Integer channel, Integer ttl, boolean read, boolean write) throws MagistralException {		
		try {

			Map<String, String> reqProps = new HashMap<String, String>(1);
			reqProps.put("Accept", "application/json");
			reqProps.put("Authorization", "Basic " + auth(pubKey, subKey, secretKey));
			
			Map<String, Object> parameters = new HashMap<String, Object>();
			if (pubKey != null) parameters.put("pubKey", pubKey);
			if (subKey != null) parameters.put("subKey", subKey);
			if (secretKey != null) parameters.put("authKey", secretKey);
			
			if (topic != null) parameters.put("topic", topic);		
			if (user != null) parameters.put("user", user);
			if (channel != null) parameters.put("channel", channel);
			if (ttl != null) parameters.put("ttl", ttl);
			
			parameters.put("read", read);
			parameters.put("write", write);
			
			String host = "app.magistral.io";
			RESTcommunicator.call("https", host, "api/magistral/net/grant", "PUT", reqProps, parameters, "Unnable to grant user permissions from [" + host + "]");
			
			return userPermissions(pubKey, subKey, secretKey, user); 
		} catch (Exception e) {
			throw new MagistralException(e); 
		}		
	}
	
	public static List<PermMeta> revoke(String pubKey, String subKey, String secretKey, String user, String topic, Integer channel) throws MagistralException {		
		try {

			Map<String, String> reqProps = new HashMap<String, String>(1);
			reqProps.put("Accept", "application/json");
			reqProps.put("Authorization", "Basic " + auth(pubKey, subKey, secretKey));
			
			Map<String, Object> parameters = new HashMap<String, Object>();
			if (pubKey != null) parameters.put("pubKey", pubKey);
			if (subKey != null) parameters.put("subKey", subKey);
			if (secretKey != null) parameters.put("authKey", secretKey);
			
			if (topic != null) parameters.put("topic", topic);		
			if (user != null) parameters.put("user", user);
			if (channel != null) parameters.put("channel", channel);
						
			String host = "app.magistral.io";
			RESTcommunicator.call("https", host, "api/magistral/net/revoke", "DELETE", reqProps, parameters, "Unnable to revoke user permissions from [" + host + "]");
						
			return userPermissions(pubKey, subKey, secretKey, user);
		} catch (Exception e) {
			throw new MagistralException(e); 
		}		
	}

}
