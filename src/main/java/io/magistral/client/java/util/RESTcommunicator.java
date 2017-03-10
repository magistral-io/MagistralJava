package io.magistral.client.java.util;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.AbstractMap.SimpleEntry;
import java.io.BufferedReader;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.json.simple.JSONObject;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.client.apache4.ApacheHttpClient4Handler;

public enum RESTcommunicator {
	
	instance;
	
	private Client client;
	
	private RESTcommunicator() {		
		ThreadSafeClientConnManager m = new ThreadSafeClientConnManager();
		m.setDefaultMaxPerRoute(5);
		m.setMaxTotal(10);
		
		client = new Client(new ApacheHttpClient4Handler(new DefaultHttpClient(m), new BasicCookieStore(), true));
		client.setConnectTimeout(10000);
	}
	
	public void setCredentials(String user, String password) {
		client.addFilter(new HTTPBasicAuthFilter(user, password));
	}
	
	public SimpleEntry<Integer, String> get(String path) {
		ClientResponse response = client.resource(path)
				.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
		
		return clientResponseToString(response);
	}

	public SimpleEntry<Integer, String> get(String path, MultivaluedMap<String, String> params) {
		ClientResponse response = client.resource(path)
				.queryParams(params).accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
		
		return clientResponseToString(response);
	}
	
	public SimpleEntry<Integer, String> get(String path, MultivaluedMap<String, String> params, String ... mediaType) {
		ClientResponse response = client.resource(path)
				.queryParams(params).accept(mediaType).get(ClientResponse.class);
		
		return clientResponseToString(response);
	}
	
	public SimpleEntry<Integer, String> put(String path, MultivaluedMap<String, String> parameters, String ... mediaType) {
		
		StringBuilder url = new StringBuilder(path);
		if (parameters != null) {
			int i = 0;
			for (String key : parameters.keySet()) {
				url.append(i == 0 ? '?' : '&');
				url.append(key).append('=');
				url.append(parameters.get(key));
				i++;
			}
		}
		
		ClientResponse res = (mediaType == null || mediaType.length == 0) 
				? client.resource(path).queryParams(parameters).put(ClientResponse.class)
				: client.resource(path).queryParams(parameters).accept(mediaType).put(ClientResponse.class);
		
		return clientResponseToString(res);
	}
	
	public SimpleEntry<Integer, String> delete(String path, MultivaluedMap<String, String> parameters) {
		
		StringBuilder url = new StringBuilder(path);
		if (parameters != null) {
			int i = 0;
			for (String key : parameters.keySet()) {
				url.append(i == 0 ? '?' : '&');
				url.append(key).append('=');
				url.append(parameters.get(key));
				i++;
			}
		}
		
		ClientResponse res = client.resource(path)
				.queryParams(parameters).accept(MediaType.APPLICATION_JSON)				
				.delete(ClientResponse.class);
		
		return clientResponseToString(res);
	}
	
	public SimpleEntry<Integer, String> post(String path) {
		WebResource wr = client.resource(path);
		
		ClientResponse res = wr.accept(MediaType.APPLICATION_FORM_URLENCODED)	
				.header("Connection", "keep-alive")
				.header("Accept-Encoding", "gzip")
				.post(ClientResponse.class);
		return clientResponseToString(res);
	}

	public SimpleEntry<Integer, String> post(String path, MultivaluedMap<String, String> form) {
		if (form == null || form.isEmpty()) return post(path);
		
		WebResource wr = client.resource(path);				
		ClientResponse res = wr.accept(MediaType.APPLICATION_FORM_URLENCODED)	
				.header("Connection", "keep-alive")
				.header("Accept-Encoding", "gzip")
				.post(ClientResponse.class, form);		
		return clientResponseToString(res);
	}

	public SimpleEntry<Integer, String> post(String path, String json) {
		WebResource wr = client.resource(path);		
		ClientResponse res = wr.accept(MediaType.APPLICATION_JSON)	
				.header("Connection", "keep-alive")
				.header("Accept-Encoding", "gzip")
				.post(ClientResponse.class, json);		
		return clientResponseToString(res);
	}

	public SimpleEntry<Integer, String> post(String path, JSONObject json) {				
		return post(path, json.toJSONString());
	}
	
	private synchronized SimpleEntry<Integer, String> clientResponseToString(ClientResponse r) {
		
		int status = r.getStatus();
		
		try (InputStream is = r.getEntityInputStream();
				BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is));) {
			
			StringBuilder sb = new StringBuilder();
			
			String line;
	        while ((line = bufferedReader.readLine()) != null) {
	            sb.append(line);
	        }
	        
	        return new SimpleEntry<Integer, String>(status, sb.toString());
		} catch (Exception e) {
			e.printStackTrace();
			return new SimpleEntry<Integer, String>(status, e.getMessage());
		} finally {
			r.close();
		}
	}

//	public String call(String protocol, String host, String path, String requestMethod, Map<String, String> requestProperties, Map<String, Object> parameters, String failMessage) throws MagistralException {
//		try {
			
//			WebResource webResource = client.resource(protocol + "://" + host + "/" + path);
//			ClientResponse response = webResource
//					.accept(MediaType.APPLICATION_FORM_URLENCODED)
//					.header("Connection", "keep-alive")
//					.header("Accept-Encoding", "gzip")
//					.post(ClientResponse.class, formData);
			
			
			
//			StringBuilder url = new StringBuilder(protocol + "://" + host + "/" + path);
//			if (parameters != null) {
//				int i = 0;
//				for (String key : parameters.keySet()) {
//					url.append(i == 0 ? '?' : '&');
//					url.append(key).append('=');
//					url.append(parameters.get(key));
//					i++;
//				}
//			}
			
//			URL _url = new URL(url.toString());
//			
//			HttpURLConnection conn = (HttpURLConnection)_url.openConnection();
//			conn.setRequestMethod(requestMethod);		
//			
//			if (requestProperties != null) {
//				for (String rqpn : requestProperties.keySet()) {
//					conn.setRequestProperty(rqpn, requestProperties.get(rqpn));
//				}
//			}
//			
//			if (conn.getResponseCode() != 200) {
//				BufferedReader br = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
//				StringBuilder sb = new StringBuilder();				
//				String output;
//				while ((output = br.readLine()) != null) {
//					sb.append(output);
//				}
//				br.close();	
//					
//				throw new MagistralException(failMessage + ": " + sb.toString());
//			}
//	 
//			BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
//	 
//			StringBuilder sb = new StringBuilder();
//			String line;
//			while ((line = br.readLine()) != null) {
//				sb.append(line);
//			}	 
//			conn.disconnect();
//			br.close();
			
//			return sb.toString();			
//		} catch (MalformedURLException e) {
//			throw new MagistralException(e.getMessage());
//		} catch (IOException e) {
//			throw new MagistralException(e.getMessage());
//		}
//	}
}
