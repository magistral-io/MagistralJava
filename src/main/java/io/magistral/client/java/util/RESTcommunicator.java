package io.magistral.client.java.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import io.magistral.client.MagistralException;

public class RESTcommunicator {
	
	public static String call(String protocol, String host, String path, String requestMethod, Map<String, String> requestProperties, Map<String, Object> parameters, String failMessage) throws MagistralException {
		try {
			StringBuilder url = new StringBuilder(protocol + "://" + host + "/" + path);
			if (parameters != null) {
				int i = 0;
				for (String key : parameters.keySet()) {
					url.append(i == 0 ? '?' : '&');
					url.append(key).append('=');
					url.append(parameters.get(key));
					i++;
				}
			}
			
			URL _url = new URL(url.toString());
			
			HttpURLConnection conn = (HttpURLConnection)_url.openConnection();
			conn.setRequestMethod(requestMethod);		
			
			if (requestProperties != null) {
				for (String rqpn : requestProperties.keySet()) {
					conn.setRequestProperty(rqpn, requestProperties.get(rqpn));
				}
			}
			
			if (conn.getResponseCode() != 200) {
				BufferedReader br = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
				StringBuilder sb = new StringBuilder();				
				String output;
				while ((output = br.readLine()) != null) {
					sb.append(output);
				}
				br.close();	
					
				throw new MagistralException(failMessage + ": " + sb.toString());
			}
	 
			BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
	 
			StringBuilder sb = new StringBuilder();
			String line;
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}	 
			conn.disconnect();
			br.close();
			
			return sb.toString();			
		} catch (MalformedURLException e) {
			throw new MagistralException(e.getMessage());
		} catch (IOException e) {
			throw new MagistralException(e.getMessage());
		}
	}
}
