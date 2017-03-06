package io.magistral.client.java.util;

import java.util.HashMap;
import java.util.Map;

import io.magistral.client.pub.PubMeta;

public enum PrimitiveFactory {

	instance;
	
	public static PrimitiveFactory getInstance() {
		return instance;
	}
	
	private Map<String, Map<Integer, PubMeta>> pubMetaMap = new HashMap<>();
	public PubMeta createPubMeta(String topic, int channel) {
		
		if (!pubMetaMap.containsKey(topic)) {
			PubMeta meta = new PubMeta();
			meta.setTopic(topic);
			meta.setChannel(channel);
			
			Map<Integer, PubMeta> pmm = new HashMap<>();
			pmm.put(channel, meta);
			
			pubMetaMap.put(topic, pmm);
			return meta;
		} else {
			Map<Integer, PubMeta> pmm = pubMetaMap.get(topic);
			if (!pmm.containsKey(channel)) {
				PubMeta meta = new PubMeta();
				meta.setTopic(topic);
				meta.setChannel(channel);
				
				pmm.put(channel, meta);
				pubMetaMap.put(topic, pmm);
				return meta;
			} else {
				return pubMetaMap.get(topic).get(channel);
			}
		}
	}
}
