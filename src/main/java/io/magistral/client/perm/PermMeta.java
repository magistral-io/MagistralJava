package io.magistral.client.perm;

import java.io.Serializable;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("serial")
public class PermMeta implements Serializable {
	
	private String topic;
	private Map<Integer, SimpleEntry<Boolean, Boolean>> permissions = null;

	public PermMeta() {}

	public String topic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Set<Integer> channels() {
		return permissions == null ? Collections.emptySet() : permissions.keySet();
	}
	
	public boolean readable(int channel) {
		if (permissions == null || !permissions.containsKey(channel)) return false;
		return permissions.get(channel).getKey();
	}
	
	public boolean writable(int channel) {
		if (permissions == null || !permissions.containsKey(channel)) return false;
		return permissions.get(channel).getValue();
	}
	
	public void addPermissions(Map<Integer, SimpleEntry<Boolean, Boolean>> permissions) {
		if (permissions == null) return;		
		if (this.permissions == null) {
			this.permissions = permissions;
		} else {			
			this.permissions.putAll(permissions);
		}
	}
	
	public void addPermission(int channel, boolean readable, boolean writable) {		
		if (this.permissions == null) {
			this.permissions = new HashMap<>(1);			
		} 		
		this.permissions.put(channel, new SimpleEntry<Boolean, Boolean>(readable, writable));			
	}
	
	@Override
	public String toString() {
		return permissions == null ? "{}" : permissions.toString();
	}
}
