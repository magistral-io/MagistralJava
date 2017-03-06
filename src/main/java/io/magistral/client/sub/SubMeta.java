package io.magistral.client.sub;

import java.io.Serializable;
import java.util.List;

@SuppressWarnings("serial")
public class SubMeta implements Serializable {

	private String group;
	private String topic;
	private List<String> endPoints;
	private int channel;
	
	public SubMeta() {}
	
	public String getGroup() {
		return group;
	}
	public void setGroup(String group) {
		this.group = group;
	}
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	public List<String> getEndPoints() {
		return endPoints;
	}
	public void setEndPoints(List<String> endPoints) {
		this.endPoints = endPoints;
	}
	public int getChannel() {
		return channel;
	}
	public void setChannel(int channel) {
		this.channel = channel;
	}	
}
