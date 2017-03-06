package io.magistral.client.topics;

import java.io.Serializable;
import java.util.Set;

@SuppressWarnings("serial")
public class TopicMeta implements Serializable {
	
	private String topicName;
	private Set<Integer> сhannels;
	
	public TopicMeta() {}
		
	public String getTopicName() {
		return topicName;
	}
	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}

	public Set<Integer> getСhannels() {
		return сhannels;
	}
	public void setСhannels(Set<Integer> сhannels) {
		this.сhannels = сhannels;
	}
}
