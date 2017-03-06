package io.magistral.client.pub;

import java.io.Serializable;

@SuppressWarnings("serial")
public class PubMeta implements Serializable {
	
	private String topic;
	private int channel;
	
	public PubMeta() {}

	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getChannel() {
		return channel;
	}
	public void setChannel(int channel) {
		this.channel = channel;
	}
}
