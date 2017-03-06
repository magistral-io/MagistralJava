package io.magistral;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Message implements Serializable {
	
	private String topic;
	private int channel;
	private long timestamp;
	private byte[] body;
	
	public Message() {}

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

	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long time) {
		this.timestamp = time;
	}

	public byte[] getBody() {
		return body;
	}
	public void setBody(byte[] body) {
		this.body = body;
	}
}
