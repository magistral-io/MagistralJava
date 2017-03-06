package io.magistral.client.data;

import java.util.List;
import io.magistral.Message;

public class History {
	
	private List<Message> messages;

	public List<Message> getMessages() {
		return messages;
	}
	public void setMessages(List<Message> messages) {
		this.messages = messages;
	}
}
