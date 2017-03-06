package io.magistral.client.sub;

import io.magistral.client.MagistralException;
import io.magistral.client.MessageEvent;

public interface NetworkListener {
	public void messageReceived(MessageEvent event);	
	public void disconnected(String topic);
	public void reconnect(String topic);
	public void connected(String topic);	
	public void error(MagistralException ex);
}
