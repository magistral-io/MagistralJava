package io.magistral.client;

import java.util.List;
import java.util.concurrent.Future;

import io.magistral.client.data.IHistory;
import io.magistral.client.pub.PubMeta;
import io.magistral.client.sub.NetworkListener;
import io.magistral.client.sub.SubMeta;
import io.magistral.client.topics.TopicMeta;

public interface IMagistral extends IAccessControl, IHistory {
	
	public Future<SubMeta> subscribe(String topic, NetworkListener listener) throws MagistralException;
	public Future<SubMeta> subscribe(String topic, int channel, NetworkListener listener) throws MagistralException;
	public Future<SubMeta> subscribe(String topic, String group, NetworkListener listener) throws MagistralException;
	public Future<SubMeta> subscribe(String topic, String group, int channel, NetworkListener listener) throws MagistralException;
	
	public Future<SubMeta> subscribe(String topic, NetworkListener listener, io.magistral.client.sub.Callback callback) throws MagistralException;
	public Future<SubMeta> subscribe(String topic, int channel, NetworkListener listener, io.magistral.client.sub.Callback callback) throws MagistralException;
	public Future<SubMeta> subscribe(String topic, String group, NetworkListener listener, io.magistral.client.sub.Callback callback) throws MagistralException;
	public Future<SubMeta> subscribe(String topic, String group, int channel, NetworkListener listener, io.magistral.client.sub.Callback callback) throws MagistralException;
	
	public Future<SubMeta> unsubscribe(String topic) throws MagistralException;
	public Future<SubMeta> unsubscribe(String topic, int channel) throws MagistralException;
	
	public Future<PubMeta> publish(String topic, byte[] msg) throws MagistralException;
	public Future<PubMeta> publish(String topic, int channel, byte[] msg) throws MagistralException;
	
	public Future<PubMeta> publish(String topic, byte[] msg, io.magistral.client.pub.Callback callback) throws MagistralException;
	public Future<PubMeta> publish(String topic, int channel, byte[] msg, io.magistral.client.pub.Callback callback) throws MagistralException;
		
	public Future<List<TopicMeta>> topics() throws MagistralException;
	public Future<List<TopicMeta>> topics(io.magistral.client.topics.Callback callback) throws MagistralException;
	public Future<TopicMeta> topic(String topic) throws MagistralException;
	public Future<TopicMeta> topic(String topic, io.magistral.client.topics.Callback callback) throws MagistralException;
	
	public void close();
}
