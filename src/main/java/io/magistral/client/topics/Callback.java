package io.magistral.client.topics;

import java.util.List;

import io.magistral.client.MagistralException;

public interface Callback {
	public void success(List<TopicMeta> meta);
	public void error(MagistralException ex);
}
