package io.magistral.client.pub;

import io.magistral.client.MagistralException;

public interface Callback {
	public void success(PubMeta meta);
	public void error(MagistralException ex);
}
