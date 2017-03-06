package io.magistral.client.data;

import io.magistral.client.MagistralException;

public interface Callback {
	public void success(History meta);
	public void error(MagistralException ex);
}
