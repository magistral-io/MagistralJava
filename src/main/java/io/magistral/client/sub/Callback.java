package io.magistral.client.sub;

public interface Callback {
	public void success(SubMeta meta);
	public void error(Throwable ex);
}
