package io.magistral.client.perm;

import java.util.List;
import io.magistral.client.MagistralException;

public interface Callback {
	public void success(List<PermMeta> meta);
	public void error(MagistralException ex);
}
