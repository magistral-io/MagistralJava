package io.magistral.client;

@SuppressWarnings("serial")
public class MagistralException extends Exception {

	public MagistralException() {
		super();
	}

	public MagistralException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public MagistralException(String message, Throwable cause) {
		super(message, cause);
	}

	public MagistralException(String message) {
		super(message);
	}

	public MagistralException(Throwable cause) {
		super(cause);
	}

	
}
