package io.magistral.client;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public enum ERROR {
	
	UNIDENTIFIED_APP(1317400, "Unnable to identify application"),
	PUB_KEY_INVALID_FORMAT(1317401, "Invalid format of public key"),
	SUB_KEY_INVALID_FORMAT(1317402, "Invalid format of sub key"),
	SECRET_KEY_INVALID_FORMAT(1317403, "Invalid format of secret key"),
	NO_TOKEN_PROVIDED(1317404, "No connection token provided"),
	NO_PERMISSIONS(1317405, "User has no permissions for resource"),
	NO_PERMISSIONS_TO_READ(1317406, "User has no permissions to read"),
	NO_PERMISSIONS_TO_WRITE(1317407, "User has no permissions to write"),
	
	MSG_COUNT_QUOTA(1317501, "Message count quota exhausted"),
	MSG_SIZE_LIMIT_OVERCOME(1317502, "Message size exceeds the allowable limit"),
	APP_LIMIT_OVERCOME(1317503, "Application count limit reached"),
	TOPIC_LIMIT_OVERCOME(1317504, "Topic count limit reached"),
	CHANNEL_LIMIT_OVERCOME(1317505, "Channel count limit reached"),
	CON_COUNT_LIMIT_OVERCOME(1317506, "Connections count limit reached");

	int code;
	String message;
	
	private static Map<Integer, ERROR> cm = null;
	
	ERROR(int _code, String _message){
		code = _code; message = _message;		
	}
	
	public static ERROR getByCode(int code) {
		if (cm == null) {
			cm = new HashMap<Integer, ERROR>(values().length);
			for (ERROR e : values()) cm.put(e.code(), e); 
		}
		return cm.get(code);
	}
	
	public static Set<Integer> codes() {
		if (cm == null) {
			cm = new HashMap<Integer, ERROR>(values().length);
			for (ERROR e : values()) cm.put(e.code(), e); 
		}
		return cm.keySet();
	}
	
	public int code() {
		return code;
	}
	
	public String message() {
		return message;
	}
}

