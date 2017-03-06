package io.magistral.client.java.util;

public class KeyExtractor {
	
	public static String sKeyToCipher(String sKey) {
		StringBuilder sb = new StringBuilder(16);
		sb.append(sKey.substring(26))
			.insert(0, sKey.charAt(11)).insert(5, sKey.charAt(12))
			.insert(2, sKey.charAt(22)).insert(14, sKey.charAt(23));
		return sb.toString();
	}
}
