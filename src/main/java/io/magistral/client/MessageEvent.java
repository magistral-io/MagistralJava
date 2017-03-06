package io.magistral.client;

import java.util.EventObject;

@SuppressWarnings("serial")
public class MessageEvent extends EventObject {
	
	public MessageEvent(String topic, int channel, byte[] msg, long index, long timestamp) {
		super(new Object[]{topic, channel, msg, index, timestamp});		
	}
	
	@Override
	public Object getSource() {
		return super.getSource();
	}

	public String topic() {
		Object[] o = (Object[])getSource();
		return o[0].toString();
	}

	public int channel() {
		Object[] o = (Object[])getSource();
		return (int)o[1];
	}

	public byte[] msgBody() {
		Object[] o = (Object[])getSource();
		return (byte[])o[2];
	}
	
	public long index() {
		Object[] o = (Object[])getSource();
		return (long)o[3];
	}
	
	public long timestamp() {
		Object[] o = (Object[])getSource();
		return (long)o[4];
	}
}
