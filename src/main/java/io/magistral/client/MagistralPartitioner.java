package io.magistral.client;

import java.util.Map;

import org.apache.kafka.common.Cluster;

public class MagistralPartitioner implements org.apache.kafka.clients.producer.Partitioner {

	/*
	public MagistralPartitioner(VerifiableProperties props) {}

	public int partition(Object key, int a_numPartitions) {
		int partition = 0;
		String stringKey = (String) key;
		int offset = stringKey.lastIndexOf('.');
		if (offset > 0) {
			partition = Integer.parseInt( stringKey.substring(offset+1)) % a_numPartitions;
		}
		return partition;
	}
	*/

	@Override
	public void configure(Map<String, ?> arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int partition(String arg0, Object arg1, byte[] arg2, Object arg3, byte[] arg4, Cluster arg5) {
		// TODO Auto-generated method stub
		return 0;
	}
}
