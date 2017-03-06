package io.magistral.client.data;

import java.util.concurrent.Future;

import io.magistral.client.MagistralException;

public interface IHistory {
	public Future<History> history(String topic, int channel, int count) throws MagistralException;
	public Future<History> history(String topic, int channel, long start, int count) throws MagistralException;
	public Future<History> history(String topic, int channel, long start, long end) throws MagistralException;
	
	public Future<History> history(String topic, int channel, int count, Callback callback) throws MagistralException;
	public Future<History> history(String topic, int channel, long start, int count, Callback callback) throws MagistralException;
	public Future<History> history(String topic, int channel, long start, long end, Callback callback) throws MagistralException;
}
