package io.magistral.client;

import java.util.List;
import java.util.concurrent.Future;

import io.magistral.client.perm.Callback;
import io.magistral.client.perm.PermMeta;

public interface IAccessControl {
	
	public Future<List<PermMeta>> permissions() throws MagistralException;
	public Future<List<PermMeta>> permissions(Callback callback) throws MagistralException;
	
	public Future<List<PermMeta>> permissions(String topic) throws MagistralException;
	public Future<List<PermMeta>> permissions(String topic, Callback callback) throws MagistralException;
	
	public Future<List<PermMeta>> grant(String user, String topic, boolean read, boolean write) throws MagistralException;
	public Future<List<PermMeta>> grant(String user, String topic, boolean read, boolean write, int ttl) throws MagistralException;
	public Future<List<PermMeta>> grant(String user, String topic, int channel, boolean read, boolean write) throws MagistralException;
	public Future<List<PermMeta>> grant(String user, String topic, int channel, boolean read, boolean write, int ttl) throws MagistralException;
	
	public Future<List<PermMeta>> grant(String user, String topic, boolean read, boolean write, Callback callback) throws MagistralException;
	public Future<List<PermMeta>> grant(String user, String topic, boolean read, boolean write, int ttl, Callback callback) throws MagistralException;
	public Future<List<PermMeta>> grant(String user, String topic, int channel, boolean read, boolean write, Callback callback) throws MagistralException;
	public Future<List<PermMeta>> grant(String user, String topic, int channel, boolean read, boolean write, int ttl, Callback callback) throws MagistralException;
	
	public Future<List<PermMeta>> revoke(String user, String topic) throws MagistralException;
	public Future<List<PermMeta>> revoke(String user, String topic, Callback callback) throws MagistralException;
	public Future<List<PermMeta>> revoke(String user, String topic, int channel) throws MagistralException;
	public Future<List<PermMeta>> revoke(String user, String topic, int channel, Callback callback) throws MagistralException;
}
