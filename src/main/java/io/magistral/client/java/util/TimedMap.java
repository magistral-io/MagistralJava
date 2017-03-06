package io.magistral.client.java.util;

import java.util.Collections;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class TimedMap<K, V> {
	
	private final ConcurrentMap<K, ExpiredObject<K, V>> map = new ConcurrentHashMap<K, ExpiredObject<K, V>>();
	private final Lock writeLock = new ReentrantLock();
	private static final long DEFAULT_TTL = 60000L;
	private final Timer timer = new Timer("TimedMap Timer", true);

	@SuppressWarnings("hiding")
	public class ExpiredObject<K, V> {
		private final V value;
		private final ExpiryTask<K> task;
		private final long ttl;

		public ExpiredObject(K key, V value) {
			this(key, value, DEFAULT_TTL);
		}

		public ExpiredObject(K key, V value, long ttl) {
			this.value = value;
			this.task = new ExpiryTask<K>(key);
			this.ttl = ttl;
			timer.schedule(this.task, ttl);
		}

		public ExpiryTask<K> getTask() {
			return task;
		}

		public V getValue() {
			return value;
		}

		public long getTtl() {
			return ttl;
		}
	}
	
	private TimedMapEvictionListener _listener = null;
	public void addEvictionListener(TimedMapEvictionListener listener) {
		if (_listener == null) _listener = listener;
	}

	@SuppressWarnings("hiding")
	class ExpiryTask<K> extends TimerTask {
		private final K key;

		public ExpiryTask(K key) {
			this.key = key;
		}

		public K getKey() {
			return key;
		}

		@Override
		public void run() {
//			System.out.println("Expiring element with key [" + key + "]");
			try {
				writeLock.lock();
				if (map.containsKey(key)) {
					Object eo = map.remove(getKey());
					if (_listener != null && getKey() != null && eo != null) {
						_listener.evicted(new java.util.AbstractMap.SimpleEntry<K, Object>(getKey(), eo));
					}
				}
			} finally {
				writeLock.unlock();
			}
		}
	}

	public void put(K key, V value, long expiry) {
		try {
			writeLock.lock();

			final ExpiredObject<K, V> object = map.putIfAbsent(key, new ExpiredObject<K, V>(key, value, expiry));
			/*
			 * Was there an existing entry for this key? If so, cancel the
			 * existing timer
			 */
			if (object != null)
				object.getTask().cancel();
		} finally {
			writeLock.unlock();
		}
	}

	public void put(K key, V value) {
		put(key, value, DEFAULT_TTL);
	}

	public V get(K key) {
		return (V) (map.containsKey(key) ? map.get(key).getValue() : null);
	}

	public void clear() {
		try {
			writeLock.lock();
			for (ExpiredObject<K, V> obj : map.values()) {
				obj.getTask().cancel();
			}
			map.clear();
			timer.purge(); // Force removal of all cancelled tasks
		} finally {
			writeLock.unlock();
		}
	}

	public boolean containsKey(K key) {
		return map.containsKey(key);
	}
	
	public Set<K> keySet() {
		return Collections.unmodifiableSet(map.keySet());
	}

	public boolean isEmpty() {
		return map.isEmpty();
	}

	public V remove(K key) {
		final ExpiredObject<K, V> object;
		try {
			writeLock.lock();
			object = map.remove(key);
			if (object != null)
				object.getTask().cancel();
		} finally {
			writeLock.unlock();
		}
		return (object == null ? null : object.getValue());
	}

	public int size() {
		return map.size();
	}
}
