package com.sam.enrichment.processor;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.streamline.cache.Cache;
import com.hortonworks.streamline.cache.stats.CacheStats;
import com.hortonworks.streamline.cache.view.config.BytesUnit;
import com.hortonworks.streamline.cache.view.config.ExpiryPolicy;
import com.hortonworks.streamline.cache.view.config.ExpiryPolicy.Size;
import com.hortonworks.streamline.cache.view.config.ExpiryPolicy.Ttl;

public class InMemoryCache implements Cache<String,Object> {

	 protected static final Logger LOG = LoggerFactory
	            .getLogger(InMemoryCache.class);
	Map<String,Object> myMap = new ConcurrentHashMap<String,Object>();
	
	@Override
	public void clear() {
		myMap.clear();
	}

	@Override
	public Object get(String key) {
		LOG.info("get key: " + key );
		return myMap.get(key);
	}

	@Override
	public Map<String, Object> getAll(Collection<? extends String> arg0) {
		return myMap;
	}

	@Override
	public ExpiryPolicy getExpiryPolicy() {
		return null;
	}

	@Override
	public void put(String key, Object val) {
		LOG.info("put key: " + key + "value:" + val);
		myMap.put(key, val);
	}

	@Override
	public void putAll(Map<? extends String, ? extends Object> arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void remove(String arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeAll(Collection<? extends String> arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long size() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public CacheStats stats() {
		// TODO Auto-generated method stub
		return null;
	}

}
