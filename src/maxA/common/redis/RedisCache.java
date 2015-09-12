package maxA.common.redis;

import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by daniel on 2/9/15. Redis cache for different types of data.
 * Implements saving patterns
 */
public enum RedisCache {
	INSTANCE;

	public final static String NA = "{message: NOT_AVAILABLE}";

	/**
	 * The method takes a JsonNode string and stores its string representation
	 * on Redis. If ttl is not null sets an expiration time in seconds
	 * 
	 * @param key
	 * @param data
	 * @param ttl
	 */
	public void addByKey(String key, String data, int ttl) {
		Jedis jedis = JedisPoolManager.INSTANCE.getJedis();
		jedis.set(key, data);
		if (ttl > 0) {
			jedis.expire(key, ttl);
		}
		jedis.close();
	}

	/**
	 * The method uses a counter stored in redis to create a key of the form
	 * key:{indexName+1}
	 * 
	 * @param key
	 * @param indexName
	 * @param data
	 * @param ttl
	 */
	public void addAndIncrement(String key, String indexName, String data,
			int ttl) {
		Jedis jedis = JedisPoolManager.INSTANCE.getJedis();
		long id = jedis.incr(indexName);

		if (key.endsWith(":")) {
			key += "" + id;

		} else {
			key += ":" + id;
		}
		jedis.set(key, data);
		if (ttl > 0) {
			jedis.expire(key, ttl);
		}
		jedis.close();
	}

	/**
	 * Decrements the given key atomically
	 * 
	 * @param key
	 */
	public void decrementFromName(String key) {
		Jedis jedis = JedisPoolManager.INSTANCE.getJedis();
		jedis.decr(key);
		jedis.close();
	}

	/**
	 * Returns the String value for the Key
	 * 
	 * @param key
	 * @return
	 */
	public String getFromKey(String key) {
		Jedis jedis = JedisPoolManager.INSTANCE.getJedis();
		String res = jedis.get(key);
		jedis.close();
		return res;
	}

	/**
	 * Returns the values for the specified keys in the parameters
	 * 
	 * @param keys
	 * @return
	 */
	public List<String> mgetFromKey(String... keys) {
		Jedis jedis = JedisPoolManager.INSTANCE.getJedis();
		List<String> res = jedis.mget(keys);
		jedis.close();

		return res;
	}

	/**
	 * The method is used to increment a counter
	 * 
	 * @param key
	 */
	public long incrementKey(String key) {
		Jedis jedis = JedisPoolManager.INSTANCE.getJedis();
		long val = jedis.incr(key);
		jedis.close();
		return val;
	}

	/**
	 * The method takes a JsonNode string and stores it in hashes on Redis. If
	 * ttl is positive, it sets an expiration time in seconds
	 * 
	 * @param key
	 * @param field
	 * @param value
	 * 
	 */
	public void addtoHashByKey(String key, String field, String value) {
		Jedis jedis = JedisPoolManager.INSTANCE.getJedis();
		// hset returns 1 if a new key-value is added, 0 if an old key-value is
		// overridden
		jedis.hset(key, field, value);
		jedis.close();

	}

	/**
	 * Returns the values for the specified key in the parameters from hashes
	 * 
	 * @param key
	 * @param fields
	 * @return
	 */
	public List<String> mHashgetFromKey(String key, String... fields) {
		Jedis jedis = JedisPoolManager.INSTANCE.getJedis();
		List<String> res = jedis.hmget(key, fields);
		jedis.close();

		return res;
	}

	/**
	 * Returns the value for the specified key in the parameters from hash set
	 * 
	 * @param key
	 * @param field
	 * @return
	 */
	public String hashGetFromKey(String key, String field) {
		Jedis jedis = JedisPoolManager.INSTANCE.getJedis();
		String res = jedis.hget(key, field);
		jedis.close();

		return res;
	}

	/**
	 * Returns all the values for the specified hashset
	 * 
	 * @param key
	 * @return
	 */
	public Map<String, String> getAllFromHSet(String key) {
		Jedis jedis = JedisPoolManager.INSTANCE.getJedis();
		Map<String, String> res = jedis.hgetAll(key);
		jedis.close();

		return res;
	}

	public Set<String> getAllFromSet(String key) {
		Jedis jedis = JedisPoolManager.INSTANCE.getJedis();
		Set<String> res = jedis.smembers(key);
		jedis.close();
		return res;
	}

	public Set<String> getAllKeys(String prefix) {
		Jedis jedis = JedisPoolManager.INSTANCE.getJedis();
		Set<String> res = jedis.keys(prefix + "*");
		jedis.close();

		return res;
	}

	/**
	 * Adds to the set with key, the value
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public long addToSet(String key, String value) {
		Jedis jedis = JedisPoolManager.INSTANCE.getJedis();
		long res = jedis.sadd(key, value);
		jedis.close();

		return res;
	}

	/**
	 * Removes from the set with key, the value
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public long removeFromSet(String key, String value) {
		Jedis jedis = JedisPoolManager.INSTANCE.getJedis();
		long res = jedis.srem(key, value);
		jedis.close();
		return res;
	}

	/**
	 * Remove key from REDIS
	 * 
	 * @param key
	 * @return
	 */
	public long removeByKey(String key) {
		Jedis jedis = JedisPoolManager.INSTANCE.getJedis();
		long res = jedis.del(key);
		jedis.close();
		return res;
	}

	/**
	 * Remove field from key REDIS
	 * 
	 * @param key
	 * @param field
	 * @return
	 */
	public long removeFieldFromKey(String key, String field) {
		Jedis jedis = JedisPoolManager.INSTANCE.getJedis();
		long res = jedis.hdel(key, field);
		jedis.close();
		return res;
	}

	/**
	 * Get the TTL
	 * 
	 * @param key
	 * @return
	 */
	public long getTTL(String key) {
		Jedis jedis = JedisPoolManager.INSTANCE.getJedis();
		long ttl = jedis.ttl(key);
		jedis.close();
		return ttl;
	}

	public boolean existsKey(String key) {
		Jedis jedis = JedisPoolManager.INSTANCE.getJedis();
		boolean res = jedis.exists(key);
		jedis.close();
		return res;
	}
}