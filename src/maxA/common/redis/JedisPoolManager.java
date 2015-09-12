package maxA.common.redis;

/**
 * Created by max2 on 8/7/15.
 */

import avro.Give;
import avro.GiveLogSchemaHelper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import maxA.common.Constants;
import org.json4s.jackson.Json;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Created by daniel on 2/4/15.
 */
public enum JedisPoolManager {
    INSTANCE;

    private JedisPool pool = new JedisPool( new JedisPoolConfig(),
                                            Constants.REDIS_SERVER,
                                            Integer.parseInt( Constants.REDIS_PORT ));

    public Jedis getJedis() {
        return pool.getResource();
    }

    public void destroy() {
        pool.destroy();
    }
}
