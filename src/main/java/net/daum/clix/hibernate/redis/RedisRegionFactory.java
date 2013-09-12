package net.daum.clix.hibernate.redis;

import java.util.Properties;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.NaturalIdRegion;
import org.hibernate.cfg.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

/**
 * @author jtlee
 * @author 84june
 */
public class RedisRegionFactory extends AbstractRedisRegionFactory {

	private static final long serialVersionUID = 1L;

	private final Logger LOG = LoggerFactory.getLogger(getClass());

    public RedisRegionFactory(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void start(Settings settings, Properties properties) throws CacheException {
        this.settings = settings;
        this.properties = properties;
        LOG.info("Initializing RedisClient(Jedis)...");
        this.pool = new JedisPool(new JedisPoolConfig(), properties.getProperty("redis.host", "localhost"),
                Integer.valueOf(properties.getProperty("redis.port", String.valueOf(Protocol.DEFAULT_PORT))),
                Integer.valueOf(properties.getProperty("redis.timeout",String.valueOf(Protocol.DEFAULT_TIMEOUT))),
                properties.getProperty("redis.password",null));
    }

    @Override
    public void stop() {
        this.pool.destroy();
    }

    @Override
    public NaturalIdRegion buildNaturalIdRegion(String regionName, Properties properties, CacheDataDescription metadata) throws CacheException {
        throw new UnsupportedOperationException(); //TODO still not implemented
    }

}
