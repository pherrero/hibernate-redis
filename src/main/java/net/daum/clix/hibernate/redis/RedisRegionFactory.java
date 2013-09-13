package net.daum.clix.hibernate.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.NaturalIdRegion;
import org.hibernate.cfg.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ShardedJedisPool;

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
        int defaultTimeout = Integer.parseInt(properties.getProperty("redis.timeout", String.valueOf(Protocol.DEFAULT_TIMEOUT)));
        String defaultPassword = properties.getProperty("redis.password", null);
        String[] shards = properties.getProperty("redis.shards", "localhost").split(",");
        List<JedisShardInfo> shardList = new ArrayList<JedisShardInfo>(shards.length);
        for (String shard : shards) {
        	String[] shardAttributes = shard.split(":");
        	int attributeIndex = 0;
        	String hostname = shardAttributes[attributeIndex++];
        	int port = (shardAttributes.length > attributeIndex) ? 
        			Integer.parseInt(shardAttributes[attributeIndex++]) : Protocol.DEFAULT_PORT;
        	int timeout = (shardAttributes.length > attributeIndex) ? 
                			Integer.parseInt(shardAttributes[attributeIndex++]) : defaultTimeout;
            String password = (shardAttributes.length > attributeIndex) ? 
                        	shardAttributes[attributeIndex++] : defaultPassword;
            JedisShardInfo shardInfo = new JedisShardInfo(hostname, port, timeout);
            shardInfo.setPassword(password);
            shardList.add(shardInfo);
        }
        this.pool = new ShardedJedisPool(new JedisPoolConfig(), shardList);
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
