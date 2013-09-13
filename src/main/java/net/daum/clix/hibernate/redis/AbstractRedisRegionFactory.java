package net.daum.clix.hibernate.redis;

import java.util.Properties;

import net.daum.clix.hibernate.redis.jedis.JedisCacheImpl;
import net.daum.clix.hibernate.redis.region.RedisCollectionRegion;
import net.daum.clix.hibernate.redis.region.RedisEntityRegion;
import net.daum.clix.hibernate.redis.region.RedisQueryResultRegion;
import net.daum.clix.hibernate.redis.region.RedisTimestampsRegion;
import net.daum.clix.hibernate.redis.strategy.RedisAccessStrategyFactory;
import net.daum.clix.hibernate.redis.strategy.RedisAccessStrategyFactoryImpl;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.CollectionRegion;
import org.hibernate.cache.spi.EntityRegion;
import org.hibernate.cache.spi.QueryResultsRegion;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.TimestampsRegion;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cfg.Settings;

import redis.clients.jedis.ShardedJedisPool;

/**
 * @author jtlee
 * @author 84june
 */
abstract class AbstractRedisRegionFactory implements RegionFactory {

	private static final long serialVersionUID = 1L;

	protected ShardedJedisPool pool;
	protected Properties properties;
	protected Settings settings;

	/**
	 * {@link RedisAccessStrategyFactory} for creating various access strategies
	 */
	private final RedisAccessStrategyFactory accessStrategyFactory = new RedisAccessStrategyFactoryImpl();

	@Override
	public boolean isMinimalPutsEnabledByDefault() {
		return true;
	}

	@Override
	public AccessType getDefaultAccessType() {
		return AccessType.READ_WRITE;
	}

	@Override
	public long nextTimestamp() {
		return System.currentTimeMillis() / 100;
	}

	@Override
	public EntityRegion buildEntityRegion(String regionName, Properties properties, CacheDataDescription metadata) throws CacheException {
		return new RedisEntityRegion(accessStrategyFactory, getRedisCache(regionName), properties, metadata, settings);
	}

	@Override
	public CollectionRegion buildCollectionRegion(String regionName, Properties properties, CacheDataDescription metadata)
			throws CacheException {
		return new RedisCollectionRegion(accessStrategyFactory, getRedisCache(regionName), properties, metadata, settings);
	}

	@Override
	public QueryResultsRegion buildQueryResultsRegion(String regionName, Properties properties) throws CacheException {
		return new RedisQueryResultRegion(accessStrategyFactory, getRedisCache(regionName), properties);
	}

	@Override
	public TimestampsRegion buildTimestampsRegion(String regionName, Properties properties) throws CacheException {
		return new RedisTimestampsRegion(accessStrategyFactory, getRedisCache(regionName), properties);
	}

	private RedisCache getRedisCache(String regionName) {
		return new JedisCacheImpl(pool, regionName);
	}

}
