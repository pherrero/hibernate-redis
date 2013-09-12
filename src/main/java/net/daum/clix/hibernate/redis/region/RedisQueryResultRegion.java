package net.daum.clix.hibernate.redis.region;

import java.util.Properties;

import net.daum.clix.hibernate.redis.RedisCache;
import net.daum.clix.hibernate.redis.strategy.RedisAccessStrategyFactory;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.QueryResultsRegion;

/**
 * @author jtlee
 * @author 84june
 */
public class RedisQueryResultRegion extends RedisRegion implements QueryResultsRegion {

    public RedisQueryResultRegion(RedisAccessStrategyFactory accessStrategyFactory, RedisCache cache, Properties properties) {
        super(accessStrategyFactory, cache, properties);
    }

    @Override
    public Object get(Object key) throws CacheException {
        return cache.get(key);
    }

    @Override
    public void put(Object key, Object value) throws CacheException {
        cache.put(key, value);
    }

    @Override
    public void evict(Object key) throws CacheException {
        cache.remove(key);
    }

    @Override
    public void evictAll() throws CacheException {
        throw new UnsupportedOperationException("RedisQueryResultRegion#evictAll has not implemented yet!!");
    }
}
