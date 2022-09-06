package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;
    private static  final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    private boolean tryLock(String key,Long ttl){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", ttl, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    public void get(String key, Class type){
        String json = stringRedisTemplate.opsForValue().get(key);
        Object o = JSONUtil.toBean(json, type);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData),time);
    }

    /**
     * 缓存穿透，设置空缓存
     * @param id
     * @return
     */
    public <R,ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,Long time, TimeUnit unit) {
        R r;
        String json = stringRedisTemplate.opsForValue().get(keyPrefix + id);
        if (StrUtil.isNotBlank(json)){
            r = JSONUtil.toBean(json, type);
        }else{
            if (json != null){
                return null;
            }
            r = dbFallback.apply(id);
            if (r == null){
                set(keyPrefix + id,"",CACHE_SHOP_TTL, TimeUnit.MINUTES);
                return null;
            }
            set(keyPrefix + id,JSONUtil.toJsonStr(r),time, unit);
        }
        return r;
    }

    /**
     * 缓存击穿，设置互斥锁
     * @param id
     * @return
     */
    public <ID,R>R queryWithMutex(String keyPrefix,ID id, Class<R> type,Function<ID,R> dbFallback,Long time,TimeUnit unit) throws InterruptedException {
        R r;
        String json = stringRedisTemplate.opsForValue().get(keyPrefix + id);
        if (StrUtil.isNotBlank(json)){
            return JSONUtil.toBean(json, type);
        }
        if (json != null){
            return null;
        }

        try {
            if (!tryLock(LOCK_SHOP_KEY,LOCK_SHOP_TTL)){
                Thread.sleep(50);
                return queryWithMutex(keyPrefix,id, type,dbFallback,time,unit);
            }
            r = dbFallback.apply(id);
            if (r == null){
                set(keyPrefix + id,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            }else {
                set(keyPrefix + id,JSONUtil.toJsonStr(r),time, unit);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException();
        } finally {
            unLock(keyPrefix + id);
        }
        return r;
    }

    /**
     * 缓存击穿，设置逻辑过期
     * @param id
     * @return
     */
    public <ID,R>R queryWithLogicalExpire(
            String cacheKeyPrefix,String lockKeyPrefix,ID id, Class<R> type,Function<ID,R> dbFallback,Long lockTime) {

        String json = stringRedisTemplate.opsForValue().get(cacheKeyPrefix + id);
        if (StrUtil.isBlank(json)){
            return null;
        }
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject)redisData.getData(), type);
        if (redisData.getExpireTime().isAfter(LocalDateTime.now())){
            return r;
        }
        if (tryLock(lockKeyPrefix + id,lockTime)){
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    setWithLogicalExpire(cacheKeyPrefix,dbFallback.apply(id),20L,TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new RuntimeException();
                } finally {
                    unLock(lockKeyPrefix + id);
                }
            });
        }
        return r;
    }

}
