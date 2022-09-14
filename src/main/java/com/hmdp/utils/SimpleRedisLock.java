package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{
    private String name;
    private StringRedisTemplate stringRedisTemplate;
    private static final String KEY_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID().toString(true)+"-";

    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        Boolean ifAbsent = stringRedisTemplate.opsForValue().setIfAbsent(KEY_PREFIX + name,
                ID_PREFIX + Thread.currentThread().getId(),
                timeoutSec,
                TimeUnit.SECONDS);

        return Boolean.TRUE.equals(ifAbsent);
    }

    @Override
    public void unLock() {
        long threadId = Thread.currentThread().getId();
        String value = stringRedisTemplate.opsForValue().get(KEY_PREFIX + name);
        if ((ID_PREFIX + threadId).equals(value)){
            stringRedisTemplate.delete(KEY_PREFIX + name);
        }

    }
}
