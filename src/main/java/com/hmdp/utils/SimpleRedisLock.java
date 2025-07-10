package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{

    private String name;
    private StringRedisTemplate stringRedisTemplate;

    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    private static final String KEY_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID().toString(true)+"-";   // true去除横线

    @Override
    public boolean tryLock(long timeoutSec) {
        // 获取线程id
        String threadID = ID_PREFIX+Thread.currentThread().getId();
        // 获取锁
        Boolean success = stringRedisTemplate.opsForValue().setIfAbsent(KEY_PREFIX + name, threadID, timeoutSec, TimeUnit.SECONDS);
        // 防止null拆箱，null返回false
        return Boolean.TRUE.equals(success);
    }

    @Override
    public void unlock() {
        // 获取线程标识
        String threadID = ID_PREFIX+Thread.currentThread().getId();
        // 获取锁的值
        String value= stringRedisTemplate.opsForValue().get(KEY_PREFIX + name);
        if(threadID.equals(value)){
            // 释放锁
            stringRedisTemplate.delete(KEY_PREFIX + name);
        }

    }
}
