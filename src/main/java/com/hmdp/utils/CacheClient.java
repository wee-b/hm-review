package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@Component
public class CacheClient {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    // 线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        // 设置逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R,ID> R queryWithPassThrough(String prefex, ID id, Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit) {
        // 1.从redis查询商品缓存
        String key = prefex + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断
        if(StrUtil.isNotBlank(json)){
            // 3.存在，直接返回
            return JSONUtil.toBean(json, type);
        }
        if(json != null){
            return null;
        }

        // 4.不存在。查询数据库
        R r = dbFallback.apply(id);
        // 5.不存在则写入空值
        if(r == null){
            stringRedisTemplate.opsForValue().set(key, "",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        // 6.存在则更新缓存
        this.set(key, r, time, unit);

        return r;
    }

    private Boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key,"1",10,TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    /**
     *
     * @param prefex        缓存key的前缀
     * @param lockPrefix    互斥锁id的前缀
     * @param id
     * @param type
     * @param dbFallback    查询mysql数据库的函数
     * @param time          redis缓存过期时间
     * @param unit          上面时间的单位
     * @return
     * @param <R>           返回值类型
     * @param <ID>          查询id的类型
     */
    public <R,ID> R queryWithMutex(String prefex,String lockPrefix, ID id, Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit) {
        // 1.从redis查询商品缓存
        String key = prefex + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断
        if(StrUtil.isNotBlank(json)){
            // 3.存在，直接返回
            R r = JSONUtil.toBean(json, type);
            return r;
        }
        // 判断命中的是否是空值
        if(json != null){
            return null;
        }

        // 实现缓存重建
        // 4.1 获取互斥锁
        String lockKey = lockPrefix + id;
        R r = null;
        try {
            boolean isLock = tryLock(lockKey);
            if(!isLock){
                // 4.2失败，则休眠并重试
                Thread.sleep(50);
                queryWithMutex(prefex,lockPrefix,id,type,dbFallback,time,unit);
            }

            // 4.3成功,根据id查询数据库
            r = dbFallback.apply(id);
            // 5.不存在,返回错误
            if (r == null) {
                // 将空值写入redis
                stringRedisTemplate.opsForValue().set(key,"",time,unit);
                return null;
            }
            // 6.存在，写入redis
            // 解决缓存雪崩问题：为ttl设置随机数，范围是-5~5
            long ttl = time + ThreadLocalRandom.current().nextInt(-5, 6);
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(r),ttl, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 7.释放互斥锁
            unlock(lockKey);
        }

        return r;
    }

    /**
     *  使用逻辑删除缓存要预热(提前加载一遍)缓存
     */
    public <R,ID> R queryWithLogicalExpire(String prefex,String lockPrefix,ID id,Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit) {
        // 1.从redis查询商品缓存
        String key = prefex + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断
        if(StrUtil.isNotBlank(json)){
            return null;
        }
        // 4.命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(),type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            // 未过期直接返回
            return r;
        }
        // 6.过期则缓存重建
        // 6.1获取互斥锁
        String lockKey = lockPrefix + id;
        boolean isLock = tryLock(lockKey);
        if (isLock) {
            // 锁获取成功应再次检查redis缓存是否过期

            // 开启独立线程实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    R apply = dbFallback.apply(id);
                    this.setWithLogicalExpire(key, apply, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {// 释放锁
                    unlock(lockKey);
                }
            });
        }

        return r;
    }
}
