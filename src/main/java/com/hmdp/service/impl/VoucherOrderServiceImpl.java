package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setResultType(Long.class);
        SECKILL_SCRIPT.setLocation(new ClassPathResource("lua/seckill.lua"));
    }

    private IVoucherOrderService proxy;

    // 阻塞队列
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHander());
    }

    private class VoucherOrderHander implements Runnable {

        String queueName = "stream.orders";

        @Override
        public void run() {
            while (true) {
                try {
                    // 1.从队列中获取订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS streams.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    // 2.判断消息是否获取成功
                    if (list.isEmpty() || list == null) {
                        // 如果获取失败，说明没有信息，继续下一次循环
                        continue;
                    }
                    // 3.成功后下单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 创建订单
                    handleVoucherOrder(voucherOrder);
                    // 4.ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                    handlePendingList();
                }
            }
        }
    }

    private void handlePendingList() {
        String queueName = "stream.orders";

        while (true) {
            try {
                // 1.从panding-list中获取订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS streams.order >
                List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                        Consumer.from("g1", "c1"),
                        StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                        StreamOffset.create(queueName, ReadOffset.lastConsumed())
                );
                // 2.判断消息是否获取成功
                if (list.isEmpty() || list == null) {
                    // 如果获取失败，说明panding-list没有信息，继续下一次循环
                    break;
                }
                // 3.成功后下单
                MapRecord<String, Object, Object> record = list.get(0);
                Map<Object, Object> value = record.getValue();
                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                // 创建订单
                handleVoucherOrder(voucherOrder);
                // 4.ACK确认 SACK stream.orders g1 id
                stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
            } catch (Exception e) {
                log.error("处理订单异常",e);
                try {
                    Thread.sleep(20);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        // 创建锁对象
//        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        // 无参默认获取锁失败不等待，锁超时时间30s
        if (!isLock){
            // 获取锁失败
            log.error("不允许重复下单");
            return;
        }
        try {
            // 获取代理对象（事务）
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            // 释放锁
            lock.unlock();
        }

    }

    @Override
    @Transactional
    public Result seckillVoucher(Long voucherId) {
        // 获取用户
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );

        // 2，判断结果为0
        int r = result.intValue();
        if (r != 0) {
            return Result.fail(r==1?"库存不足":"不能重复下单");
        }

        proxy = (IVoucherOrderService)AopContext.currentProxy();

        return Result.ok(orderId);
    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 5.实现一人一单
        Long userId = voucherOrder.getUserId();

        int count = query().eq("user_id", userId).eq("voucher_id",voucherOrder.getVoucherId()).count();
        if (count > 0){
            log.error("用户已经购买了一次");
            return;
        }

        // 6.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")    // set stock = stock - 1
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock",0)     // where id = ? and stock > 0
                .update();
        if(!success){
            log.error("库存不足");
            return ;
        }

        // 7.创建订单
        save(voucherOrder);
    }

/**
 * 版本2：使用阻塞队列
  */

//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024*1024);
//    private class VoucherOrderHander implements Runnable {
//        @Override
//        public void run() {
//            try {
//                while (true) {
//                    // 从队列中获取订单信息
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    // 创建订单
//                    handleVoucherOrder(voucherOrder);
//                }
//            } catch (InterruptedException e) {
//
//                log.error("处理订单异常",e);
//            }
//        }
//    }
//    @Override
//    @Transactional
//    public Result seckillVoucher(Long voucherId) {
//        // 获取用户
//        Long userId = UserHolder.getUser().getId();
//        // 1.执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(),
//                userId.toString()
//        );
//        // 2，判断结果为0
//        int r = result.intValue();
//        if (r != 0) {
//            return Result.fail(r==1?"库存不足":"不能重复下单");
//        }
//        // 2.2 结果为0，有购买资格，把下单信息保存到阻塞队列
//        long orderId = redisIdWorker.nextId("order");
//        // 创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        // 订单ID
//        voucherOrder.setId(orderId);
//        // 用户ID
//        Long userID = UserHolder.getUser().getId();
//        voucherOrder.setUserId(userID);
//        // 代金卷ID
//        voucherOrder.setVoucherId(voucherId);
//        // 放入阻塞队列
//        orderTasks.add(voucherOrder);
//
//        proxy = (IVoucherOrderService)AopContext.currentProxy();
//
//        return Result.ok(orderId);
//    }

/**
 * 版本1：不用消息队列
 */
//    @Override
//    @Transactional
//    public Result seckillVoucher(Long voucherId) {
//        // 1.查询优惠卷
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        // 2.判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())){
//            return Result.fail("秒杀尚未开始");
//        }
//        // 3.判断秒杀是否结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())){
//            return Result.fail("秒杀已经结束");
//        }
//        // 4.判断库存是否充足
//        if(voucher.getStock()<1){
//            return Result.fail("库存充足");
//        }
//
//        Long userId = UserHolder.getUser().getId();
//        // 创建锁对象
////        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        boolean isLock = lock.tryLock();
//        // 无参默认获取锁失败不等待，锁超时时间30s
//        if (!isLock){
//            // 获取锁失败
//            return Result.fail("不允许重复下单");
//        }
//        try {
//            // 获取代理对象（事务）
//            IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            // 释放锁
//            lock.unlock();
//        }
//    }

//    @Transactional
//    public Result createVoucherOrder(Long voucherId) {
//        // 5.实现一人一单
//        Long userId = UserHolder.getUser().getId();
//
//        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
//        if (count > 0){
//            return Result.fail("用户已经购买了一次");
//        }
//
//        // 6.扣减库存
//        boolean success = seckillVoucherService.update()
//                .setSql("stock = stock - 1")    // set stock = stock - 1
//                .eq("voucher_id", voucherId).gt("stock",0)     // where id = ? and stock > 0
//                .update();
//        if(!success){
//            return Result.fail("库存不足");
//        }
//
//        // 7.创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        // 订单ID
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        // 用户ID
//        Long userID = UserHolder.getUser().getId();
//        voucherOrder.setUserId(userID);
//        // 代金卷ID
//        voucherOrder.setVoucherId(voucherId);
//        save(voucherOrder);
//
//        // 8.返回订单ID
//        return Result.ok(orderId);
//    }
}
