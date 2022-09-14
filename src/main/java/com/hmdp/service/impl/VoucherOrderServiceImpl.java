package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.springframework.aop.framework.AopContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

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
    ISeckillVoucherService iSeckillVoucherService;

    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Override
    public Result seckillVoucher(Long voucherId) {
        return seckillVoucherByPessimistic(voucherId);
    }

    @Transactional
    public Result seckillVoucherByPessimistic(Long voucherId) {
        SeckillVoucher seckillVoucher = iSeckillVoucherService.getById(voucherId);
        if (seckillVoucher.getBeginTime().isAfter(LocalDateTime.now()) || seckillVoucher.getEndTime().isBefore(LocalDateTime.now())){
            return Result.fail("该优惠券不在使用时间范围内!");
        }

        if(seckillVoucher.getStock() <= 0){
            return Result.fail("该优惠券库存不足!");
        }

        Long userId = UserHolder.getUser().getId();
        /*
            //单机版（JVM锁）
            synchronized (userId.toString().intern()){
            IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId,userId);
        }*/

        //集群版（分布式锁）
        SimpleRedisLock simpleRedisLock = new SimpleRedisLock("order:"+userId,stringRedisTemplate);
        if (!simpleRedisLock.tryLock(1200)){
            return Result.fail("一人只能下一单!");
        }
        try {
            IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId,userId);
        }finally {
            simpleRedisLock.unLock();
        }
    }

    @Transactional
    public Result createVoucherOrder(Long voucherId, Long userId) {

        Integer count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0){
            return Result.fail("一人只能下一单!");
        }
        boolean success = iSeckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock",0)
                .update();
        if (!success){
            return Result.fail("该优惠券库存不足!");
        }
        RedisIdWorker redisIdWorker = new RedisIdWorker(stringRedisTemplate);
        long orderId = redisIdWorker.nextId("order");
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(UserHolder.getUser().getId());
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);
        return Result.ok(orderId);
    }

    @Transactional
    public Result seckillVoucherByOptimistic(Long voucherId) {
        SeckillVoucher seckillVoucher = iSeckillVoucherService.getById(voucherId);
        if (seckillVoucher.getBeginTime().isAfter(LocalDateTime.now()) || seckillVoucher.getEndTime().isBefore(LocalDateTime.now())){
            return Result.fail("该优惠券不在使用时间范围内!");
        }

        if(seckillVoucher.getStock() <= 0){
            return Result.fail("该优惠券库存不足!");
        }

        boolean success = iSeckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock",0).update();
        if (!success){
            return Result.fail("该优惠券库存不足!");
        }
        RedisIdWorker redisIdWorker = new RedisIdWorker(stringRedisTemplate);
        long orderId = redisIdWorker.nextId("order");
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(UserHolder.getUser().getId());
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);
        return Result.ok(orderId);
    }
}
