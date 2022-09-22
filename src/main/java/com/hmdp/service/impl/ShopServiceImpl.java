package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;




    @Override
    public Result queryById(Long id) throws InterruptedException {
        Shop shop = queryWithMutex(id);
        if (shop == null){
            return Result.fail("店铺不存在！");
        }else {
            return Result.ok(shop);
        }
    }

    /**
     * 缓存穿透，设置空缓存
     * @param id
     * @return
     */
    public Shop queryWithPassThrough(Long id) {
        return cacheClient.queryWithPassThrough(CACHE_SHOP_KEY + id, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
    }

    /**
     * 缓存击穿，设置互斥锁
     * @param id
     * @return
     */
    public Shop queryWithMutex(Long id) throws InterruptedException {
        return cacheClient.queryWithMutex(CACHE_SHOP_KEY,id,Shop.class,id2->getById(id2),CACHE_SHOP_TTL,TimeUnit.MINUTES);
    }

    /**
     * 缓存击穿，设置逻辑过期
     * @param id
     * @return
     */
    public Shop queryWithLogicalExpire(Long id) {
        return cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY,LOCK_SHOP_KEY,id,Shop.class,this::getById,LOCK_SHOP_TTL);
    }




    /**
     * 解决数据双读不一致（先查库再删redis）
     * @param shop
     * @return
     */
    @Override
    public Result update(Shop shop) {
        updateById(shop);
        stringRedisTemplate.delete(CACHE_SHOP_KEY+shop.getId());
        return Result.ok();
    }
}
