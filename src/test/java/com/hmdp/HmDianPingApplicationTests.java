package com.hmdp;

import com.hmdp.config.RedissonConfig;
import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    RedisIdWorker redisIdWorker;

    @Resource
    RedissonConfig redissonConfig;

    ExecutorService es = Executors.newFixedThreadPool(500);

    @Resource
    IShopService shopService;

    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Test
    void testIdWorker() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(300);
        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                System.out.println(redisIdWorker.nextId("order"));

            }
            latch.countDown();
        };
        long start = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("time="+(end-start));
    }

    @Test
    void testRedisson() throws InterruptedException {
        RedissonClient client = redissonConfig.redissonClient();
        RLock lock = client.getLock("login2");
        boolean b = lock.tryLock(10000, TimeUnit.SECONDS);
        System.out.println(b);

    }

    @Test
    void loadGEO() throws InterruptedException {
        List<Shop> shopList = shopService.list();
        Map<Long, List<Shop>> listMap = shopList.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        for (Map.Entry<Long, List<Shop>> entry : listMap.entrySet()) {
            List<Shop> shops = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(shops.size());
            for (Shop shop : shops) {
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),
                        new Point(shop.getX(),shop.getY())
                ));
//                stringRedisTemplate.opsForGeo().add(SHOP_GEO_KEY+entry.getKey(),new Point(shop.getX(),shop.getY()),shop.getId().toString());
            }
            stringRedisTemplate.opsForGeo().add(SHOP_GEO_KEY+entry.getKey(),locations);

        }
    }

}
