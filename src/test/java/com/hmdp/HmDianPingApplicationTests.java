package com.hmdp;

import com.hmdp.config.RedissonConfig;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    RedisIdWorker redisIdWorker;

    @Resource
    RedissonConfig redissonConfig;

    ExecutorService es = Executors.newFixedThreadPool(500);

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

}
