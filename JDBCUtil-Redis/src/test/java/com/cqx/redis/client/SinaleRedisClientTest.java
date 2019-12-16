package com.cqx.redis.client;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinaleRedisClientTest {
    private static final Logger logger = LoggerFactory.getLogger(SinaleRedisClientTest.class);
    private RedisClient rc;
    private String key;

    @Before
    public void setUp() throws Exception {
        rc = RedisFactory.builder()
                .setMode(RedisFactory.SINGLE_MODE_TYPE)
                .setIp("10.1.4.185")
                .setPort(6380)
                .build();
        key = "cqx1";
    }

    @After
    public void tearDown() throws Exception {
        rc.close();
    }

    @Test
    public void transaction() throws Exception {
        rc.startTransaction();// 启动事务
        rc.set(key, "abc123");
//        rc.commit();// 提交事务
        rc.rollback();// 回滚事务
//        for (Object object : rc.getTransactionResult()) {// 事务执行结果
//            logger.info("transaction_result：{}", object);
//        }
        query();
    }

    @Test
    public void query() throws Exception {
        String result = rc.get(key);
        logger.info("result：{}", result);
    }
}