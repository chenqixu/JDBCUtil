package com.cqx.redis.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Transaction;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SinaleRedisClient implements RedisClient {
    private static final Logger logger = LoggerFactory.getLogger(SinaleRedisClient.class);
    private Jedis jedis;
    private boolean autoCommit = true;// 默认不开事务
    private List<Object> transactionResult;// 事务执行结果
    private Transaction transaction;// 事务
    private boolean isTransactionCommit = false;// 事务是否提交

    public SinaleRedisClient(RedisFactory.Builder builder) {
        jedis = new Jedis(builder.getIp(), builder.getPort());
    }

    @Override
    public String set(String key, String value) {
        if (!autoCommit) {
            transaction.set(key, value);
            return "transaction set waiting";
        } else {
            return jedis.set(key, value);
        }
    }

    public boolean setnx(String key, String value) {
        if (!autoCommit) {
            transaction.setnx(key, value);
            return false;
        } else {
            return setnx(key, value, null);
        }
    }

    @Override
    public boolean setnx(String key, String value, Integer seconds) {
        long ret = jedis.setnx(key, value);
        logger.debug("[setnx ret]：{}", ret);
        if (ret == 0) {// 原先逻辑没有设置成功，就把记录失效seconds
//		if(ret == 1 && seconds != null){// 我的逻辑，如果设置成功，就把记录失效seconds
            long eret = jedis.expire(key, seconds);
            logger.debug("[expire ret]：{}", eret);
        }
        return ret == 1;
    }

    @Override
    public Long del(String key) {
        if (!autoCommit) {
            transaction.del(key).get();
            return 0L;
        } else {
            return jedis.del(key);
        }
    }

    @Override
    public Long hdel(String key, String field) {
        if (!autoCommit) {
            transaction.hdel(key, field).get();
            return 0L;
        } else {
            return jedis.hdel(key, field);
        }
    }

    @Override
    public Long hset(String key, String field, String value) {
        if (!autoCommit) {
            transaction.hset(key, field, value).get();
            return 0L;
        } else {
            return jedis.hset(key, field, value);
        }
    }

    @Override
    public Long hsetnx(String key, String field, String value) {
        if (!autoCommit) {
            transaction.hsetnx(key, field, value).get();
            return 0L;
        } else {
            return jedis.hsetnx(key, field, value);
        }
    }

    @Override
    public String get(String key) throws SQLException {
        queryCheck();// 查询前的事务检查
        return jedis.get(key);
    }

    @Override
    public String hget(String key, String field) throws SQLException {
        queryCheck();// 查询前的事务检查
        return jedis.hget(key, field);
    }

    @Override
    public Map<String, String> hgetAll(String key) throws SQLException {
        queryCheck();// 查询前的事务检查
        return jedis.hgetAll(key);
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor) throws SQLException {
        queryCheck();// 查询前的事务检查
        return jedis.hscan(key, cursor);
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor, ScanParams params) throws SQLException {
        queryCheck();// 查询前的事务检查
        return jedis.hscan(key, cursor, params);
    }

    @Override
    public void close() throws SQLException {
        if (!autoCommit && transaction != null) {
            try {
                transaction.close();
            } catch (IOException e) {
                throw new SQLException("事务关闭IO异常：" + e.getMessage());
            }
        }
        jedis.close();
    }

    /**
     * 开启事务
     */
    @Override
    public void startTransaction() {
        autoCommit = false;
        isTransactionCommit = false;
        transactionResult = new ArrayList<>();
        transaction = jedis.multi();
    }

    /**
     * 提交事务
     */
    @Override
    public void commit() {
        if (!autoCommit) {
            transactionResult = transaction.exec();
            isTransactionCommit = true;
        }
    }

    /**
     * 回滚事务
     */
    @Override
    public void rollback() {
        if (!autoCommit) {
            transaction.discard();
            isTransactionCommit = true;
        }
    }

    /**
     * 获取上一次事务的返回值，并清空
     *
     * @return
     */
    public List<Object> getTransactionResult() {
        List<Object> lastReplayList = new ArrayList<>();
        for (Object object : transactionResult) {
            lastReplayList.add(object);
        }
        transactionResult.clear();
        return lastReplayList;
    }

    /**
     * 查询前的事务检查
     *
     * @throws SQLException
     */
    private void queryCheck() throws SQLException {
        if (!autoCommit && !isTransactionCommit) {// 如果有开事务，得等事务提交后才能查询
            throw new SQLException("事务未提交不允许查询，请检查！");
        }
    }
}
