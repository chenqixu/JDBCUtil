package com.cqx.redis.client;

import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface RedisClient {
	static final int DEFUALT_MAX_IDLE = 10;
	static final int DEFUALT_MAX_TOTAL = 100;
	static final int DEFUALT_MAX_WAIT_MILLIS = 3000;
	public String set(String key, String value);
	public boolean setnx(String key, String value);
	public boolean setnx(String key, String value, Integer seconds);
	public Long del(String key);
	public Long hdel(String key, String field);
	public Long hset(String key, String field, String value);
	public Long hsetnx(String key, String field, String value);
    public String get(String key) throws SQLException;
	public String hget(String key, String field) throws SQLException;
	public Map<String, String> hgetAll(String key) throws SQLException;
	public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor) throws SQLException;
	public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor, ScanParams params) throws SQLException;
	public void close() throws SQLException;// 事务启动后，在close必须检测事务是否有关闭
	public void startTransaction();// 启动事务
	public void commit();// 提交
	public void rollback();// 回滚
	public List<Object> getTransactionResult();// 获取上一次事务的返回值，并清空
}
