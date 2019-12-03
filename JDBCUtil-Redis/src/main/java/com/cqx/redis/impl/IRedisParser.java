package com.cqx.redis.impl;

import com.cqx.redis.bean.table.HashTable;
import com.cqx.redis.client.RedisClient;
import com.cqx.redis.jdbc.RedisResultSet;

import java.sql.SQLException;
import java.util.Map;

/**
 * IRedisParser
 *
 * @author chenqixu
 */
public interface IRedisParser {
    boolean checkType(String sql);

    void init(RedisClient rc, boolean isPrepared) throws SQLException;

    String[] getFieldsArr();

    int run(Map<String, String> _fieldMap) throws SQLException;

    RedisResultSet getRedisResultSet();

    HashTable getHashTable();

    void close();
}
