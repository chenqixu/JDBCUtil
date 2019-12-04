package com.cqx.redis.impl;

import com.cqx.redis.bean.table.HashTable;
import com.cqx.redis.bean.table.HashTableQuery;
import com.cqx.redis.client.RedisClient;
import com.cqx.redis.comm.RedisConst;
import com.cqx.redis.jdbc.RedisResultSet;
import com.cqx.redis.utils.CommonUtils;
import com.cqx.redis.utils.StartsWithResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;

/**
 * DeleteRedisParser
 *
 * @author chenqixu
 */
public class DeleteRedisParser implements IRedisParser {

    private static final String SQL_KEY = RedisConst.KEY_DELETE;
    private static final String SQL_FROM = RedisConst.KEY_FROM;
    private static final String SQL_WHERE = RedisConst.KEY_WHERE_SPACE_LR;
    private static final Logger logger = LoggerFactory.getLogger(DeleteRedisParser.class);

    static {
        RedisHashSqlParser.registerParser(new DeleteRedisParser());
    }

    private RedisClient rc;
    private String sql;// 转小写的SQL
    private HashTable hashTable;
    private StartsWithResult from;
    private RedisWhereParser redisWhereParser;// where解析处理类

    /**
     * delete from tablename where field1=xx and field2=xx;
     *
     * @param sql
     * @return
     */
    @Override
    public boolean checkType(String sql) {
        // 保护引号或双引号内的值不做小写转换
        // 按引号切割，并去除左右空格
        this.sql = CommonUtils.conditionalProtection(sql, "'").trim();
        // 判断是否delete开头，替换delete，去除左右空格
        StartsWithResult delete = CommonUtils.startsWith(this.sql, SQL_KEY);
        boolean isDeleteStart = delete.isStartsWith();
        // 判断是否from开头
        from = CommonUtils.startsWith(delete.getStr(), SQL_FROM);
        boolean isFromStart = from.isStartsWith();
        return isDeleteStart && isFromStart;
    }

    @Override
    public void init(RedisClient rc, boolean isPrepared) throws SQLException {
        this.rc = rc;
        // 解析SQL，获取表名，条件
        // delete from tablename where field1='' and field2='';
        // delete from tablename where field1 in('','') and field2 in ('','');
        // delete from tablename where field1=? and field2=?;
        // delete from tablename where field1=:field1 and field2=:field2;
        String _sql = from.getStr();
        if (_sql.contains(SQL_WHERE)) {
            // 解析SQL
            // 初始化where解析处理类
            redisWhereParser = new RedisWhereParser(isPrepared);
            redisWhereParser.split(_sql);// 按" where "进行切割，获取表名和查询条件

            // 获取表定义
            hashTable = redisWhereParser.getHashTable();

            // 解析查询条件，根据查询条件循环插入表定义hashTableDef的hashTableQueryList
            redisWhereParser.parser();
            // 条件校验，条件只能在def_field、def_key范围内
            redisWhereParser.checkWhere();
        } else {
            throw new SQLException("sql删除语句不包含where关键字，请检查！");
        }
    }

    /**
     * PreparedStatement中，用于按此字段顺序拼接条件
     *
     * @return
     */
    @Override
    public String[] getFieldsArr() {
        return redisWhereParser.getWhereKeyArr();
    }

    /**
     * 实现hdel或del
     *
     * @param _fieldMap
     * @return
     */
    @Override
    public int run(Map<String, String> _fieldMap) throws SQLException {
        int ret = 0;
        // 拼接所有的条件，确认使用hgetAll还是hget
        redisWhereParser.connectingCondition(_fieldMap);
        // 循环查询条件
        for (HashTableQuery hashTableQuery : hashTable.getHashTableQueryList()) {
            if (hashTableQuery.isHgetAll()) {
                // 多行记录
                logger.debug("del，field：{}", hashTableQuery.getField());
                long _ret = rc.del(hashTableQuery.getField());
                ret = ret + (int) _ret;
            } else {
                // 一行记录
                logger.debug("del，field：{}，key：{}", hashTableQuery.getField(), hashTableQuery.getKey());
                long _ret = rc.hdel(hashTableQuery.getField(), hashTableQuery.getKey());
                ret = ret + (int) _ret;
            }
        }
        return ret;
    }

    @Override
    public RedisResultSet getRedisResultSet() {
        return null;
    }

    @Override
    public HashTable getHashTable() {
        return hashTable;
    }

    @Override
    public void close() {

    }

}
