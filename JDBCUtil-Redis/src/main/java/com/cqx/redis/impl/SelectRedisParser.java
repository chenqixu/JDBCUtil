package com.cqx.redis.impl;

import com.alibaba.fastjson.JSON;
import com.cqx.redis.bean.table.HashTable;
import com.cqx.redis.bean.table.HashTableConstant;
import com.cqx.redis.bean.table.HashTableQuery;
import com.cqx.redis.client.RedisClient;
import com.cqx.redis.comm.RedisConst;
import com.cqx.redis.jdbc.RedisResultSet;
import com.cqx.redis.jdbc.RedisResultSetMetaData;
import com.cqx.redis.jdbc.RedisRowData;
import com.cqx.redis.utils.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SelectRedisParser
 *
 * @author chenqixu
 */
public class SelectRedisParser implements IRedisParser {

    private static final String SQL_KEY = RedisConst.KEY_SELECT_SPACE_R;
    private static final String SQL_FROM = RedisConst.KEY_FROM_SPACE_LR;
    private static final String SQL_WHERE = RedisConst.KEY_WHERE_SPACE_LR;
    private static final Logger logger = LoggerFactory.getLogger(SelectRedisParser.class);

    static {
        RedisHashSqlParser.registerParser(new SelectRedisParser());
    }

    private RedisClient rc;
    private String sql;// 转小写的SQL
    private RedisResultSet redisResultSet;
    private RedisResultSetMetaData redisResultSetMetaData;// 元数据
    private HashTable hashTable;
    private boolean isSetRedisHash = false;// 是否不用redisResultSet，而是存入redisHashResult中
    private Map<String, Map<String, String>> redisHashResult = new HashMap<>();
    private boolean isPrepared;// 是否预编译SQL
    private RedisWhereParser redisWhereParser;// where解析处理类

    @Override
    public boolean checkType(String sql) {
        // 保护引号或双引号内的值不做小写转换
        // 按引号切割，并去除左右空格
        this.sql = CommonUtils.conditionalProtection(sql, "'").trim();
        // 处理后，判断是否以select开头
        return this.sql.startsWith(SQL_KEY);
    }

    @Override
    public void init(RedisClient rc, boolean isPrepared) throws SQLException {
        this.rc = rc;
        this.isPrepared = isPrepared;
        // 解析SQL，获取 表名、字段、查询条件
        // 其中查询条件必须全部包含def_field，且只支持and关系
        // select field1,field2,field3 from table_name where field1='' and field2='' and field3=''
        // select field1,field2,field3 from table_name where field1 in('','') and field2 in('','') and field3 in('','')
        // select field1,field2,field3 from table_name where field1=? and field2=? and field3=?
        // select field1,field2,field3 from table_name where field1=:field1 and field2=:field2 and field3=:field3
        String _sql = sql;
        if (_sql.contains(SQL_FROM) && _sql.contains(SQL_WHERE)) {
            // 解析SQL
            _sql = _sql.replaceFirst(SQL_KEY, "").trim();// 替换掉开头的"select "，并去除左右空格
            String[] _sql_arr = _sql.split(SQL_FROM, -1);// 按" from "进行切割
            String _fields = _sql_arr[0].trim();// 获取查询字段，并去除左右空格：field1,field2,field3

            // 初始化where解析处理类
            redisWhereParser = new RedisWhereParser(isPrepared);
            redisWhereParser.split(_sql_arr[1]);// 按" where "进行切割，获取表名和查询条件

            // 获取表定义
            hashTable = redisWhereParser.getHashTable();
            // 校验字段是否和表定义一致
            HashTableConstant.checkFields(_fields, hashTable, "查询");
            // 设置查询字段
            hashTable.setQuery_fields(_fields);

            // 初始化结果
            redisResultSet = new RedisResultSet();

            // 设置MetaData
            redisResultSetMetaData = new RedisResultSetMetaData(hashTable);
            redisResultSet.setRedisResultSetMetaData(redisResultSetMetaData);

            // 解析查询条件，根据查询条件循环插入表定义hashTableDef的hashTableQueryList
            redisWhereParser.parser();
            // 条件校验，条件只能在def_field、def_key范围内
            redisWhereParser.checkWhere();
        } else {
            throw new SQLException("sql查询语句不包含from或where关键字，请检查！");
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
     * 实现hget
     *
     * @param _fieldMap
     * @return
     */
    @Override
    public int run(Map<String, String> _fieldMap) throws SQLException {
        // 拼接所有的条件，确认使用hgetAll还是hget
        redisWhereParser.connectingCondition(_fieldMap);
        // 循环查询条件
        for (HashTableQuery hashTableQuery : hashTable.getHashTableQueryList()) {
            if (hashTableQuery.isHgetAll()) {
                // 多行记录
                Map<String, String> map = rc.hgetAll(hashTableQuery.getField());
                for (Map.Entry<String, String> tmp : map.entrySet()) {
                    String queryValue = tmp.getValue();
                    if (queryValue != null && queryValue.length() > 0) {
                        logger.debug("hgetAll：field：{}，key：{}，value：{}", hashTableQuery.getField(), tmp.getKey(), queryValue);
                        valueToRS(hashTableQuery.getField(), tmp.getKey(), queryValue);
                    }
                }
            } else {
                // 一行记录
                String queryValue = rc.hget(hashTableQuery.getField(), hashTableQuery.getKey());
                if (queryValue != null && queryValue.length() > 0) {
                    logger.debug("hget：field：{}，key：{}，value：{}", hashTableQuery.getField(), hashTableQuery.getKey(), queryValue);
                    valueToRS(hashTableQuery.getField(), hashTableQuery.getKey(), queryValue);
                }
            }
        }
        return 0;
    }

    /**
     * 值设置到RS
     *
     * @param field
     * @param key
     * @param queryValue
     */
    private void valueToRS(String field, String key, String queryValue) {
        // JSON to Map
        Map<String, Object> queryMap = JSON.parseObject(queryValue);
        if (isSetRedisHash) {// 到Map
            // 新的一行
            Map<String, String> valueMap = new HashMap<>();
            // 按查询字段顺序插入
            for (String _field : hashTable.getQuery_fields_arr()) {
                String _value = queryMap.get(_field) == null ? null : queryMap.get(_field).toString();
                valueMap.put(_field, _value);
            }
            redisHashResult.put(field, valueMap);
        } else {// 到ResultSet
            // 生成新的一行
            List<RedisRowData> newRow = redisResultSet.newRow();
            // 按查询字段顺序插入
            for (String _field : hashTable.getQuery_fields_arr()) {
                redisResultSet.addColumnData(newRow, new RedisRowData(queryMap.get(_field)));
            }
        }
    }

    @Override
    public RedisResultSet getRedisResultSet() {
        return redisResultSet;
    }

    public void setSetRedisHash(boolean setRedisHash) {
        isSetRedisHash = setRedisHash;
    }

    public Map<String, Map<String, String>> getRedisHashResult() {
        return redisHashResult;
    }

    /**
     * 返回查询字段个数
     *
     * @return
     */
    public int getQuery_fields_len() {
        return hashTable.getQuery_fields_arr().length;
    }

    @Override
    public HashTable getHashTable() {
        return hashTable;
    }

    @Override
    public void close() {

    }

}
