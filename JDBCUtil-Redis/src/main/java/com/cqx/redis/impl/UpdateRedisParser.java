package com.cqx.redis.impl;

import com.cqx.redis.bean.table.*;
import com.cqx.redis.client.RedisClient;
import com.cqx.redis.jdbc.RedisResultSet;
import com.cqx.redis.utils.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;

/**
 * UpdateRedisParser
 * <pre>
 *     1、更新暂时不允许有笛卡尔积的情况出现，否则代码不好控制
 *     2、更新必须有key
 * </pre>
 *
 * @author chenqixu
 */
public class UpdateRedisParser implements IRedisParser {

    private static final String SQL_KEY = "update ";
    private static final String SQL_SET = " set ";
    private static final String SQL_WHERE = " where ";
    private static final String SQL_EQUAL = "=";
    private static final Logger logger = LoggerFactory.getLogger(UpdateRedisParser.class);

    static {
        RedisHashSqlParser.registerParser(new UpdateRedisParser());
    }

    private RedisClient rc;
    private String sql;// 转小写的SQL
    private HashTable hashTable;
    private HashTableUpdate hashTableUpdate;// 更新对象
    private RedisWhereParser redisWhereParser;// where解析处理类
    private boolean isPrepared;

    @Override
    public boolean checkType(String sql) {
        // 保护引号或双引号内的值不做小写转换
        // 按引号切割，并去除左右空格
        this.sql = CommonUtils.conditionalProtection(sql, "'").trim();
        // 处理后，判断是否以update开头
        return this.sql.startsWith(SQL_KEY);
    }

    @Override
    public void init(RedisClient rc, boolean isPrepared) throws SQLException {
        this.rc = rc;
        this.isPrepared = isPrepared;
        // 解析SQL，获取表名，条件
        // update tablename set field3='',field4='' where field1='' and field2='';
        // update tablename set field3=?,field4=? where field1=? and field2=?;
        // update tablename set field3=:field3,field4=:field4 where field1=:field1 and field2=:field2;
        String noupdate_sql = sql.replaceFirst(SQL_KEY, "").trim();// 替换掉开头的update，并去左右空格
        String[] set_arr = noupdate_sql.split(SQL_SET, -1);// 以" set "进行分割
        // 校验set关键字
        if (set_arr.length != 2) throw new SQLException("update语句没有set关键字，请检查");
        String tableName = set_arr[0].trim();// 获取表名
        String[] left_arr = set_arr[1].trim().split(SQL_WHERE, -1);// 去左右空格后以" where "进行分割
        // 校验where关键字
        if (left_arr.length != 2) throw new SQLException("update语句没有where关键字，请检查");
        String update_fields = left_arr[0].trim();// 获取更新字段，并去左右空格

        // 拼接 tablename where field1=xx and field2=xx;语句
        String _where_sql = tableName + SQL_WHERE + left_arr[1].trim();

        // 初始化where解析处理类，更新不允许有笛卡尔积的情况出现，更新必须有key
        redisWhereParser = new RedisWhereParser(false, true, isPrepared);
        redisWhereParser.split(_where_sql);// 按" where "进行切割，获取表名和查询条件

        // 获取表定义
        hashTable = redisWhereParser.getHashTable();

        // 处理更新字段：field3=xx,field4=xx
        parserUpdateFields(update_fields);

        // 解析查询条件，根据查询条件循环插入表定义hashTableDef的hashTableQueryList
        redisWhereParser.parser();
        // 条件校验，条件只能在def_field、def_key范围内
        redisWhereParser.checkWhere();
    }

    /**
     * 处理更新字段
     *
     * @param update_fields
     * @throws SQLException
     */
    private void parserUpdateFields(String update_fields) throws SQLException {
        // 处理更新字段
        // statement：field3='',field4=''
        // preparedStatement：field3=?,field4=?
        // preparedStatement：field3=:field3,field4=:field4
//        Map<String, String> updateFieldsMap = new HashMap<>();
        HashTableFieldMap updateFieldsMap = new HashTableFieldMap();
        String[] update_fields_arr = update_fields.split(",", -1);// 按逗号分隔
        // 按"="分隔，重新拼凑一个做为校验字段
        StringBuffer sb = new StringBuffer();
        for (String _update_field : update_fields_arr) {
            String[] _field_arr = _update_field.split(SQL_EQUAL, -1);
            if (_field_arr.length != 2) throw CommonUtils.createSQLException("字段后面必须带上更新的值");
            sb.append(_field_arr[0]).append(",");
            if (isPrepared) {// PreparedStatement
                updateFieldsMap.put(_field_arr[0], "");
            } else {// Statement
                // 这里是直接设置filedValueStr
                updateFieldsMap.put(new HashTableField(_field_arr[0], _field_arr[1], hashTable.getRedisColumnByName(_field_arr[0])));
            }
        }
        if (sb.length() > 0) sb.deleteCharAt(sb.length() - 1);
        // 校验更新字段是否在定义内
        HashTableConstant.checkFields(sb.toString(), hashTable);
        // 根据更新的字段和值，以及表定义，生成HashTableUpdate对象
        hashTableUpdate = new HashTableUpdate(updateFieldsMap, hashTable, sb.toString());
    }

    /**
     * PreparedStatement中，用于按此字段顺序拼接条件
     *
     * @return
     */
    @Override
    public String[] getFieldsArr() {
        return CommonUtils.appendStrArray(hashTableUpdate.getUpdateFields(), redisWhereParser.getWhereKeyArr());
    }

    /**
     * 实现hset或者是组合方式：hdel+hset
     * <pre>
     *     涉及更新def_field，就是组合方式：hdel+hset
     *     涉及更新value，就是hset
     * </pre>
     *
     * @param _fieldMap
     * @return
     */
    @Override
    public int run(Map<String, String> _fieldMap) throws SQLException {
        int ret = 0;
        // 拼接所有的条件，确认使用hgetAll还是hget
        redisWhereParser.connectingCondition(_fieldMap);
        // 重新设置更新的值，PreparedStatement
        if (isPrepared) {
            // 循环更新的字段Map，把值设置进去
            Map<String, String> _updateMap = hashTableUpdate.getUpdateFieldsMap();
            for (Map.Entry<String, String> entry : _updateMap.entrySet()) {
                _updateMap.put(entry.getKey(), _fieldMap.get(entry.getKey()));
            }
        }
        // 实际上List只有一个子对象
        for (HashTableQuery hashTableQuery : hashTable.getHashTableQueryList()) {
            if (hashTableQuery.isHgetAll()) {
                // 多行记录，不正常，在上面有做不允许笛卡尔积的校验了
                throw new SQLException("更新的条件不允许有笛卡尔积，请检查");
            } else {
                // 一行记录
                String queryValue = rc.hget(hashTableQuery.getField(), hashTableQuery.getKey());
                if (queryValue != null && queryValue.length() > 0) {
                    // 设置旧值
                    hashTableUpdate.setOldValue(hashTableQuery.getField(), hashTableQuery.getKey(), queryValue);
                    logger.debug("hget：field：{}，key：{}，value：{}，isUpdateField：{}，isUpdateValue：{}",
                            hashTableQuery.getField(), hashTableQuery.getKey(), queryValue,
                            hashTableUpdate.isUpdateField(), hashTableUpdate.isUpdateValue());
                    // 判断是要hset还是hdel+hset
                    if (hashTableUpdate.isUpdateField() && hashTableUpdate.isUpdateValue()) {
                        logger.debug("hdel，getField_old：{}，getKey：{}；hset，getField_new：{}，getValue_new：{}",
                                hashTableUpdate.getField_old(), hashTableUpdate.getKey(),
                                hashTableUpdate.getField_new(), hashTableUpdate.getValue_new());
                        long hdel_ret = rc.hdel(hashTableUpdate.getField_old(), hashTableUpdate.getKey());
                        long hset_ret = rc.hset(hashTableUpdate.getField_new(), hashTableUpdate.getKey(), hashTableUpdate.getValue_new());
                        ret = (int) hdel_ret + (int) hset_ret;
                    } else if (hashTableUpdate.isUpdateField()) {
                        logger.debug("hdel，getField_old：{}，getKey：{}；hset，getField_new：{}",
                                hashTableUpdate.getField_old(), hashTableUpdate.getKey(), hashTableUpdate.getField_new());
                        long hdel_ret = rc.hdel(hashTableUpdate.getField_old(), hashTableUpdate.getKey());
                        long hset_ret = rc.hset(hashTableUpdate.getField_new(), hashTableUpdate.getKey(), queryValue);
                        ret = (int) hdel_ret + (int) hset_ret;
                    } else if (hashTableUpdate.isUpdateValue()) {
                        logger.debug("hset，getField_old：{}，getKey：{}，getValue_new：{}",
                                hashTableUpdate.getField_old(), hashTableUpdate.getKey(), hashTableUpdate.getValue_new());
                        long hset_ret = rc.hset(hashTableUpdate.getField_old(), hashTableUpdate.getKey(), hashTableUpdate.getValue_new());
                        ret = (int) hset_ret;
                    } else {
                        logger.debug("不需要更新");
                    }
                }
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
