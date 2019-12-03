package com.cqx.redis.impl;

import com.cqx.redis.bean.table.HashTable;
import com.cqx.redis.bean.table.HashTableConstant;
import com.cqx.redis.bean.table.HashTableField;
import com.cqx.redis.bean.table.HashTableFieldMap;
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
 * InsertIntoRedisParser
 *
 * @author chenqixu
 */
public class InsertIntoRedisParser implements IRedisParser {

    private static final String SQL_KEY = RedisConst.KEY_INSERT;
    private static final String SQL_INTO = RedisConst.KEY_INTO;
    private static final String SQL_VALUES = RedisConst.KEY_VALUES;
    private static final String SQL_SELECT = RedisConst.KEY_SELECT_SPACE;
    private static final Logger logger = LoggerFactory.getLogger(InsertIntoRedisParser.class);

    static {
        RedisHashSqlParser.registerParser(new InsertIntoRedisParser());
    }

    private RedisClient rc;
    private String tableName;
    private String sql;
    private HashTable hashTable;
    private boolean isPrepared;
    private boolean isSelect = false;// 值是从select来，默认是false
    private SelectRedisParser selectRedisParser;
    private HashTableFieldMap statementFieldMap;

    /**
     * 严谨一点，先校验insert，然后校验后面是不是紧跟着into
     *
     * @param sql
     * @return
     */
    @Override
    public boolean checkType(String sql) {
        // 保护引号或双引号内的值不做小写转换
        // 按引号切割，并去除左右空格
        this.sql = CommonUtils.conditionalProtection(sql, "'").trim();
        // 判断是否insert开头，替换insert，去除左右空格
        StartsWithResult insert = CommonUtils.startsWith(this.sql, SQL_KEY);
        boolean isInsertStart = insert.isStartsWith();
        // 判断是否into开头
        StartsWithResult into = CommonUtils.startsWith(insert.getStr(), SQL_INTO);
        boolean isIntoStart = into.isStartsWith();
        return isInsertStart && isIntoStart;
    }

    @Override
    public void init(RedisClient rc, boolean isPrepared) throws SQLException {
        this.rc = rc;
        this.isPrepared = isPrepared;
        // 解析SQL，获取插入字段，表名
        // insert into table_name(field1,field2,field3) values(:field1,:field2,:field3);
        // insert into table_name(field1,field2,field3) values(?,?,?);
        // insert into table_name(field1,field2,field3) values('','','');
        // insert into table_name(field1,field2,field3) select field1,field2,field3 from table_name_other;
        // 获取table_name后values之前
        String _insert_into_fields = "";// 插入的字段
        String[] _sql_arr;// 按values或" select "进行分割后的结果
        String _sql = sql;
        // 解析SQL
        _sql = _sql.replaceFirst(SQL_KEY, "").trim();// 替换掉开头的insert，并去除左右空格
        _sql = _sql.replaceFirst(SQL_INTO, "").trim();// 替换掉开头的into，并去除左右空格
        // 可能是values也可能是select
        // 判断有没select，没有就算values，因为" select "比较好匹配
        if (_sql.contains(SQL_SELECT)) {// select
            isSelect = true;
            _sql_arr = _sql.split(SQL_SELECT, -1);// 按" select "进行分割
            if (_sql_arr.length != 2) throw CommonUtils.createSQLException("按 select 解析出现了问题");
            // 1：table_name(field1,field2,field3)
            // 2：field1,field2,field3 from table_name_other
            String _select_left = SQL_SELECT + _sql_arr[1].trim();// field1,field2,field3 from table_name_other
            selectRedisParser = new SelectRedisParser();
            selectRedisParser.checkType(_select_left);
            selectRedisParser.setSetRedisHash(true);
            selectRedisParser.init(rc, false);
        } else {// values
            _sql_arr = _sql.split(SQL_VALUES, -1);// 按values进行分割
            if (_sql_arr.length != 2) throw CommonUtils.createSQLException("按values解析出现了问题");
            // 1：table_name(field1,field2,field3)
            // 2：values(:field1,:field2,:field3) --PreparedStatement
            // 2：values(?,?,?) --PreparedStatement
            // 2：values('','','') --Statement
        }
        String _tablename_field = _sql_arr[0];// 取出table_name(field1,field2,field3)
        _tablename_field = _tablename_field.replace(")", "");// 替换掉右边的括号
        String[] _tablename_field_arr = _tablename_field.split("\\(", -1);// 按左括号进行分割
        tableName = _tablename_field_arr[0].trim();
        _insert_into_fields = _tablename_field_arr[1].trim();

        // 通过表名获取表定义
        hashTable = HashTableConstant.getHashTableByName(tableName);
        // 设置插入字段
        hashTable.setQuery_fields(_insert_into_fields);

        // 如果是select，需要对前后字段做个数校验
        if (isSelect) {
            if (hashTable.getQuery_fields_arr().length != selectRedisParser.getQuery_fields_len()) {
                throw CommonUtils.createSQLException("插入字段个数和查询字段个数不一致");
            }
        }

        if (!isPrepared && !isSelect) {// Statement
            statementFieldMap = new HashTableFieldMap();
            // 解析values里的内容，需要支持select
            // insert into table_name(field1,field2,field3) values('','','');
            String _values = _sql_arr[1].trim();// values后面的内容
            // 去左右括号
            _values = CommonUtils.replaceStrLRBrackets(_values);
            // 按,分割内容，按查询字段做映射
            String[] _values_arr = _values.split(",", -1);
            // 需要对前后字段做个数校验
            if (hashTable.getQuery_fields_arr().length != _values_arr.length) {
                throw CommonUtils.createSQLException("插入字段个数和查询字段个数不一致");
            }
            // 设置值到filedMap中
            for (int i = 0; i < hashTable.getQuery_fields_arr().length; i++) {
                // 这里是直接设置filedValueStr
                statementFieldMap.put(new HashTableField(i,
                        hashTable.getQuery_fields_arr()[i],
                        _values_arr[i],
                        hashTable.getRedisColumnByName(hashTable.getQuery_fields_arr()[i])));
            }
        }
    }

    @Override
    public String[] getFieldsArr() {
        return hashTable.getQuery_fields_arr();
    }

    /**
     * 插入数据
     *
     * @param _fieldMap
     * @return
     */
    @Override
    public int run(Map<String, String> _fieldMap) throws SQLException {
        if (isSelect) {
            selectRedisParser.run(_fieldMap);
            int ret = 0;
            for (Map.Entry<String, Map<String, String>> entry : selectRedisParser.getRedisHashResult().entrySet()) {
                // 一行一行插入
                ret = ret + insertInto(entry.getValue());
            }
            return ret;
        } else {
            if (isPrepared) {// prearedstatement
                return insertInto(_fieldMap);
            } else {// statement
                return insertInto(statementFieldMap);
            }
        }
    }

    /**
     * 实现hset
     *
     * @param _fieldMap
     * @return
     */
    private int insertInto(Map<String, String> _fieldMap) {
        String field = getField(_fieldMap);
        String key = getKey(_fieldMap);
        String value = CommonUtils.changeMapToJSON(hashTable.getQuery_fields_arr(), _fieldMap);
        long ret = rc.hset(field, key, value);
        return Integer.valueOf(String.valueOf(ret));
    }

    /**
     * 拼接field，field是定义好的不变
     *
     * @param valueMap
     * @return
     */
    private String getField(Map<String, String> valueMap) {
        StringBuilder sb = new StringBuilder();
        for (String value : hashTable.getDef_field_arr()) {
            sb.append(valueMap.get(value));
        }
        return sb.toString();
    }

    /**
     * 拼接key，key是定义好的不变
     *
     * @param valueMap
     * @return
     */
    private String getKey(Map<String, String> valueMap) {
        StringBuilder sb = new StringBuilder();
        for (String value : hashTable.getDef_key_arr()) {
            sb.append(valueMap.get(value));
        }
        return sb.toString();
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
