package com.cqx.redis.bean.table;

import com.cqx.redis.comm.RedisConst;
import com.cqx.redis.jdbc.RedisColumn;
import com.cqx.redis.utils.CommonUtils;

import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Field
 *
 * @author chenqixu
 */
public class HashTableField implements Comparable<HashTableField> {
    private int index;
    private String filedName;
    private Object filedValue;
    private String filedValueStr;
    private RedisColumn redisColumn;

    public HashTableField() {
    }

    public HashTableField(int index, String filedName, Object filedValue, RedisColumn redisColumn) throws SQLException {
        this(index, filedName, filedValue, null, redisColumn);
    }

    public HashTableField(String filedName, String filedValueStr, RedisColumn redisColumn) throws SQLException {
        this(0, filedName, null, filedValueStr, redisColumn);
    }

    public HashTableField(int index, String filedName, String filedValueStr, RedisColumn redisColumn) throws SQLException {
        this(index, filedName, null, filedValueStr, redisColumn);
    }

    public HashTableField(int index, String filedName, Object filedValue, String filedValueStr, RedisColumn redisColumn) throws SQLException {
        this.index = index;
        this.filedName = filedName;
        this.filedValue = filedValue;
        this.redisColumn = redisColumn;
        this.filedValueStr = parserValue(filedValueStr);
    }

    /**
     * 解析值
     */
    private String parserValue(String filedValueStr) throws SQLException {
        if (redisColumn != null) {
            String _value;
            // 分两种情况
            // 1、filedValue有值
            // 2、filedValue没有值
            if (filedValue != null) {// filedValue有值，PreparedStatement的情况
                switch (redisColumn.getType()) {
                    case 2:// java.math.BigDecimal
                        _value = String.valueOf(filedValue);
                        break;
                    case 93:// java.sql.Timestamp
                        _value = String.valueOf(((java.sql.Timestamp) filedValue).getTime());
                        break;
                    case 12:// java.lang.String
                    default:
                        _value = filedValue.toString();
                        break;
                }
            } else {// filedValue没有值，Statement的情况
                switch (redisColumn.getType()) {
                    case 93:// java.sql.Timestamp
                        // 去左右空格，转小写
                        filedValueStr = filedValueStr.trim().toLowerCase();
                        if (filedValueStr.equals(RedisConst.SYSDATE)) {// 如果是sysdate，取当前时间
                            _value = String.valueOf(new Timestamp(System.currentTimeMillis()).getTime());
                        } else {
                            throw CommonUtils.createSQLException("时间字段内容无法解析，值：" + filedValueStr);
                        }
                        break;
                    case 2:// java.math.BigDecimal
                    case 12:// java.lang.String
                    default:
                        _value = filedValueStr;
                        break;
                }
            }
            return _value;
        } else {// 没有传入字段类型
            return filedValueStr;
        }
    }

    public String toString() {
        return "index：" + index + "，filedName：" + filedName + "，filedValue：" + filedValue + "，filedValueStr：" + filedValueStr + "，redisColumn：" + redisColumn;
    }

    @Override
    public int compareTo(HashTableField o) {
        return this.index - o.index;
    }

    public String getFiledValueStr() {
        return filedValueStr;
    }

    public int getIndex() {
        return index;
    }

    public String getFiledName() {
        return filedName;
    }

    public Object getFiledValue() {
        return filedValue;
    }

    public RedisColumn getRedisColumn() {
        return redisColumn;
    }
}
