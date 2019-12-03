package com.cqx.redis.jdbc;

/**
 * redis具体结果，按行
 *
 * @author chenqixu
 */
public class RedisRowData {

    private Object value;

    public RedisRowData() {
    }

    public RedisRowData(Object value) {
        this.value = value;
    }

    public RedisRowData(String value) {
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
