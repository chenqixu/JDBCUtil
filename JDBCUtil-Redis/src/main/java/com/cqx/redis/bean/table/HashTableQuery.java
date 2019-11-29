package com.cqx.redis.bean.table;

/**
 * HashTableQuery
 *
 * @author chenqixu
 */
public class HashTableQuery {

    private String field;
    private String key;
    private boolean isHgetAll = false;// 是否hgetAll

    public HashTableQuery(String field) {
        this.field = field;
        isHgetAll = true;
    }

    public HashTableQuery(String field, String key) {
        this.field = field;
        this.key = key;
    }

    public String toString() {
        return "field：" + field + "，key：" + key + "，isHgetAll：" + isHgetAll;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public boolean isHgetAll() {
        return isHgetAll;
    }

    public void setHgetAll(boolean hgetAll) {
        isHgetAll = hgetAll;
    }
}
