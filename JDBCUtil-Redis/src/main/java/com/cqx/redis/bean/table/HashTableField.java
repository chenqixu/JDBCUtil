package com.cqx.redis.bean.table;

/**
 * Field
 *
 * @author chenqixu
 */
public class HashTableField implements Comparable<HashTableField> {
    private int index;
    private String filedName;
    private String filedValue;

    public HashTableField(int index, String filedValue) {
        this(index, filedValue, null);
    }

    public HashTableField(int index, String filedName, String filedValue) {
        this.index = index;
        this.filedName = filedName;
        this.filedValue = filedValue;
    }

    public String toString() {
        return "index：" + index + "，filedName：" + filedName + "，filedValue：" + filedValue;
    }

    @Override
    public int compareTo(HashTableField o) {
        return this.index - o.index;
    }

    public String getFiledValue() {
        return filedValue;
    }
}
