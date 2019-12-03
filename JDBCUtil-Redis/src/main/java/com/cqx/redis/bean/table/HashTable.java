package com.cqx.redis.bean.table;

import com.cqx.redis.jdbc.RedisColumn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HashTable
 *
 * @author chenqixu
 */
public class HashTable implements Cloneable {
    private String fields;// 表字段
    private String[] fields_arr;// 表字段数组
    private String query_fields;// 查询字段
    private String[] query_fields_arr;// 查询字段数组
    private String def_field;// 条件field
    private String def_key;// 条件key
    private String[] def_field_arr;// 条件field数组
    private String[] def_key_arr;// 条件key数组
    private RedisColumn[] redisColumns;
    private Map<String, RedisColumn> redisColumnMap = new HashMap<>();
    private List<HashTableQuery> hashTableQueryList = new ArrayList<>();

    public HashTable(String fields, String def_field, String def_key, RedisColumn[] redisColumns) {
        this.fields = fields;
        this.def_field = def_field;
        this.def_key = def_key;
        this.def_field_arr = def_field.split(",", -1);
        this.def_key_arr = def_key.split(",", -1);
        this.fields_arr = fields.split(",", -1);
        this.redisColumns = redisColumns;
        for (RedisColumn redisColumn : redisColumns) {
            redisColumnMap.put(redisColumn.getName(), redisColumn);
        }
    }

    public String getFields() {
        return fields;
    }

    public String getDef_field() {
        return def_field;
    }

    public void setDef_field(String def_field) {
        this.def_field = def_field;
    }

    public String getDef_key() {
        return def_key;
    }

    public void setDef_key(String def_key) {
        this.def_key = def_key;
    }

    public String[] getDef_field_arr() {
        return def_field_arr;
    }

    public void setDef_field_arr(String[] def_field_arr) {
        this.def_field_arr = def_field_arr;
    }

    public RedisColumn[] getRedisColumns() {
        return redisColumns;
    }

    public void setRedisColumns(RedisColumn[] redisColumns) {
        this.redisColumns = redisColumns;
    }

    public RedisColumn getRedisColumnByName(String name) {
        return redisColumnMap.get(name);
    }

    public String getQuery_fields() {
        return query_fields;
    }

    public void setQuery_fields(String query_fields) {
        this.query_fields = query_fields;
        this.query_fields_arr = query_fields.split(",", -1);
    }

    public String[] getQuery_fields_arr() {
        return query_fields_arr;
    }

    public String[] getDef_key_arr() {
        return def_key_arr;
    }

    public void setDef_key_arr(String[] def_key_arr) {
        this.def_key_arr = def_key_arr;
    }

    public List<HashTableQuery> getHashTableQueryList() {
        return hashTableQueryList;
    }

    public void addHashTableQueryList(HashTableQuery hashTableQuery) {
        this.hashTableQueryList.add(hashTableQuery);
    }

    public String[] getFields_arr() {
        return fields_arr;
    }

    private void newHashTableQueryList() {
        hashTableQueryList = new ArrayList<>();
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        Object object = super.clone();
        // 拷贝后，如果有指针，还是指针，所以必须new
        ((HashTable) object).newHashTableQueryList();
        return object;
    }
}
