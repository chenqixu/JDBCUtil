package com.cqx.redis.bean.table;

import com.alibaba.fastjson.JSON;
import com.cqx.redis.utils.CommonUtils;

import java.util.Map;

/**
 * HashTableUpdate
 *
 * @author chenqixu
 */
public class HashTableUpdate {

    private String field_old;
    private String field_new;
    private String key;
    private String value_old;
    private String value_new;
    private Map<String, String> updateFieldsMap;
    private HashTable hashTable;// 表定义
    // 更新有三种情况：1、更新field和value；2、更新field；3、更新value
    // 如果：【实际上value无论如何都会更新，所以只要判断field是否更新
    // 更新有两种情况：1、更新field和value；2、更新value】
    private boolean isUpdateField;// 是否更新field
    private boolean isUpdateValue;// 是否更新value

    public HashTableUpdate() {
    }

    public HashTableUpdate(Map<String, String> updateFieldsMap, HashTable hashTable) {
        this.updateFieldsMap = updateFieldsMap;
        this.hashTable = hashTable;
    }

    public void setOldValue(String field, String key, String value) {
        this.field_old = field;
        this.key = key;
        this.value_old = value;
        // 把value转换成Map
        Map oldValueMap = JSON.parseObject(value);
        // 把updatefiedMap合并进来
        Map<String, String> newValueMap = CommonUtils.mergeMap(oldValueMap, updateFieldsMap);
        // 判断是要hset还是hdel+hset
        // 取出表定义的def_field，生成新的field值
        StringBuffer new_field = new StringBuffer();
        for (String def_field : hashTable.getDef_field_arr()) {
            new_field.append(newValueMap.get(def_field));
        }
        this.field_new = new_field.toString();
        // 比较新旧def_field
        if (!this.field_old.equals(this.field_new)) isUpdateField = true;

        // 取出表定义的field，生成新旧value（按字段定义排序）
        String old_value = CommonUtils.changeMapToJSON(hashTable.getFields_arr(), oldValueMap);
        value_new = CommonUtils.changeMapToJSON(hashTable.getFields_arr(), newValueMap);
        // 比较新旧value
        if (!old_value.equals(value_new)) isUpdateValue = true;
    }

    public String getField_old() {
        return field_old;
    }

    public void setField_old(String field_old) {
        this.field_old = field_old;
    }

    public String getField_new() {
        return field_new;
    }

    public void setField_new(String field_new) {
        this.field_new = field_new;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue_old() {
        return value_old;
    }

    public void setValue_old(String value_old) {
        this.value_old = value_old;
    }

    public String getValue_new() {
        return value_new;
    }

    public void setValue_new(String value_new) {
        this.value_new = value_new;
    }

    public Map<String, String> getUpdateFieldsMap() {
        return updateFieldsMap;
    }

    public void setUpdateFieldsMap(Map<String, String> updateFieldsMap) {
        this.updateFieldsMap = updateFieldsMap;
    }

    public boolean isUpdateField() {
        return isUpdateField;
    }

    public void setUpdateField(boolean updateField) {
        isUpdateField = updateField;
    }

    public boolean isUpdateValue() {
        return isUpdateValue;
    }

    public void setUpdateValue(boolean updateValue) {
        isUpdateValue = updateValue;
    }

}

