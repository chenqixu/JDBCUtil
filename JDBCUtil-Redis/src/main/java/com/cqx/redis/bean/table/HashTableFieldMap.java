package com.cqx.redis.bean.table;

import java.util.HashMap;

/**
 * HashTableFieldMap
 *
 * @author chenqixu
 */
public class HashTableFieldMap extends HashMap<String, String> {

    public HashTableFieldMap() {
    }

    public void put(HashTableField hashTableField) {
        put(hashTableField.getFiledName(), hashTableField.getFiledValueStr());
    }

}
