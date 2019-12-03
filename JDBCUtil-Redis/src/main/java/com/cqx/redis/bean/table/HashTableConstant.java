package com.cqx.redis.bean.table;

import com.cqx.redis.jdbc.RedisColumn;
import com.cqx.redis.utils.CommonUtils;

import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HashTableConstant
 *
 * @author chenqixu
 */
public class HashTableConstant {
    private static final ConcurrentHashMap<String, HashTable> hashTableMap = new ConcurrentHashMap<>();

    static {
        load();
    }

    public static void load() {
        // fjbi_busdatacollect_list
        RedisColumn[] redisColumns = {
                new RedisColumn("task_template_id", "task_template_id", 2, "NUMBER", "java.math.BigDecimal"),
                new RedisColumn("file_name", "file_name", 12, "VARCHAR2", "java.lang.String"),
                new RedisColumn("source_machine", "source_machine", 12, "VARCHAR2", "java.lang.String"),
                new RedisColumn("source_path", "source_path", 12, "VARCHAR2", "java.lang.String"),
                new RedisColumn("check_file_path", "check_file_path", 12, "VARCHAR2", "java.lang.String"),
                new RedisColumn("data_index", "data_index", 12, "VARCHAR2", "java.lang.String"),
                new RedisColumn("source_file_suffix", "source_file_suffix", 12, "VARCHAR2", "java.lang.String"),
                new RedisColumn("check_file_suffix", "check_file_suffix", 12, "VARCHAR2", "java.lang.String"),
                new RedisColumn("source_file_createtime", "source_file_createtime", 2, "NUMBER", "java.math.BigDecimal"),
                new RedisColumn("file_size", "file_size", 2, "NUMBER", "java.math.BigDecimal"),
                new RedisColumn("file_recordnum", "file_recordnum", 2, "NUMBER", "java.math.BigDecimal"),
                new RedisColumn("file_cycle", "file_cycle", 12, "VARCHAR2", "java.lang.String"),
                new RedisColumn("merge_name", "merge_name", 12, "VARCHAR2", "java.lang.String"),
                new RedisColumn("insert_time", "insert_time", 93, "DATE", "java.sql.Timestamp"),
                new RedisColumn("file_status", "file_status", 2, "NUMBER", "java.math.BigDecimal"),
                new RedisColumn("file_status_updatetime", "file_status_updatetime", 93, "DATE", "java.sql.Timestamp")
        };
        HashTable fjbi_busdatacollect_list = new HashTable("task_template_id,file_name,source_machine,source_path,check_file_path,data_index,source_file_suffix,check_file_suffix,source_file_createtime,file_size,file_recordnum,file_cycle,merge_name,insert_time,file_status,file_status_updatetime",
                "task_template_id,data_index,file_status", "file_name", redisColumns);
        hashTableMap.put("fjbi_busdatacollect_list", fjbi_busdatacollect_list);
    }

    public static HashTable getHashTableByName(String tableName) throws SQLException {
        // 由于查询条件放在静态类中，必须进行拷贝，否则会有BUG
        HashTable cloneHashTable;
        try {
            cloneHashTable = (HashTable) hashTableMap.get(tableName).clone();
        } catch (CloneNotSupportedException e) {
            throw CommonUtils.createSQLException("表未创建：" + tableName);
        }
        return cloneHashTable;
    }

    public static void checkFields(String _fields, HashTable hashTable) throws SQLException {
        if (_fields == null || _fields.length() == 0) throw new SQLException("语法不正确，查询字段为空，请检查！");
        String[] fields_arr = _fields.split(",", -1);
        int fields_len = fields_arr.length;
        int check_len = 0;
        for (String field : fields_arr) {
            if (hashTable.getRedisColumnByName(field) != null) check_len++;
        }
        if (!(fields_len == check_len)) {
            throw new SQLException("语法不正确，查询字段和定义字段不符，请检查！查询字段：" + _fields + "，定义字段：" + hashTable.getFields());
        }
    }

}
