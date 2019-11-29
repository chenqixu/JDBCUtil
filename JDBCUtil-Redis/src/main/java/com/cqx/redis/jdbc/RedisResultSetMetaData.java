package com.cqx.redis.jdbc;

import com.cqx.redis.bean.table.HashTable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * redis结果集元数据
 *
 * @author chenqixu
 */
public class RedisResultSetMetaData implements java.sql.ResultSetMetaData {

    private String[] fields_arr;
    private List<RedisColumn> redisColumnList = new ArrayList<>();

    public RedisResultSetMetaData(HashTable hashTable) {
        this(hashTable.getQuery_fields_arr());
        // 第一行是空记录
        redisColumnList.add(new RedisColumn());
        for (String field : fields_arr) {
            redisColumnList.add(hashTable.getRedisColumnByName(field));
        }
    }

    /**
     * 传入查询字段
     *
     * @param fields
     */
    public RedisResultSetMetaData(String fields) {
        fields_arr = fields.split(",", -1);
    }

    public RedisResultSetMetaData(String[] fields_arr) {
        this.fields_arr = fields_arr;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return fields_arr.length;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return 0;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        if (column == 0) throw new SQLException("不支持的列");
        return redisColumnList.get(column).getLabel();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        if (column == 0) throw new SQLException("不支持的列");
        return redisColumnList.get(column).getName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return null;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return null;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return null;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        if (column == 0) throw new SQLException("不支持的列");
        return redisColumnList.get(column).getType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        if (column == 0) throw new SQLException("不支持的列");
        return redisColumnList.get(column).getTypeName();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        if (column == 0) throw new SQLException("不支持的列");
        return redisColumnList.get(column).getClassName();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
