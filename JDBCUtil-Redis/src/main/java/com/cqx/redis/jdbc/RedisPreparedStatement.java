package com.cqx.redis.jdbc;

import com.cqx.redis.bean.table.HashTable;
import com.cqx.redis.bean.table.HashTableField;
import com.cqx.redis.bean.table.HashTableFieldMap;
import com.cqx.redis.client.RedisClient;
import com.cqx.redis.impl.RedisHashSqlParser;
import com.cqx.redis.utils.CommonUtils;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.*;
import java.util.*;

/**
 * RedisPreparedStatement
 *
 * @author chenqixu
 */
public class RedisPreparedStatement implements PreparedStatement {

    private HashTableFieldMap hashTableFieldMap = new HashTableFieldMap();
    private List<Map<String, String>> hashTableFieldMapList = new ArrayList<>();
    private RedisClient rc;
    private String sql;
    private RedisHashSqlParser redisHashSqlParser;
    private String[] fields_arr;
    private HashTable hashTable;

    public RedisPreparedStatement(RedisClient rc) {
        this.rc = rc;
        redisHashSqlParser = new RedisHashSqlParser(rc, true);
    }

    /**
     * 解析SQL
     *
     * @param sql
     */
    public void prepare(String sql) throws SQLException {
        this.sql = sql;
        redisHashSqlParser.parser(sql);
        fields_arr = redisHashSqlParser.getFieldsArr();
        hashTable = redisHashSqlParser.getHashTable();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        addBatch();
        executeBatch();
        return redisHashSqlParser.getRedisResultSet();
    }

    @Override
    public int executeUpdate() throws SQLException {
        return 0;
    }

    private HashTableField createHashTableField(int parameterIndex, Object x) throws SQLException {
        if (parameterIndex > fields_arr.length)
            throw CommonUtils.createSQLException("不支持的列，索引：" + parameterIndex);
        return new HashTableField(parameterIndex, fields_arr[parameterIndex - 1], x, hashTable.getRedisColumnByName(fields_arr[parameterIndex - 1]));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {

    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {

    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {

    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        // 根据prepare出来的字段映射，来获取字段名称
        hashTableFieldMap.put(createHashTableField(parameterIndex, x));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        // 根据prepare出来的字段映射，来获取字段名称
        hashTableFieldMap.put(createHashTableField(parameterIndex, x));
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        // 根据prepare出来的字段映射，来获取字段名称
        hashTableFieldMap.put(createHashTableField(parameterIndex, x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        // 根据prepare出来的字段映射，来获取字段名称
        hashTableFieldMap.put(createHashTableField(parameterIndex, x));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        // 根据prepare出来的字段映射，来获取字段名称
        hashTableFieldMap.put(createHashTableField(parameterIndex, x));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        // 根据prepare出来的字段映射，来获取字段名称
        hashTableFieldMap.put(createHashTableField(parameterIndex, x));
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        // 根据prepare出来的字段映射，来获取字段名称
        hashTableFieldMap.put(createHashTableField(parameterIndex, x));
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {

    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        // 根据prepare出来的字段映射，来获取字段名称
        hashTableFieldMap.put(createHashTableField(parameterIndex, x));
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        // 根据prepare出来的字段映射，来获取字段名称
        hashTableFieldMap.put(createHashTableField(parameterIndex, x));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        // 根据prepare出来的字段映射，来获取字段名称
        hashTableFieldMap.put(createHashTableField(parameterIndex, x));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void clearParameters() throws SQLException {

    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {

    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {

    }

    @Override
    public boolean execute() throws SQLException {
        return false;
    }

    /**
     * 表示本条数据结束
     *
     * @throws SQLException
     */
    @Override
    public void addBatch() throws SQLException {
//        // 排序
//        Collections.sort(fieldList);
//        Map<String, String> fieldMap = new HashMap<>();
//        // 把内容写入字段映射关系
//        for (int i = 0; i < fieldList.size(); i++) {
//            fieldMap.put(fields_arr[i], fieldList.get(i).getFiledValue());
//        }
//        fieldMapList.add(fieldMap);
//        // 清理List
//        fieldList.clear();
        // 数据拷贝
        Map<String, String> fieldMap = new HashMap<>();
        fieldMap.putAll(hashTableFieldMap);
        // 加入List
        hashTableFieldMapList.add(fieldMap);
        // 清理
        hashTableFieldMap.clear();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {

    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {

    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {

    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {

    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {

    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {

    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {

    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {

    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {

    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return 0;
    }

    @Override
    public void close() throws SQLException {
        redisHashSqlParser.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {

    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {

    }

    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return null;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    /**
     * 处理缓存中的数据，真正的去执行rc.set、del、get
     *
     * @return
     * @throws SQLException
     */
    @Override
    public int[] executeBatch() throws SQLException {
        int[] rets = new int[hashTableFieldMapList.size()];
        for (int i = 0; i < hashTableFieldMapList.size(); i++) {
            rets[i] = redisHashSqlParser.execute(hashTableFieldMapList.get(i));
        }
        // 清理List
        hashTableFieldMapList.clear();
        return rets;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
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
