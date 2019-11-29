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

//    public RedisRowData(T value, RedisColumn redisColumn) {
//        switch (redisColumn.getClassName()) {
//            case "java.math.BigDecimal":
//                java.math.BigDecimal t1 = (java.math.BigDecimal) value;
//                t = value;
//                break;
//            case "java.lang.String":
//                java.lang.String t2 = (java.lang.String) value;
//                break;
//            case "java.sql.Timestamp":
//                java.sql.Timestamp t3 = (java.sql.Timestamp) value;
//                break;
//            default:
//                Object t = value;
//                break;
//        }
//    }

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
