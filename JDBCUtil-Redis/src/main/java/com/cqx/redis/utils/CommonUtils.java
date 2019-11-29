package com.cqx.redis.utils;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * CommonUtils
 *
 * @author chenqixu
 */
public class CommonUtils {

    /**
     * 条件保护
     * <pre>
     *     field1='a' and field2='b'
     *     split => array[]
     *     0、field1=
     *     1、a
     *     2、 and field2=
     *     3、b
     * </pre>
     *
     * @param sql
     * @return
     */
    public static String conditionalProtection(String sql, String split_str) {
        // 保护引号或双引号内的值不做小写转换
        String _tmp_single = sql;
        String[] _tmp_single_arr = _tmp_single.split(split_str, -1);
        StringBuilder sb_single = new StringBuilder();
        for (int i = 0; i < _tmp_single_arr.length; i++) {
            // 逢单不处理
            if (i % 2 == 0) {
                sb_single.append(_tmp_single_arr[i].toLowerCase());
            } else {
                sb_single.append(_tmp_single_arr[i]);
            }
        }
        return sb_single.toString();
    }

    /**
     * 判断是否以某个字符串开头，并对首个进行替换
     *
     * @param str
     * @param key
     * @return
     */
    public static StartsWithResult startsWith(String str, String key) {
        boolean isStartsWith = str.startsWith(key);
        String resultStr = str;
        if (isStartsWith) resultStr = str.replaceFirst(key, "").trim();
        return new StartsWithResult(isStartsWith, resultStr);
    }

    /**
     * 替换可能的多余字符
     *
     * @param value
     * @return
     */
    public static String replaceStr(String value) {
        value = value.replaceAll("\"", "");// 替换掉可能的双引号
        value = value.replaceAll("'", "");// 替换掉可能的单引号
        return value;
    }

    /**
     * 替换左右括号
     *
     * @param value
     * @return
     */
    public static String replaceStrLRBrackets(String value) {
        value = value.replace("(", "");// 替换掉左括号
        value = value.replace(")", "");// 替换掉右括号
        return value;
    }

    /**
     * 判断切割后数组长度是否正确
     *
     * @param value
     * @param split_str
     * @param len
     * @return
     */
    public static boolean splitAndCheckLen(String value, String split_str, int len) throws SQLException {
        if (value == null || value.trim().length() == 0 || split_str == null || split_str.trim().length() == 0 || len <= 0)
            throw new SQLException("输入条件有误，请检查。value：" + value + "，split_str：" + split_str + "，len：" + len);
        return value.split(split_str, -1).length == len;
    }

    /**
     * 以map2为准，合并到map1中
     *
     * @param map1
     * @param map2
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V> Map<K, V> mergeMap(Map<K, V> map1, Map<K, V> map2) {
        if (map1 != null && map2 != null) {
            Map<K, V> newMap = new HashMap<>();
            newMap.putAll(map1);
            newMap.putAll(map2);
            return newMap;
        } else {
            throw new NullPointerException("map1或map2值为空，map1：" + map1 + "，map2：" + map2);
        }
    }

    /**
     * 根据数组拼接JSON.value
     *
     * @param arr
     * @param valueMap
     * @return
     */
    public static String changeMapToJSON(String[] arr, Map<String, String> valueMap) {
        StringBuilder sb = new StringBuilder("{");
        for (String value : arr) {
            String _v = valueMap.get(value);
            if (_v != null && _v.length() > 0) {
                sb.append("\"" + value)
                        .append("\":\"")
                        .append(_v)
                        .append("\",");
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append("}");
        return sb.toString();
    }

    /**
     * 返回SQLException描述
     *
     * @param msg
     * @return
     */
    public static SQLException createSQLException(String msg) {
        return new SQLException("SQL语法错误，" + msg + "，请检查");
    }

    public static Integer[] changeIntToIntegerArray(int[] ret) {
        Integer[] result = new Integer[ret.length];
        for (int i = 0; i < ret.length; i++) {
            result[i] = ret[i];
        }
        return result;
    }
}
