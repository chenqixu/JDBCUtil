package com.cqx.redis.impl;

import com.cqx.redis.bean.table.CartesianProduct;
import com.cqx.redis.bean.table.HashTable;
import com.cqx.redis.bean.table.HashTableConstant;
import com.cqx.redis.bean.table.HashTableQuery;
import com.cqx.redis.comm.RedisConst;
import com.cqx.redis.utils.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;

/**
 * where条件处理
 *
 * @author chenqixu
 */
public class RedisWhereParser {

    private static final String SQL_WHERE = RedisConst.KEY_WHERE_SPACE_LR;
    private static final String SQL_AND = RedisConst.KEY_AND_SPACE;
    private static final String SQL_EQUAL = RedisConst.KEY_EQUAL;
    private static final String SQL_IN = RedisConst.KEY_IN;
    private static final Logger logger = LoggerFactory.getLogger(RedisWhereParser.class);

    private String tableName;
    private String _where;
    private List<String> whereKeyList = new ArrayList<>();// 条件key列表，有序
    private Map<String, List<String>> whereList = new HashMap<>();// 条件Map
    private HashTable hashTable;
    private boolean hasCartesianProduct = true;// 是否允许笛卡尔积，默认允许
    private boolean hasKeyMust = false;// 条件中，key是否必须，默认非必须
    private boolean isPrepared = false;// 是否预编译SQL，默认false，预编译只支持=?，不支持in ('','','')

    public RedisWhereParser() {
    }

    public RedisWhereParser(boolean isPrepared) {
        this.isPrepared = isPrepared;
    }

    public RedisWhereParser(boolean hasCartesianProduct, boolean hasKeyMust, boolean isPrepared) {
        this.hasCartesianProduct = hasCartesianProduct;
        this.hasKeyMust = hasKeyMust;
        this.isPrepared = isPrepared;
    }

    /**
     * 语句解析
     *
     * @param where_sql
     */
    public void split(String where_sql) throws SQLException {
        String[] _tablename_where_arr = where_sql.split(SQL_WHERE, -1);// 按" where "进行切割，获取表名和查询条件
        tableName = _tablename_where_arr[0].trim();// 获取表名，并去除左右空格：table_name
        _where = _tablename_where_arr[1].trim();// 获取查询条件，并去除左右空格：field1='' and field2='' and field3=''

        // 通过表名获取表定义
        logger.debug("tableName：{}", tableName);
        hashTable = HashTableConstant.getHashTableByName(tableName);
        if (hashTable == null) throw new SQLException("无法通过表名获取到表定义，请确认，tableName：" + tableName);
    }

    /**
     * 解析查询条件，根据查询条件循环插入表定义hashTableDef的hashTableQueryList
     *
     * @throws SQLException
     */
    public void parser() throws SQLException {
        // 解析查询条件，根据查询条件循环插入表定义hashTableDef的hashTableQueryList
        String[] _and_arr = _where.split(SQL_AND, -1); // 按" and "进行切割
        for (String and : _and_arr) {
            String _and = and.trim();// 先去除左右空格
            if (_and.contains(SQL_EQUAL) && CommonUtils.splitAndCheckLen(_and, SQL_EQUAL, 2)) {// 判断是否是"="，还要按"="切割判断数组长度是否2
                String[] _equal_arr = _and.split(SQL_EQUAL, -1);// 按"="进行切割
                String _equal_field = _equal_arr[0].trim();// 查询字段，并去除左右空格
                String _equal_value = _equal_arr[1].trim();// 查询字段条件，并去除左右空格
                _equal_value = CommonUtils.replaceStr(_equal_value);// 替换可能的多余字符
                if (!isPrepared) whereListGetAndSet(whereList, _equal_field, _equal_value);// 不是预编译，加入条件
                whereKeyList.add(_equal_field);// 加入条件key列表，用于校验
            } else if (_and.contains(SQL_IN) && CommonUtils.splitAndCheckLen(_and, SQL_IN, 2)) {// 判断是否是" in"，还要按"in"切割判断数组长度是否2
                String[] _in_arr = _and.split(SQL_IN, -1);// 按" in"进行切割
                String _in_field = _in_arr[0].trim();// 查询字段，并去除左右空格
                String _in_value = _in_arr[1].trim();// 查询字段条件，并去除左右空格
                _in_value = CommonUtils.replaceStrLRBrackets(_in_value);// 替换掉左右括号
                String[] _in_value_arr = _in_value.split(",", -1);// 按","进行切割
                if (!isPrepared) {// 不是预编译
                    for (String in_value : _in_value_arr) {
                        String _in_value_tmp = CommonUtils.replaceStr(in_value);// 替换可能的多余字符
                        whereListGetAndSet(whereList, _in_field, _in_value_tmp);// 加入条件
                    }
                }
                whereKeyList.add(_in_field);// 加入条件key列表，用于校验
            } else {
                throw new SQLException("sql查询语句中的查询条件有误，请检查！条件：" + _where);
            }
        }
    }

    /**
     * 拼接所有的条件
     *
     * @return
     */
    public void connectingCondition(Map<String, String> _fieldMap) throws SQLException {
        // 如果是预编译
        if (isPrepared) {
            whereList = new HashMap<>();// 清空条件Map
            // 循环处理条件
            for (Map.Entry<String, String> entry : _fieldMap.entrySet()) {
                whereListGetAndSet(whereList, entry.getKey(), entry.getValue());// 加入条件
            }
        }
        // 打印条件
        printWhereMap();
        // 获取定义
        String def_key = hashTable.getDef_key();
        String[] def_field_arr = hashTable.getDef_field_arr();
        // 拼接所有条件
        CartesianProduct cartesianProduct = new CartesianProduct();
        // HashTable.def_field_arr转换成笛卡尔积
        for (int i = 1; i < def_field_arr.length; i++) {
            cartesianProduct.addQuery(whereList.get(def_field_arr[i]));
        }
        // 转换成笛卡尔积
        cartesianProduct.dealCartesianProduct(whereList.get(def_field_arr[0]));
        logger.debug("笛卡尔积：{}", cartesianProduct.getCartesianProductList());
        // 不允许有笛卡尔积
        if (!hasCartesianProduct && cartesianProduct.getCartesianProductList().size() > 1)
            throw new SQLException("条件不允许有笛卡尔积的情况，请检查");

        // 确认有没def_key，如果有，则走hget，如果没有则走hgetAll
        List<String> def_key_values = whereList.get(def_key);
        if (def_key_values != null) {// hget
            for (String _cartesianProduct : cartesianProduct.getCartesianProductList()) {
                for (String def_key_value : def_key_values) {
                    hashTable.addHashTableQueryList(new HashTableQuery(_cartesianProduct, def_key_value));
                }
            }
        } else {// hgetAll
            for (String _cartesianProduct : cartesianProduct.getCartesianProductList()) {
                hashTable.addHashTableQueryList(new HashTableQuery(_cartesianProduct));
            }
        }
        logger.debug("拼接所有的条件：{}", hashTable.getHashTableQueryList());
    }

    /**
     * 增加到条件Map
     *
     * @param whereList
     * @param field
     * @param condition
     */
    private void whereListGetAndSet(Map<String, List<String>> whereList, String field, String condition) {
        List<String> valueList = whereList.get(field);
        if (valueList == null) {
            valueList = new ArrayList<>();
            whereList.put(field, valueList);
        }
        valueList.add(condition);
    }

    /**
     * 校验查询条件，条件只能在def_field、def_key范围内，def_key可以没有，但def_field必须全要
     */
    public void checkWhere() throws SQLException {
        String def_key = hashTable.getDef_key();
        String[] def_field_arr = hashTable.getDef_field_arr();
        // 复制一个whereKeyList
        List<String> _whereKeyList = new ArrayList<>();
        _whereKeyList.addAll(whereKeyList);
        // 把表定义的field放到一个集合
        List<String> defFieldList = new ArrayList<>();
        defFieldList.addAll(Arrays.asList(def_field_arr));
        // 比对表定义的field
        Iterator<String> iterable = defFieldList.iterator();
        while (iterable.hasNext()) {
            String _def = iterable.next();
            for (String _key : _whereKeyList) {
                if (_def.equals(_key)) {
                    _whereKeyList.remove(_key);
                    iterable.remove();
                    break;
                }
            }
        }
        // 比对表定义的key
        boolean hasCheckKey = false;// 查询条件是否有带key
        for (String _key : _whereKeyList) {
            if (def_key.equals(_key)) {
                _whereKeyList.remove(_key);
                hasCheckKey = true;
                break;
            }
        }
        // 1、保证defFieldList.size为0
        // 2、def_key可以不匹配
        // 3、keyList.size也必须为0
        // 4、如果hasKeyMust为真，则查询条件必须带key
        if (defFieldList.size() == 0 && _whereKeyList.size() == 0 && (hasKeyMust ? hasCheckKey : true)) {
        } else {
            if (hasKeyMust ? !hasCheckKey : false) {// 查询条件没有key
                logger.warn("校验查询条件，hasKeyMust：{}，hasCheckKey：{}", hasKeyMust, hasCheckKey);
                throw new SQLException("查询条件必须有key【" + def_key + "】，请检查！");
            } else {// 其他
                logger.warn("校验查询条件，定义的field剩余列表：{}，where条件剩余列表：{}", defFieldList, _whereKeyList);
                throw new SQLException("查询条件只能在def_field、def_key范围内，def_key可以没有【" + def_key + "】，但def_field必须全要【" + hashTable.getDef_field() + "】，请检查！");
            }
        }
    }

    /**
     * 打印条件Map
     */
    private void printWhereMap() {
        logger.debug("打印条件Map，{}", whereList);
    }

    public String getTableName() {
        return tableName;
    }

    public HashTable getHashTable() {
        return hashTable;
    }

    public Map<String, List<String>> getWhereList() {
        return whereList;
    }

    public String[] getWhereKeyArr() {
        String[] result = new String[whereKeyList.size()];
        whereKeyList.toArray(result);
        return result;
    }

}
