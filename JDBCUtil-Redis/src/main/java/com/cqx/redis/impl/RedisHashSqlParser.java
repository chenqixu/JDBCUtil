package com.cqx.redis.impl;

import com.cqx.redis.client.RedisClient;
import com.cqx.redis.jdbc.RedisResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * RedisHashSqlParser
 *
 * @author chenqixu
 */
public class RedisHashSqlParser {

    private static final Logger logger = LoggerFactory.getLogger(RedisHashSqlParser.class);
    private static List<IRedisParser> iRedisParserList = new ArrayList<>();

    static {
        try {
            Class.forName(InsertIntoRedisParser.class.getName());
            Class.forName(SelectRedisParser.class.getName());
            Class.forName(DeleteRedisParser.class.getName());
            Class.forName(UpdateRedisParser.class.getName());
        } catch (ClassNotFoundException e) {
            logger.error("解析类加载失败，" + e.getMessage(), e);
        }
    }

    private RedisClient rc;
    private String sql;//sql
    private IRedisParser iRedisParser;
    private boolean isPrepared = false;// 是否是PreparedStatement，默认是Statement

    public RedisHashSqlParser(RedisClient rc) {
        this.rc = rc;
    }

    public RedisHashSqlParser(RedisClient rc, boolean isPrepared) {
        this.rc = rc;
        this.isPrepared = isPrepared;
    }

    public static void registerParser(IRedisParser iRedisParser) {
        iRedisParserList.add(iRedisParser);
    }

    /**
     * 解析sql
     * <pre>
     *     select
     *     update
     *     delete
     *     insert into
     * </pre>
     *
     * @param sql
     * @throws SQLException
     */
    public void parser(String sql) throws SQLException {
        this.sql = sql.trim();
        // 先识别是哪个类来处理
        for (IRedisParser _iRedisParser : iRedisParserList) {
            if (_iRedisParser.checkType(sql)) {
                iRedisParser = _iRedisParser;
                break;
            }
        }
        // 有处理类，就进行处理
        if (iRedisParser != null) {
            // 初始化
            iRedisParser.init(rc, isPrepared);
        } else {
            throw new SQLException("sql语法不标准，无法识别到对应的处理类。");
        }
    }

    public String[] getFieldsArr() {
        return iRedisParser.getFieldsArr();
    }

    public int execute(Map<String, String> _fieldMap) throws SQLException {
        return iRedisParser.run(_fieldMap);
    }

    public RedisResultSet getRedisResultSet() {
        return iRedisParser.getRedisResultSet();
    }
}
