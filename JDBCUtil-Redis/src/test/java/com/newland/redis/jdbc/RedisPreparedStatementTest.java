package com.newland.redis.jdbc;

import com.cqx.redis.client.RedisClient;
import com.cqx.redis.client.RedisFactory;
import com.cqx.redis.jdbc.RedisPreparedStatement;
import com.cqx.redis.utils.CommonUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;

public class RedisPreparedStatementTest {

    private static final Logger logger = LoggerFactory.getLogger(RedisPreparedStatementTest.class);
    private RedisClient redisClient;
    private long task_template_id = 10000L;
    private int data_index = 1;
    private RedisPreparedStatement redisPreparedStatement;

    @Before
    public void before() {
        redisClient = RedisFactory.builder()
                .setMode(RedisFactory.CLUSTER_MODE_TYPE)
                .setIp_ports("10.1.8.200:10000,10.1.8.200:10001,10.1.8.200:10002,10.1.8.200:10003,10.1.8.200:30000,10.1.8.200:30001,10.1.8.200:30002,10.1.8.200:30003")
                .build();
        redisPreparedStatement = new RedisPreparedStatement(redisClient);
    }

    @After
    public void after() throws SQLException {
        redisClient.close();
    }

    @Test
    public void insertInto() throws Exception {
        //        String fields = "task_template_id,file_name,source_machine,source_path,check_file_path,data_index,source_file_suffix,check_file_suffix,source_file_createTime,file_size,file_recordNum,file_cycle,merge_name,insert_time,file_status,file_status_updateTime";
        String fields = "task_template_id,file_name,source_machine,data_index,file_status,insert_time";
        String field = ":" + fields.replaceAll(",", ",:");
        String sql = "insert into fjbi_busdatacollect_list(" + fields + ") values(" + field + ")";
        redisPreparedStatement.prepare(sql);
        redisPreparedStatement.setString(1, String.valueOf(task_template_id));
        redisPreparedStatement.setString(2, "S0400220190813240000598989612");
        redisPreparedStatement.setString(3, "10.1.8.205");
        redisPreparedStatement.setString(4, String.valueOf(data_index));
        redisPreparedStatement.setString(5, "1");
        redisPreparedStatement.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
        redisPreparedStatement.addBatch();
        int[] ret = redisPreparedStatement.executeBatch();
        logger.info("ret：{}", Arrays.asList(CommonUtils.changeIntToIntegerArray(ret)));
    }

    @Test
    public void insertIntoSelect() throws Exception {
        //        String fields = "task_template_id,file_name,source_machine,source_path,check_file_path,data_index,source_file_suffix,check_file_suffix,source_file_createTime,file_size,file_recordNum,file_cycle,merge_name,insert_time,file_status,file_status_updateTime";
        String fields = "task_template_id,file_name,source_machine,data_index,file_status";
        StringBuffer where = new StringBuffer();
        where.append(" where task_template_id=:task_template_id ")
                .append(" and data_index=:data_index ")
                .append(" and file_status=:file_status ");
        String sql = "insert into fjbi_busdatacollect_list(" + fields + ") select " + fields + " from fjbi_busdatacollect_list " + where.toString();
        redisPreparedStatement.prepare(sql);
        redisPreparedStatement.setString(1, String.valueOf(task_template_id));
        redisPreparedStatement.setString(2, String.valueOf(data_index));
        redisPreparedStatement.setString(3, "1");
        redisPreparedStatement.addBatch();
        int[] ret = redisPreparedStatement.executeBatch();
        logger.info("ret：{}", Arrays.asList(CommonUtils.changeIntToIntegerArray(ret)));
    }

    @Test
    public void select() throws Exception {
        String sql = "select task_template_id,file_name,source_machine,source_path,check_file_path,data_index,file_status,insert_time from fjbi_busdatacollect_list where task_template_id=:task_template_id and data_index=:data_index and file_status=:file_status";
        redisPreparedStatement.prepare(sql);
        redisPreparedStatement.setString(1, String.valueOf(task_template_id));
        redisPreparedStatement.setString(2, String.valueOf(data_index));
        redisPreparedStatement.setString(3, "1");
        ResultSet rs = redisPreparedStatement.executeQuery();
        ResultSetMetaData rsMeta = rs.getMetaData();
        while (rs.next()) {
            for (int i = 0, size = rsMeta.getColumnCount(); i < size; ++i) {
                int columenType = rsMeta.getColumnType(i + 1);
                Object value;
                if (columenType == 93) {
                    value = rs.getTimestamp(i + 1);
                } else if (columenType == 2) {
                    value = rs.getInt(i + 1);
                } else if (columenType == 12) {
                    value = rs.getString(i + 1);
                } else {
                    value = rs.getObject(i + 1);
                }
                logger.info("ColumnLabel：{}，value：{}，columenType：{}", rsMeta.getColumnLabel(i + 1), value, columenType);
            }
        }
        rs.close();
    }

    @Test
    public void update() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("update fjbi_busdatacollect_list ")
                .append("set insert_time=? ")
                .append("where ")
                .append(" task_template_id=? ")
                .append(" and data_index=? ")
                .append(" and file_status=? ")
                .append(" and file_name=? ")
        ;
        redisPreparedStatement.prepare(sb.toString());
        redisPreparedStatement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        redisPreparedStatement.setString(2, String.valueOf(task_template_id));
        redisPreparedStatement.setString(3, String.valueOf(data_index));
        redisPreparedStatement.setString(4, "1");
        redisPreparedStatement.setString(5, "S0400220190813240000598989609");
        redisPreparedStatement.addBatch();
        int[] ret = redisPreparedStatement.executeBatch();
        logger.info("ret：{}", Arrays.asList(CommonUtils.changeIntToIntegerArray(ret)));
    }

    @Test
    public void delete() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("delete from fjbi_busdatacollect_list ")
                .append("where ")
                .append(" task_template_id=? ")
                .append(" and data_index=? ")
                .append(" and file_status=? ")
                .append(" and file_name=? ");
        redisPreparedStatement.prepare(sb.toString());
        redisPreparedStatement.setString(1, String.valueOf(task_template_id));
        redisPreparedStatement.setString(2, String.valueOf(data_index));
        redisPreparedStatement.setString(3, "1");
        redisPreparedStatement.setString(4, "S0400220190813240000598989609");
        redisPreparedStatement.addBatch();
        int[] ret = redisPreparedStatement.executeBatch();
        logger.info("ret：{}", Arrays.asList(CommonUtils.changeIntToIntegerArray(ret)));
    }

    @Test
    public void string() {
        String sql = "insert  into tablename select 1,insert,2 from b";
        logger.info("sql【{}】", sql.replaceFirst("insert", ""));
        logger.info("sql【{}】", sql.replaceFirst("insert", "").trim());
    }

}