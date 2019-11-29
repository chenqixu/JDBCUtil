package com.newland.redis.jdbc;

import com.cqx.redis.client.RedisClient;
import com.cqx.redis.client.RedisFactory;
import com.cqx.redis.jdbc.RedisStatement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class RedisStatementTest {

    private static final Logger logger = LoggerFactory.getLogger(RedisStatementTest.class);
    private RedisClient redisClient;
    private long task_template_id = 10000L;
    private int data_index = 1;
    private RedisStatement redisStatement;

    @Before
    public void setUp() throws Exception {
        redisClient = RedisFactory.builder()
                .setMode(RedisFactory.CLUSTER_MODE_TYPE)
                .setIp_ports("10.1.8.200:10000,10.1.8.200:10001,10.1.8.200:10002,10.1.8.200:10003,10.1.8.200:30000,10.1.8.200:30001,10.1.8.200:30002,10.1.8.200:30003")
                .build();
        redisStatement = new RedisStatement(redisClient);
    }

    @After
    public void tearDown() throws Exception {
        redisClient.close();
    }

    @Test
    public void insertInto() throws Exception {
        String fields = "task_template_id,file_name,source_machine,data_index,file_status";
        StringBuffer values = new StringBuffer();
        values.append(task_template_id)
                .append(",")
                .append("'S0400220190813240000598989610'")
                .append(",")
                .append("'10.1.8.205'")
                .append(",")
                .append(data_index)
                .append(",")
                .append("1");
        String sql = "insert into fjbi_busdatacollect_list(" + fields + ") values(" + values.toString() + ")";
        int ret = redisStatement.executeUpdate(sql);
        logger.info("ret：{}", ret);
    }

    @Test
    public void insertIntoSelect() throws Exception {
        String fields = "task_template_id,file_name,source_machine,data_index,file_status";
        StringBuffer where = new StringBuffer();
        where.append(" where task_template_id='")
                .append(task_template_id)
                .append("' and data_index in('1','2','3','0') and file_status in('21','20','0','1','31','40','41','50','51')");
        String sql = "insert into fjbi_busdatacollect_list(" + fields + ") select " + fields + " from fjbi_busdatacollect_list " + where.toString();
        int ret = redisStatement.executeUpdate(sql);
        logger.info("ret：{}", ret);
    }

    @Test
    public void select() throws Exception {
//        String sql = "select field1,field2,field3 from fjbi_busdatacollect_list where task_template_id='task001' and file_status='01' and data_index='05' and file_name='aaa'";
        String sql = "select task_template_id,file_name,source_machine,source_path,check_file_path,data_index,file_status from fjbi_busdatacollect_list where task_template_id in('10000') and data_index in('1','2','3','0') and file_status in('21','20','0','1','31','40','41','50','51')";
//        String sql = "select task_template_id,file_name,source_machine,source_path,check_file_path,data_index,file_status from fjbi_busdatacollect_list where task_template_id in('10000') and data_index in('1','2','3','0') and file_status in('21','20','0','1','31','40','41','50','51') and file_name='S0400220190813240000598989609'";
        ResultSet rs = redisStatement.executeQuery(sql);
        ResultSetMetaData rsMeta = rs.getMetaData();
        while (rs.next()) {
            for (int i = 0, size = rsMeta.getColumnCount(); i < size; ++i) {
                Object value = rs.getObject(i + 1);
//                logger.info("ColumnLabel：{}，value：{}", rsMeta.getColumnLabel(i + 1), value);
            }
        }
        rs.close();
    }

    @Test
    public void update() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("update fjbi_busdatacollect_list ")
//                .append("set file_status=20 ,file_status_updateTime=sysdate ")
                .append("set file_status=1 ")
                .append(" where task_template_id=")
                .append(task_template_id)
                .append(" and data_index = ")
                .append(data_index)
                .append(" and file_status=1 ")
                .append(" and file_name='S0400220190813240000598989611'");
        int ret = redisStatement.executeUpdate(sb.toString());
        logger.info("ret：{}", ret);
    }

    @Test
    public void delete() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("delete from fjbi_busdatacollect_list ")
                .append("where ")
                .append(" task_template_id=")
                .append(task_template_id)
                .append(" and data_index = ")
                .append(data_index)
                .append(" and file_status=1 ")
                .append(" and file_name='S0400220190813240000598989610'");
        int ret = redisStatement.executeUpdate(sb.toString());
        logger.info("ret：{}", ret);
    }

}