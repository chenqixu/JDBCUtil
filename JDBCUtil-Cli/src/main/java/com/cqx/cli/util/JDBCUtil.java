package com.cqx.cli.util;

import com.cqx.cli.bean.CmdBean;
import com.cqx.cli.tool.CmdTool;
import com.cqx.cli.tool.impl.IResultSetDeal;
import com.cqx.cli.tool.impl.PrintResultSetDeal;
import com.cqx.cli.tool.impl.WriteResultSetDeal;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC工具
 *
 * @author chenqixu
 */
public class JDBCUtil {

    protected Map<String, String> driverMap = new HashMap<>();
    protected Connection conn = null;
    protected Statement stm = null;
    protected PreparedStatement pstmt = null;
    protected CmdBean cmdBean = null;
    protected SqlTypeUtil sqlTypeUtil = new SqlTypeUtil();

    public JDBCUtil() {
        // loadDriver
        loadDriver();
        // init SqlTypes
        sqlTypeUtil.init();
    }

    public JDBCUtil(Connection conn) {
        this();
        this.conn = conn;
    }

    public JDBCUtil(CmdBean cmdBean) {
        this();
        this.cmdBean = cmdBean;
        try {
            String DriverClassName = driverMap.get(cmdBean.getType());
            Class.forName(DriverClassName);
            conn();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void conn() throws SQLException {
        DriverManager.setLoginTimeout(15); // 超时
        conn = DriverManager.getConnection(cmdBean.getDns(), cmdBean.getUsername(), cmdBean.getPassword());
        CmdTool.println("conn success! conn is " + conn);
    }

    public void checkConnection() throws SQLException {
        //如果连接不正常
        CmdTool.debug("conn：" + conn);
        //释放
//        closeAll();
        //重连
//        conn();
    }

    /**
     * 执行sql查询语句，返回结果
     *
     * @param sql
     * @return ResultSet
     */
    public ResultSet executeQuery(String sql) throws SQLException {
        checkConnection();
        ResultSet rs;
//        try {
        stm = conn.createStatement();
        rs = stm.executeQuery(sql);
//        } finally {
//            //如果在rs.next()之前关闭了Statement或PreparedStatement，会导致下面的异常
//            //java.sql.SQLException: 关闭的语句: next
//            closeStm();
//        }
        if (rs == null) {
            return null;
        }
        return rs;
    }

    public boolean descMetaData(ResultSet rs) throws SQLException {
        int cnt = 0;
        if (rs != null) {
            try {
                if (rs.next()) {
                    for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                        String columnName = rs.getMetaData().getColumnName(i + 1);
                        int columnType = rs.getMetaData().getColumnType(i + 1);
                        CmdTool.println("columnName：" + columnName + "，columnType：" + columnType +
                                "，Type：" + sqlTypeUtil.getNameByteType(columnType));
                        cnt++;
                    }
                }
            } finally {
                closeResultSet(rs);
            }
        }
        return cnt > 0;
    }

    /**
     * 执行更新语句，返回执行结果
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    public int executeUpdate(String sql) throws SQLException {
        checkConnection();
        int result = -1;
        try {
            stm = conn.createStatement();
            result = stm.executeUpdate(sql);
        } finally {
            closeStm();
        }
        return result;
    }

    /**
     * 执行存储过程
     *
     * @param sql
     * @throws SQLException
     */
    public void prepareCall(String sql) throws SQLException {
        sql = "{" + sql + "}";
        CallableStatement callStmt = null;
        boolean result;
        try {
            callStmt = conn.prepareCall(sql);
            result = callStmt.execute();
        } finally {
            closeCallableStatement(callStmt);
        }
        CmdTool.println("prepareCall：" + result);
    }

    /**
     * 无参的预处理操作
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    public long executeBatch(String sql) throws SQLException {
        return executeBatch(sql, null);
    }

    /**
     * 通过掺入List参数来进行相关组件的操作
     *
     * @param sql
     * @param params
     * @throws SQLException
     */
    public long executeBatch(String sql, List<List<String>> params) throws SQLException {
        checkConnection();
        long count = 0;
        try {
            conn.setAutoCommit(false);// 关闭自动提交
            if (pstmt == null)
                pstmt = conn.prepareStatement(sql);// 预编译SQL
            if (params != null)
                for (List<String> param : params) {
                    int index = 1;
                    for (String p : param) {
                        if (p == null || "".equals(p)) {
                            pstmt.setObject(index++, null);
                        } else {
                            pstmt.setString(index++, p);
                        }
                    }
                    pstmt.addBatch();
                }
            int[] cnt = pstmt.executeBatch();
            conn.commit();
            count = arraySum(cnt);
        } catch (SQLException e) {
            try {
                if (pstmt != null)
                    pstmt.close();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            throw e;
        }
        return count;
    }

    /**
     * <pre>
     *     desc tablename
     *     export d:/test.txt as select * from a
     *     import d:/test.txt as insert into a
     * </pre>
     *
     * @param sql
     * @throws SQLException
     */
    public void parserSql(String sql) throws Exception {
        try {
            String newSql = sql.trim();
            if (newSql.startsWith("desc") && cmdBean.getType().equals("oracle")) {
                String tableName = newSql.replace("desc", "").trim();
                newSql = desc(tableName);
                CmdTool.debug("newSql：" + newSql);
                boolean flag = descMetaData(executeQuery(newSql));
                if (!flag) {
                    newSql = desc(cmdBean.getUsername(), tableName);
                    CmdTool.debug("newSql：" + newSql);
                    printlnResultSet(executeQuery(newSql), 0);
                }
            } else if (newSql.startsWith("export")) {
                //去掉export
                //使用" as "切割
                String _sql = newSql.replace("export", "").trim();
                String[] arr = _sql.split(" as ");
                if (arr.length == 2) {
                    exportData(arr[0], arr[1]);
                }
            } else if (newSql.startsWith("import")) {
                //去掉import
                //使用" as "切割
                String _sql = newSql.replace("import", "").trim();
                String[] arr = _sql.split(" as ");
                if (arr.length == 2) {
                    importData(arr[0], arr[1]);
                }
            } else if (newSql.startsWith("select")) {
                printlnResultSet(executeQuery(newSql));
            } else if (newSql.startsWith("insert")) {
                printlnUpdateResult(executeUpdate(newSql));
            } else if (newSql.startsWith("begin")) {//执行存储过程
                //前面删掉了结尾的;，这里需要补上
                newSql = newSql + ";";
                printlnUpdateResult(executeBatch(newSql));
            } else if (newSql.startsWith("call")) {//执行存储过程
                prepareCall(newSql);
            } else {
                String reason = "无法识别的语句，sql：" + newSql;
                String SQLState = "err";
                int vendorCode = -1;
                throw new SQLException(reason, SQLState, vendorCode);
            }
        } finally {
            closeStm();
        }
    }

    public void exportData(String filename, String sql) throws Exception {
        FileUtil fileUtil = new FileUtil();
        //创建文件
        fileUtil.createFile(filename, "UTF-8");
        //执行sql语句并将结果写入文件
        int count = writeResultSet(executeQuery(sql), fileUtil);
        //关闭文件
        fileUtil.closeWrite();
        CmdTool.println("success save data to " + filename + "，file count is " + count);
    }

    public void importData(String filename, String sql) throws Exception {
        FileUtil fileUtil = new FileUtil();
        //读取文件
        fileUtil.getFile(filename, "UTF-8");
        //生成sql
        List<String> sqllist = fileUtil.read(sql);
        //关闭文件
        fileUtil.closeRead();
        //执行sql
        int count = 0;
        int fail = 0;
        for (String str : sqllist) {
            CmdTool.debug("sql语句：" + str);
            long result = printlnUpdateResult(executeUpdate(str));
            if (result == 0 || result == 1) count++;
            else fail++;
        }
        CmdTool.println("success import data to " + sql + "，success count is " + count + "，fail count is " + fail);
    }

    public String desc(String owner, String tablename) {
        StringBuffer sb = new StringBuffer();
        sb.append("select column_name||' '||data_type||'('||data_length||')' from ALL_TAB_COLUMNS ")
                .append("where table_name=upper('")
                .append(tablename)
                .append("') and owner=upper('")
                .append(owner)
                .append("')");
        return sb.toString();
    }

    public String desc(String tablename) {
        StringBuffer sb = new StringBuffer();
        sb.append("select * from ")
                .append(tablename)
                .append(" where rownum=1 ");
        return sb.toString();
    }

    public long printlnUpdateResult(long result) {
        CmdTool.println("执行结果：" + result);
        return result;
    }

    public void printlnResultSet(ResultSet rs, int limit) throws SQLException {
        dealResultSet(rs, limit, new PrintResultSetDeal());
    }

    public void printlnResultSet(ResultSet rs) throws SQLException {
        printlnResultSet(rs, 5);
    }

    public int writeResultSet(ResultSet rs, FileUtil fileUtil) throws SQLException {
        return dealResultSet(rs, 0, new WriteResultSetDeal(fileUtil));
    }

    public <T extends IResultSetDeal> int dealResultSet(ResultSet rs, int limit, T t) throws SQLException {
        int cnt = 0;
        if (rs != null) {
            try {
                while (rs.next() && isLimit(cnt, limit)) {
                    for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                        int sqlDataType = rs.getMetaData().getColumnType(i + 1);
                        switch (sqlDataType) {
                            case Types.DATE:
                                t.execValue(String.valueOf(rs.getDate(i + 1)));
                                break;
                            case Types.TIME:
                                t.execValue(String.valueOf(rs.getTime(i + 1)));
                                break;
                            case Types.TIMESTAMP:
                                t.execValue(String.valueOf(rs.getTimestamp(i + 1)));
                                break;
                            case Types.VARCHAR:
                                t.execValue(rs.getString(i + 1));
                            case Types.NUMERIC:
                                t.execValue(String.valueOf(rs.getInt(i + 1)));
                            default:
                                t.execValue(rs.getString(i + 1));
                                break;
                        }
                        if (i < (rs.getMetaData().getColumnCount() - 1))
                            t.execValueSplit();
                    }
                    t.execValueEnd();
                    cnt++;
                }
            } finally {
                closeResultSet(rs);
            }
        }
        if (cnt == 0) {
            CmdTool.println("no record.");
        }
        return cnt;
    }

    private boolean isLimit(int cnt, int limit) {
        if (limit > 0) {//有限制
            return cnt < limit;
        } else {//无限制
            return true;
        }
    }

    private void closeConn() {
        if (conn != null)
            try {
                conn.close();
                CmdTool.debug("close conn：" + conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
    }

    private void closeStm() {
        if (stm != null)
            try {
                stm.close();
                CmdTool.debug("close stm：" + stm);
            } catch (SQLException e) {
                e.printStackTrace();
            }
    }

    private void closeStmt() {
        if (pstmt != null)
            try {
                pstmt.close();
                CmdTool.debug("close pstmt：" + pstmt);
            } catch (SQLException e) {
                e.printStackTrace();
            }
    }

    private void closeResultSet(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
                CmdTool.debug("close ResultSet：" + rs);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void closeCallableStatement(CallableStatement callStmt) {
        if (callStmt != null) {
            try {
                callStmt.close();
                CmdTool.debug("close CallableStatement：" + callStmt);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void closeAll() {
        closeStm();
        closeStmt();
        closeConn();
    }

    private long arraySum(int[] arr) {
        if (arr == null) {
            return 0;
        }
        long res = 0;
        for (int i : arr) {
            if (i > 0) {
                res += i;
            }
        }
        return res;
    }

    private void loadDriver() {
        driverMap.put("oracle", "oracle.jdbc.driver.OracleDriver");
        driverMap.put("mysql", "com.mysql.jdbc.Driver");
        driverMap.put("redis", "com.cqx.redis.jdbc.RedisDriver");
        driverMap.put("hive", "org.apache.hive.jdbc.HiveDriver");
//        driverMap.put("timesten", "TimestenJDBCTest");
    }

    protected void finalize() {
        closeAll();
    }

    public Connection getConn() {
        return conn;
    }
}
