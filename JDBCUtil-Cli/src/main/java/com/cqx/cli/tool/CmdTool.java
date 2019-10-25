package com.cqx.cli.tool;

import com.cqx.cli.bean.CmdBean;
import com.cqx.cli.util.JDBCUtil;

import java.util.Scanner;

/**
 * 命令工具
 *
 * @author chenqixu
 */
public class CmdTool {
    private static String loglevel = "info";
    private Scanner scanner;

    private CmdTool() {
        scanner = new Scanner(System.in);
    }

    public static CmdTool newbuilder() {
        return new CmdTool();
    }

    public static void main(String[] args) throws Exception {
        if (isWindow()) {
//            args = setParam("-t oracle -u bishow -p C%MuhN#q$4 -d jdbc:oracle:thin:@10.1.0.242:1521:ywxx -l info");
            args = setParam("-t oracle -u web -p T%vdNV#i$2 -d jdbc:oracle:thin:@10.1.0.242:1521:ywxx -l info");
//            args = setParam("-t oracle -u devload -p V%jjmZ#n$1 -d jdbc:oracle:thin:@10.1.0.242:1521:ywxx -l debug");
//            args = setParam("-t redis -u redis -p redis -d 192.168.230.128:6379 -l info");
//            args = setParam("-t redis -u redis -p redis -d 10.1.4.185:6380,10.1.4.185:6381,10.1.4.185:6382,10.1.4.185:6383,10.1.4.185:6384,10.1.4.185:6385 -l debug");
//            args = setParam("-t mysql -u udap -p udap -d jdbc:mysql://127.0.0.1:3306/acccountdb?useUnicode=true&characterEncoding=gbk -l info");
        }
        CmdTool.newbuilder().run(args);
    }

    public static String[] setParam(String param) {
        return param.split(" ", -1);
    }

    public static void print(String msg) {
        System.out.print(msg);
    }

    public static void println(String msg) {
        System.out.println(msg);
    }

    public static void debug(String msg) {
        if (loglevel.equals("debug"))
            System.out.println(msg);
    }

    public static void setDebug() {
        loglevel = "debug";
    }

    /**
     * 是否是本地测试
     *
     * @return
     */
    public static boolean isWindow() {
        String systemType = System.getProperty("os.name");
        if (systemType.toUpperCase().startsWith("WINDOWS")) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * <pre>
     *     1、类型 -t --type
     *     2、用户 -u --username
     *     3、密码 -p --password
     *     4、连接串 -d --dns
     * </pre>
     *
     * @param args
     */
    public void run(String[] args) throws Exception {
        CmdBean cmdBean = OptionsTool.newbuilder().parser(args).getCmdBean();
        loglevel = cmdBean.getLoglevel();
        println("cmdBean：" + cmdBean);
        JDBCUtil jdbcUtil = new JDBCUtil(cmdBean);
        String type = cmdBean.getType();
        print(type + ">");
        while (scanner.hasNextLine()) {
            String cmd = scanner.nextLine();
            if (cmd.endsWith(";")) cmd = cmd.substring(0, cmd.length() - 1);
            if (cmd.trim().equalsIgnoreCase("exit")
                    || cmd.trim().equalsIgnoreCase("quit")) {
                jdbcUtil.closeAll();
                System.exit(0);
            } else {
                debug("you input:" + cmd);
                try {
                    jdbcUtil.parserSql(cmd);
                } catch (Exception e) {
                    println(e.getMessage());
                }
                print(type + ">");
            }
        }
    }
}
