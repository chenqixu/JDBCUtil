package com.cqx.cli.tool;

import com.cqx.cli.bean.CmdBean;
import com.cqx.cli.util.PropertyUtil;
import org.apache.commons.cli.*;

/**
 * 解析参数
 *
 * @author chenqixu
 */
public class OptionsTool {

    public static final String DB_TYPE = "db.type";
    public static final String DB_USER_NAME = "db.user_name";
    public static final String DB_PASSWD = "db.passwd";
    public static final String DB_TNS = "db.tns";
    public static final String LOG_LEVEL = "log.level";

    private CmdBean cmdBean;

    private OptionsTool() {
    }

    public static OptionsTool newbuilder() {
        return new OptionsTool();
    }

    public static String[] propertyToArgs(PropertyUtil propertyUtil) {
        String[] args = new String[10];
        args[0] = "-t";
        args[1] = propertyUtil.getValueByKey(DB_TYPE);
        args[2] = "-u";
        args[3] = propertyUtil.getValueByKey(DB_USER_NAME);
        args[4] = "-p";
        args[5] = propertyUtil.getValueByKey(DB_PASSWD);
        args[6] = "-d";
        args[7] = propertyUtil.getValueByKey(DB_TNS);
        args[8] = "-l";
        args[9] = propertyUtil.getValueByKey(LOG_LEVEL);
        return args;
    }

    public CmdBean getCmdBean() {
        return cmdBean;
    }

    public OptionsTool parser(String[] args) throws ParseException {
        Options options = new Options();
        Option option;
        //-t type
        option = new Option("t", "type", true, "type");
        option.setRequired(true);
        options.addOption(option);
        //-u username
        option = new Option("u", "username", true, "username");
        option.setRequired(true);
        options.addOption(option);
        //-p password
        option = new Option("p", "password", true, "password");
        option.setRequired(true);
        options.addOption(option);
        //-d dns
        option = new Option("d", "dns", true, "dns");
        option.setRequired(true);
        options.addOption(option);
        //-l loglevel
        option = new Option("l", "loglevel", true, "loglevel");
        option.setRequired(false);
        options.addOption(option);
        //parser
        CommandLineParser parser = new GnuParser();
        CommandLine commandLine = parser.parse(options, args);
        //getOptionValue
        cmdBean = CmdBean.newbuilder()
                .setType(commandLine.getOptionValue('t'))
                .setUsername(commandLine.getOptionValue('u'))
                .setPassword(commandLine.getOptionValue('p'))
                .setDns(commandLine.getOptionValue('d'));
        if (commandLine.getOptionValue('l') != null) cmdBean.setLoglevel(commandLine.getOptionValue('l'));
        return this;
    }
}
