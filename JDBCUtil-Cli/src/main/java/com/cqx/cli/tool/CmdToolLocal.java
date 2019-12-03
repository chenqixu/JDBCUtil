package com.cqx.cli.tool;

import com.cqx.cli.util.PropertyUtil;

/**
 * CmdToolLocal
 *
 * @author chenqixu
 */
public class CmdToolLocal {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("You need input conf file.");
            System.exit(-1);
        }
        String conf_path = args[0];
        PropertyUtil propertyUtil = new PropertyUtil();
        propertyUtil.init(conf_path);
        CmdTool.newbuilder().run(OptionsTool.propertyToArgs(propertyUtil));
    }

}
