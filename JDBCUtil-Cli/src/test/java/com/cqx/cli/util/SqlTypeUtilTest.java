package com.cqx.cli.util;

import com.cqx.cli.tool.CmdTool;
import org.junit.Test;

public class SqlTypeUtilTest {

    @Test
    public void init() {
        CmdTool.setDebug();
        SqlTypeUtil sqlTypeUtil = new SqlTypeUtil();
        sqlTypeUtil.init();
    }
}