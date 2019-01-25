package com.cqx.cli.tool.impl;

import com.cqx.cli.util.FileUtil;

/**
 * 写入处理
 *
 * @author chenqixu
 */
public class WriteResultSetDeal implements IResultSetDeal {

    public static final String newLine = System.getProperty("line.separator");
    private static final String valueSplit = "|";
    private FileUtil fileUtil;

    public WriteResultSetDeal(FileUtil fileUtil) {
        this.fileUtil = fileUtil;
    }

    @Override
    public void execValue(String msg) {
        fileUtil.write(msg);
    }

    @Override
    public void execValueSplit() {
        fileUtil.write(valueSplit);
    }

    @Override
    public void execValueEnd() {
        fileUtil.write(newLine);
    }
}
