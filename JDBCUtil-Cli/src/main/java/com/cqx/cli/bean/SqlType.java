package com.cqx.cli.bean;

/**
 * SqlType
 *
 * @author chenqixu
 */
public class SqlType {
    private String name;
    private int typeCode;

    public SqlType() {
    }

    public SqlType(String name, int typeCode) {
        this.name = name;
        this.typeCode = typeCode;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getTypeCode() {
        return typeCode;
    }

    public void setTypeCode(int typeCode) {
        this.typeCode = typeCode;
    }

    public String toString() {
        return "name：" + this.name + "，typeCode：" + this.typeCode;
    }
}
