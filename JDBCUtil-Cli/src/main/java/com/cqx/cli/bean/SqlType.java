package com.cqx.cli.bean;

/**
 * SqlType
 *
 * @author chenqixu
 */
public class SqlType {
    private String name;
    private int typeCode;
    private Class<?> type;

    public SqlType() {
    }

    public SqlType(String name, int typeCode) {
        this(name, typeCode, null);
    }

    public SqlType(String name, int typeCode, Class<?> type) {
        this.name = name;
        this.typeCode = typeCode;
        this.type = type;
    }

    public String toString() {
        return "name：" + this.name + "，typeCode：" + this.typeCode + "，type：" + this.type;
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

    public Class<?> getType() {
        return type;
    }

    public void setType(Class<?> type) {
        this.type = type;
    }
}
