package com.cqx.redis.jdbc;

/**
 * RedisColumn
 *
 * @author chenqixu
 */
public class RedisColumn {
    private String label;
    private String name;
    private int type;
    private String typeName;
    private String className;

    public RedisColumn() {
    }

    public RedisColumn(String label, String name, int type, String typeName, String className) {
        this.label = label;
        this.name = name;
        this.type = type;
        this.typeName = typeName;
        this.className = className;
    }

    @Override
    public String toString() {
        return "label：" + label + "，name：" + name + "，type：" + type + "，typeName：" + typeName + "，className：" + className;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }
}
