package com.cqx.cli.util;

import com.cqx.cli.bean.SqlType;
import com.cqx.cli.tool.CmdTool;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.sql.Types;

/**
 * SqlTypeUtil
 *
 * @author chenqixu
 */
public class SqlTypeUtil {

    private Map<Integer, String> typeMap = new HashMap<>();

    public void init() {
        try {
            Class clazz = Class.forName("java.sql.Types");
            Field[] fields = clazz.getFields();
            for (Field field : fields) {
                SqlType sqlType = new SqlType(field.getName(), field.getInt(clazz));
                CmdTool.debug(sqlType.toString());
                typeMap.put(sqlType.getTypeCode(), sqlType.getName());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getNameByteType(int type) {
        return typeMap.get(type);
    }

    public Map<Integer, String> getTypeMap() {
        return typeMap;
    }
}
