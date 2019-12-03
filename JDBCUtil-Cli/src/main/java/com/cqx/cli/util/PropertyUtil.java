package com.cqx.cli.util;

import com.cqx.cli.tool.CmdTool;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertyUtil {

    private final static String spltstr = File.separator;
    private final static String rootPath = System.getProperty("user.dir");
    private final static String logpath = rootPath + spltstr + "conf" + spltstr + "loginfofactory.properties";
    private Map<String, String> propertyValueMap = new HashMap<>();

    public static void printPath() {
        System.out.println("#rootPath#" + rootPath);
    }

    public static String getProperty(String name, String defaultsValue) {
        String value = getProperty(name);
        return value == "" ? defaultsValue : value;
    }

    public static String getProperty(String name) {
        String result = "";
        File f = null;
        FileInputStream pInStream = null;
        Properties p = null;
        try {
            f = new File(logpath);
            pInStream = new FileInputStream(f);
            p = new Properties();
            p.load(pInStream);
            Enumeration<?> enuVersion = p.propertyNames();
            while (enuVersion.hasMoreElements()) {
                enuVersion.nextElement();
            }
            result = p.getProperty(name);
            f = null;
            pInStream.close();
            p.clear();
            p = null;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (f != null)
                f = null;
            if (pInStream != null)
                try {
                    pInStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            if (p != null) {
                p.clear();
                p = null;
            }
        }
        return result;
    }

    public void init(String conf_path) {
        System.out.println("PropertyUtil init：" + conf_path);
        File f = null;
        FileInputStream pInStream = null;
        Properties p = null;
        try {
            f = new File(conf_path);
            pInStream = new FileInputStream(f);
            p = new Properties();
            p.load(pInStream);
            Enumeration<?> enuVersion = p.propertyNames();
            while (enuVersion.hasMoreElements()) {
                String key = enuVersion.nextElement().toString();
                String value = p.getProperty(key);
                CmdTool.debug("key：" + key + "，value：" + value);
                propertyValueMap.put(key, value);
            }
            f = null;
            pInStream.close();
            p.clear();
            p = null;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (f != null)
                f = null;
            if (pInStream != null)
                try {
                    pInStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            if (p != null) {
                p.clear();
                p = null;
            }
        }
    }

    public String getValueByKey(String key) {
        return propertyValueMap.get(key);
    }
}
