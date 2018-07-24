package com.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2018/5/17
 * Time: 上午10:20
 */
public class Config {

    private static final Properties config = loadConfig();

    private static Properties loadConfig(){
        InputStream in = Config.class.getResourceAsStream("/binlog2Hive_conf.properties");
        Properties config = new Properties();
        try {
            config.load(in);
            in.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load binlog2Hive_conf.properties");
        }
        return config;
    }

    public static String getStringProperty(String key){
        String value = config.getProperty(key);
        if (value == null)
            return null;
        return value.trim();
    }

    public static String getPath(String tableName){
        String path = getStringProperty("default_" + tableName + "_path");
        return path;
    }
}
