package com.config;
import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by Mobin on 2017/9/12.
 */
public class DatabaseConnection {
    private static DataSource MysqlDataSource;
    public DatabaseConnection(){}
    static {
        try{
            InputStream in = DatabaseConnection.class.getClassLoader()
                    .getResourceAsStream("mysql.properties");
            Properties props = new Properties();
            props.load(in);
            MysqlDataSource = DruidDataSourceFactory.createDataSource(props);
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
    public static Connection getMysqlConnection() throws SQLException{
        return MysqlDataSource.getConnection();
    }

}
