package com.common;

import com.config.DatabaseConnection;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2018/5/17
 * Time: 下午2:46
 */
public class BinlogOption {
    private static final Logger log = LoggerFactory.getLogger(BinlogOption.class);
    public String dirPath = null;
    public int count = 0;
    public int countInterval = 50000;  //每50000写入一次
    public static String database = "mobinDB";
    public Long binlogPosition = null;  //自上次同步的游标开始同步
    public String binlogFileName = null;
    public FileSystem fs = null;
    public boolean isSyncTable = true;  //是否同步该表

    public boolean syncByBinlogFile = false;  //通过指定binlog进行同步,默认从数据库获取上次同步的游标
    public String binlogFilePath = null;

    public BinlogOption(String[] args){
        for (int i = 0; i < args.length; i ++){
            String arg = args[i];
            switch (arg){
                case "-syncByBinlogFile":
                    syncByBinlogFile = Boolean.parseBoolean(args[++ i]);
                    break;
                case "-countInterval":
                    countInterval = Integer.parseInt(args[++ i]);
                    break;
                case "-binlogFilePath":
                    binlogFilePath = args[++ i];
                    break;
                default:
                    log.error("参数无效：" + arg);
                    System.exit(-1);
            }
        }
        if (!syncByBinlogFile){
            getNextPosition();
        }else {
            checkBinlogFilePath(binlogFilePath);
        }
    }

    private void checkBinlogFilePath(String binlogFilePath) {
        if (binlogFilePath == null){
            log.error("binlogFilePath路径无效：{}",binlogFilePath);
            System.exit(-1);
        }
    }


    public  void getNextPosition(){
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = DatabaseConnection.getMysqlConnection();
            String SQL = "SELECT nextposition,binlogfilename FROM t_position";
            ps = conn.prepareStatement(SQL);
            rs = ps.executeQuery();
            while (rs.next()) {
                binlogPosition = rs.getLong("nextposition");
                binlogFileName = rs.getString("binlogfilename");
            }
            log.info("数据库最新nextposition={},binlogfilename={}",binlogPosition,binlogFileName);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                ps.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
