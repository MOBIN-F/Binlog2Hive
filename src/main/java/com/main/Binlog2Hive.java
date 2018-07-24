package com.main;

import com.common.BinlogOption;
import com.bixuange.BinlogClient;
import com.common.FSUtils;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2018/5/20
 * Time: 下午10:09
 */
public class Binlog2Hive {
    private static final Logger log = LoggerFactory.getLogger(Binlog2Hive.class);
    static {
        //-DHADOOP_USER_NAME=hdfs
        System.setProperty("HADOOP_USER_NAME","hdfs");
    }
    //227527999，binlogFileName文件为mysql-bin.000043
    ///Users/mobin/Downloads/RDS-binlog/mysql-bin.000642
    public static void main(String[] args) throws Exception {
        BinlogClient client = new BinlogClient();
        BinlogOption option = new BinlogOption(args);
        FileSystem fs = FSUtils.getFileSystem();
        registerCloseFSHook(fs);
        try {
            if (option.syncByBinlogFile) {
                client.readDataFromBinlogFile(option.binlogFilePath, fs, option);
            } else {
                client.replicationStream(fs, option);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public static void registerCloseFSHook(FileSystem fs) {
        log.info("注册FileSystem钩子");
        //只有JVM退出时才关闭fs
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    log.info("关闭FS");
                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
