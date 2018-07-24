package com.bixuange;

import com.common.BinlogOption;
import com.common.FSUtils;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.ChecksumType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.NullEventDataDeserializer;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2018/5/17
 * Time: 上午11:16
 */
public class BinlogClientTest {
    @Test
    public void binlogClinet() throws IOException {
        BinlogClient client = new BinlogClient();
        FileSystem fs = null;
        try {
            fs = FSUtils.getFileSystem();
            BinlogOption option = new BinlogOption(null);
            client.replicationStream(fs,option);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            fs.close();
        }

    }


    @Test
    public void readBinlog() throws IOException {
        File binlogFile = new File("/Users/mobin/Downloads/RDS-binlog/mysql-bin.000043");
        EventDeserializer eventDeserializer = new EventDeserializer();
//        eventDeserializer.setEventDataDeserializer(EventType.WRITE_ROWS, new ByteArrayEventDataDeserializer());
        eventDeserializer.setEventDataDeserializer(EventType.UPDATE_ROWS, new NullEventDataDeserializer());
        eventDeserializer.setChecksumType(ChecksumType.CRC32);

        BinaryLogFileReader reader = new BinaryLogFileReader(binlogFile,eventDeserializer);
        try {
            for (Event event; (event = reader.readEvent()) != null; ) {
                // && !EventHeaderV4.class.isInstance(event.getHeader())
                EventType eventType = event.getHeader().getEventType();//                TableMapEventData tableMapEventData = event.getData();
                if (eventType == EventType.TABLE_MAP) {  //判断是哪张表
                    TableMapEventData tableMapEventData = event.getData();
                    System.out.println(tableMapEventData.getTable());
                }
//
//                if (event.getHeader().getEventType() == EventType.WRITE_ROWS){
//                    WriteRowsEventData writeRowsEventData = new WriteRowsEventData();
//                    writeRowsEventData.setRows(event.getData());
////                    ByteArrayEventData byteArrayEventData = event.getData();
////                    String s = new String(byteArrayEventData.getData());
////                    System.out.println(s);
//                }
            }
        } finally {
            reader.close();
        }
    }
}
