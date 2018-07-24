package com.bixuange;

import com.common.BinlogOption;
import com.common.FSUtils;
import com.config.Config;
import com.config.DatabaseConnection;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.ChecksumType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.NullEventDataDeserializer;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2018/5/17
 * Time: 上午10:08
 */
public class BinlogClient {
    private static final Logger log = LoggerFactory.getLogger(BinlogClient.class);
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private SimpleDateFormat sfDay = new SimpleDateFormat("yyyyMMdd");

    public ConcurrentHashMap<String, List<Serializable[]>> tableRows = new ConcurrentHashMap();  //key是表前缀路径
    private static final String HOSTNAME = "localhost";
    private static final int PORT = 3306;
    private static final String USERNAME = "root";
    private static final String PASSWD = "root";

    public void replicationStream(FileSystem fs, final BinlogOption option) throws IOException, SQLException {
        BinaryLogClient client = new BinaryLogClient(HOSTNAME, PORT, USERNAME, PASSWD);
        if (!option.syncByBinlogFile){
            client.setBinlogFilename(option.binlogFileName);
            client.setBinlogPosition(option.binlogPosition);
        }
        log.info("即将解析{}文件，起始游标为：{}", option.binlogFileName, option.binlogPosition);
        client.registerEventListener(new BinaryLogClient.EventListener() {
            @Override
            public void onEvent(Event event) {
                analysisEvent(event, option, fs, true);
            }
        });
        client.connect();
    }


    public void readDataFromBinlogFile(String filePath, FileSystem fs, final BinlogOption option) throws IOException {
        log.info("正在解析{}文件",filePath);
        File binlogFile = new File(filePath);
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setEventDataDeserializer(EventType.UPDATE_ROWS, new NullEventDataDeserializer());
        eventDeserializer.setEventDataDeserializer(EventType.DELETE_ROWS, new NullEventDataDeserializer());
        eventDeserializer.setChecksumType(ChecksumType.CRC32);
        BinaryLogFileReader reader = new BinaryLogFileReader(binlogFile, eventDeserializer);
        try {
            for (Event event; (event = reader.readEvent()) != null; ) {
                analysisEvent(event, option, fs, false);
            }
        } finally {
            reader.close();
        }
    }

    private void analysisEvent(Event event, BinlogOption option, FileSystem fs, boolean isUpdatePosition) {
        EventType eventType = event.getHeader().getEventType();
        if (eventType == EventType.ROTATE) {
            RotateEventData rotateEventData = event.getData();
            option.binlogFileName = rotateEventData.getBinlogFilename();
        }
        if (eventType == EventType.TABLE_MAP) {  //判断是哪张表
            TableMapEventData tableMapEventData = event.getData();
            String tableName = tableMapEventData.getTable().toLowerCase();
            String database = tableMapEventData.getDatabase().toLowerCase();
            String dirPath = Config.getPath(tableName);
            option.isSyncTable = dirPath == null ? false : true;
            option.database = database;
            option.dirPath = dirPath;
        }
        if (option.isSyncTable && "mobinDB".equals(option.database) && eventType == EventType.WRITE_ROWS ) {
            List<Serializable[]> writeRowsEventDataList = tableRows.get(option.dirPath);
            if (writeRowsEventDataList == null) {
                writeRowsEventDataList = new ArrayList<>();
                tableRows.put(option.dirPath, writeRowsEventDataList);
            }
            WriteRowsEventData writeRowsEventData = event.getData();
            option.count = option.count + writeRowsEventData.getRows().size();
            writeRowsEventDataList.addAll(writeRowsEventData.getRows());
        }
        if (option.count > option.countInterval) {
            ConcurrentHashMap<String, ArrayList<Serializable[]>> map = new ConcurrentHashMap<>();//key是表名+分区目录名(day)，value是一条数据
            try {
                Iterator<String> iterators = tableRows.keySet().iterator();
                //不同表且分区字段不一样打开一次文件流
                while (iterators.hasNext()) {  //遍历次数少,根据表前缀路径来遍历
                    traverseTable(map, iterators, option);   //将数据按天进行分区</DATA/PUBLIC/XXX/day=20180520,row>
                }
                log.info("数据整理完毕，准备写入HDFS");
                Iterator<String> its = map.keySet().iterator();
                while (its.hasNext()) {   //遍历次数少,根据表的分区字段day遍历
                    traverseDay(event, map, its, fs, option, isUpdatePosition);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
            log.info("本次同步{}条数据", option.count);
            option.count = 0;
            tableRows.clear();
        }
    }


    private void traverseDay(Event event, ConcurrentHashMap<String, ArrayList<Serializable[]>> map,
                             Iterator<String> its, FileSystem fs,
                             BinlogOption option, boolean isUpdatePosition) throws Exception {
        OutputStream out;
        String tableName_day = its.next();
        String day = tableName_day.split("=", -1)[1];
        out = FSUtils.openOutputStream(fs, day, tableName_day);
        for (Serializable[] row : map.get(tableName_day)) {
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < row.length; i++) {
                if (i == row.length - 1) {
                    sb.append(row[i]);
                } else if (i == 1) {
                    synchronized (sdf) {
                        sdf.setTimeZone(TimeZone.getTimeZone("GTM"));
                        sb.append(sdf.format(row[1]) + "|");
                    }
                } else {
                    sb.append(row[i] + "|");
                }
            }
            sb.append("\n");
            out.write(sb.toString().getBytes(Charset.forName("UTF-8")));
        }
        out.flush();
        out.close();
        if (isUpdatePosition) {
            EventHeaderV4 eventHeaderV4 = event.getHeader();
            option.binlogPosition = eventHeaderV4.getNextPosition();
            updateBinlogNextPosition(option);
        }

    }

    private void traverseTable(ConcurrentHashMap<String, ArrayList<Serializable[]>> map, Iterator<String> iterators, BinlogOption option) {
        String table_path_prefix = iterators.next();
        String tableName = table_path_prefix.split("/", -1)[4];
        List<Serializable[]> rows = tableRows.get(table_path_prefix);
        for (Serializable[] row : rows) {
            String datetime;
            synchronized (sfDay) {
                sfDay.setTimeZone(TimeZone.getTimeZone("GTM"));
                datetime = sfDay.format(row[1]);
            }
            String key = table_path_prefix + "/day=" + datetime;
            ArrayList<Serializable[]> rowList = map.get(key);
            if (rowList == null) {
                rowList = new ArrayList<>();
                map.put(key, rowList);
            }
            rowList.add(row);
        }
    }

    public static void updateBinlogNextPosition(BinlogOption option) {
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = DatabaseConnection.getMysqlConnection();
            String SQL = "UPDATE t_position SET nextposition=?,binlogfilename=?";
            ps = conn.prepareStatement(SQL);
            ps.setLong(1, option.binlogPosition);
            ps.setString(2, option.binlogFileName);
            ps.executeUpdate();
            ps.close();
            log.info("更新游标为：{}，binlogFileName文件为{}", option.binlogPosition, option.binlogFileName);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
