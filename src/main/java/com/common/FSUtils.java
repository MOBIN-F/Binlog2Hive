package com.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class FSUtils {
    private static final Logger log = LoggerFactory.getLogger(FSUtils.class);

    public static final int BLOCK_SIZE = 240;
    public static FileSystem getFileSystem() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        return FileSystem.get(conf);
    }

    public static FileSystem getNewFileSystem() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        return FileSystem.newInstance(conf);
    }

//    public static OutputStream createLzoOutputStream(FileSystem fs, Path lzoFile) throws IOException {
//        OutputStream os = fs.create(lzoFile);
//        return createLzoOutputStream(fs, os);
//    }
//
//    public static OutputStream createLzoOutputStream(FileSystem fs, OutputStream os) throws IOException {
//
////        OutputStream os = fs.create(lzoFile);
//        return createLzoOutputStream(fs, os);
//    }

    public static OutputStream openOutputStream(FileSystem fs, String day, String tableNameDayPath) throws Exception {
        OutputStream os = null;
        Path path = getLastModifiedFile(fs,new Path(tableNameDayPath));  //获取最近写入的那个文件
        if (fs.exists(path) && fs.getFileStatus(path).getLen()/(1<<20) <= BLOCK_SIZE) {
            log.info("append内容到{}",path);
            try {
                os = fs.append(path);
                log.info(path+"文件append完成");
            } catch (Exception e) {
                log.error("文件append失败：" + e);
                //在不支持append的情况下，先将数据读出来，再写回去
                byte[] oldBytes = FSUtils.readDataFile(fs, path);  //追加的文件内容
                os = fs.create(path);  //被追加文件内容
                os.write(oldBytes);
            }
        } else if (!fs.exists(path)) {
            log.info("创建{}文件", path); //DATA/PUBLIC/TABLE1/day=20180522/000000_0
            os = fs.create(path);
            log.info("开始对{}进行分区", tableNameDayPath);
            addHivePartition(path.toString(), day, tableNameDayPath);
        } else{
            int fileName_suffix = Integer.parseInt(path.getName().split("_",-1)[1]) + 1;
            String newFilePath = tableNameDayPath + "/" + "000000_" + fileName_suffix;
            log.info("创建{}文件",newFilePath);
            os = fs.create(new Path(newFilePath));
        }
        return os;
    }

    public static Path getLastModifiedFile(FileSystem fs,Path path) throws IOException {
        if (!fs.exists(path) || !fs.exists(new Path(path.toString() + "/000000_0"))){
            return new Path(path.toString() + "/000000_0");
        }
        Long lastModifiedTime = Long.MIN_VALUE;
        Path filePath =  null;
        for (FileStatus fileStatus: fs.listStatus(path)){
            Long lastTime = fileStatus.getModificationTime();
            if (lastTime > lastModifiedTime){
                lastModifiedTime = lastTime;
                filePath = fileStatus.getPath();
            }
        }
        return filePath;
    }


    public static void addHivePartition(String path, String day,String tableName_day) {
        StringBuffer sb = new StringBuffer();
        String tableName = path.split("/", -1)[4];
        String location = tableName_day +"/";
        sb.append("USE mobinDB;");
        sb.append("ALTER TABLE ")
                .append(tableName)
                .append("  add IF NOT EXISTS partition (");
        sb.append("day=").append(day).append(")");
        sb.append(" location '").append(location).append("'");
        String HiveSQL = String.format("hive -e \"%s\"", sb.toString());
        System.out.println(HiveSQL);
        log.info(HiveSQL);
        ExecCMD.exec(HiveSQL);
    }

    public static byte[] readDataFile(FileSystem fs, Path dataFile) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(2 * 1024 * 1024);
        InputStream in = null;
        try {
            in = fs.open(dataFile);
            IOUtils.copyBytes(in, bos, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
        return bos.toByteArray();
    }


}
