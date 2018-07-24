package com.bixuange;

import com.common.FSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import sun.rmi.runtime.Log;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Semaphore;

public class FileSystemTest {

    public static void main(String[] args) throws Exception {
//        System.setProperty("HADOOP_USER_NAME", "hdfs");
        FileSystem fs = null;
        try {
            //1527386243433
            fs = FSUtils.getNewFileSystem();
            System.out.println(fs.exists(new Path("/DATA/PUBLIC/TABLE1/day=20180519/")));
            Long lastModeifTime = Long.MIN_VALUE;
            Path path = null;
            for (FileStatus fileStatus: fs.listStatus(new Path("/DATA/PUBLIC/TABLE1/day=20180519/"))){
                Long time = fileStatus.getModificationTime();
                if (time > lastModeifTime){
                    lastModeifTime = time;
                    path = fileStatus.getPath();
                }
            }

            System.out.println(path.toString());
            System.out.println(fs.getFileStatus(new Path("/DATA/PUBLIC/TABLE1/day=20180519/000000_0")).getLen());

//            System.out.println(fs.getFileStatus(new Path("/mobin.txt")).getModificationTime());
//            OutputStream out = FSUtils.openOutputStream(null, "/test/2.txt","","");
//            out.write(("test。。。。。" + "\n").getBytes());
//            out.flush();
        } finally {
//            fs.close();
        }
    }

    static class MyThread extends Thread {
        OutputStream out;
        Semaphore semaphore;

        public MyThread() {
            super("MyThread");
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < 100; i++) {
                    try {
                        synchronized (out) {
                            out.write((Thread.currentThread().getName() + "aaaaaaaaaaaaaaaaa" + i + "\n").getBytes());
                        }
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
                try {
                    synchronized (out) {
                        out.flush();
                    }
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } finally {
                semaphore.release();
            }
        }
    }
}
