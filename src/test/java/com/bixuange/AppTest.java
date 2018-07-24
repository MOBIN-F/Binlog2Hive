package com.bixuange;

import static org.junit.Assert.assertTrue;

import com.common.ExecCMD;
import com.common.FSUtils;
import com.config.DatabaseConnection;
import com.main.Binlog2Hive;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics.spi.OutputRecord;
import org.junit.Test;

import javax.swing.text.html.parser.Entity;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Semaphore;

/**
 * Unit test for simple App.
 */
public class AppTest {
    @Test
    public void shouldAnswerWithTrue() throws IOException, SQLException {
//        System.out.println(FSUtils.getFileSystem());
        System.out.println(DatabaseConnection.getMysqlConnection());
    }

    @Test
    public void append() throws IOException {
        OutputStream out = null;
        FileSystem fs = null;
        try {
            fs = FSUtils.getNewFileSystem();
            final Semaphore semaphore = new Semaphore(2);
            for (int i=0; i<=3 ; i ++){
                out = FSUtils.openOutputStream(null,"","");
                for (int j = 1000; j < 2000; j++) {
                    out.write(("aaaaaaaaaaaaaaaaa" + j + "\n").getBytes());
                }
                out.flush();
                out.close();
                out.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            out.close();
            fs.close();
        }
    }

    @Test
    public void split(){
        String str = "/DATA/PUBLIC/TABLE1/day=20180514/000000_0";
        String str1 = "T_ALLAWSMIND_20180518";
        String tableName = str1.split("_",-1)[2];
        System.out.println(tableName);

        System.out.println(new java.util.Date());

        String fileName = "/DATA/PUBLIC/TABLE1/day=20180522/000000_0";
        System.out.println(fileName.split("_",-1)[0]);
        System.out.println(Integer.parseInt(fileName.split("_",-1)[2]));
    }

    @Test
    public void process() throws IOException {
        Process proc=Runtime.getRuntime().exec("/opt/hive-1.2.2/bin/hive -e 'create table yy(id int)'");
//        String SQL = "hive -e "  + "USE met_office;ALTER TABLE T_HNAWSMIND  add IF NOT EXISTS partition (day=20180519) location '/DATA/PUBLIC/METOFFICE/T_HNAWSMIND/day=20180519/'";
//        String SQL = "open .";
//        System.out.println(SQL);
//        ExecCMD.exec(SQL);
    }

    @Test
    public void getNextPosition() throws SQLException {
//        System.out.println(Binlog2Hive.getNextPosition(null));
        HashMap<String,Integer> map = new HashMap<>();
        map.put("2",1);
        map.put("1",4);
        map.put("3",2);
        Set<Map.Entry<String,Integer>> set = map.entrySet();
        List<Map.Entry<String,Integer>> list = new ArrayList<>(set);
        Collections.sort(list, new Comparator<Map.Entry<String,Integer>>() {
            @Override
            public int compare(Map.Entry<String,Integer> o1, Map.Entry<String,Integer> o2) {
                return o1.getValue() - o2.getValue();
            }
        });

        LinkedHashMap<String,Integer> linkedHashMap = new LinkedHashMap<>();
        for (Map.Entry<String,Integer> entry : list){
            linkedHashMap.put(entry.getKey(),entry.getValue());
        }

        System.out.println(linkedHashMap);
    }
}
