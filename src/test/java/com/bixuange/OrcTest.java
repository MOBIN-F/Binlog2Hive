//package com.bixuange;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
//import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
//import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
//import org.apache.orc.OrcFile;
//import org.apache.orc.Writer;
//import org.junit.Test;
//import org.apache.orc.TypeDescription;
//
//import java.io.IOException;
//import java.nio.charset.Charset;
//
//
///**
// * Created with IDEA
// * Creater: MOBIN
// * Date: 2018/5/25
// * Time: 上午10:11
// */
//public class OrcTest {
//    @Test
//    public void orcTest() throws IOException {
//        Configuration conf = new Configuration();
//        Path path = new Path("/mobin1.orc");
//        //typeName不能有空格
//        TypeDescription schema = TypeDescription.fromString("struct<id:int,name:string>");
//        Writer writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf).setSchema(schema));
//        VectorizedRowBatch batch = schema.createRowBatch();
//        LongColumnVector id = (LongColumnVector) batch.cols[0];
//        BytesColumnVector name = (BytesColumnVector) batch.cols[1];
//        final int BATCH_SIZE = batch.getMaxSize();
//        System.out.println(BATCH_SIZE);
//        for (int i = 0; i < 10; i ++){
//            int row = batch.size ++;
//            id.vector[row] = i;
//            byte[] buffer = ("name" + i).getBytes(Charset.forName("UTF-8"));
//            name.setRef(row, buffer, 0, buffer.length);
//            if (row == BATCH_SIZE - 1){
//                writer.addRowBatch(batch);
//                batch.reset();
//            }
//        }
//        if (batch.size != 0){
//            writer.addRowBatch(batch);
//            batch.reset();
//        }
//        writer.close();
//    }
//}
