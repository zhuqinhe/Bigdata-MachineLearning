package cn.hoob.readdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Spark 读取 CVS数据
 */
public class SparkReadText {
    public static void main(String[] args) throws Exception {

        //创建SparkSession
        SparkSession sparkSession= SparkSession.builder().appName("SparkReadCVS").master("local[*]").getOrCreate();
        Dataset<Row> textdataset= sparkSession.read().
                text("D:\\ProgramFiles\\BigData\\data\\input\\files");

        //执行Action
        textdataset.show();
        textdataset.write().format("text").save("D:\\ProgramFiles\\BigData\\data\\text");
        sparkSession.stop();

    }
}
