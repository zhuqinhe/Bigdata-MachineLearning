package cn.hoob.readdata;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.HashMap;

/**
 * Spark 读取 CVS数据
 */
public class SparkReadExel {
    public static void main(String[] args) throws Exception {

        //创建SparkSession
        SparkSession sparkSession= SparkSession.builder().appName("SparkReadCVS").master("local[*]").getOrCreate();

        //(指定以后从哪里)读数据，是lazy
        //Dataset分布式数据集，是对RDD的进一步封装，是更加智能的RDD
        //dataset只有一列，默认这列叫value
        Dataset<Row> cvsdataset= sparkSession.read().
                format("com.crealytics.spark.excel")
                .option("useHeader", "true") // 是否将第一行作为表头
                .option("inferSchema", "false") // 是否推断schema
                .option("workbookPassword", "None") // excel文件的打开密码
                .load("D:\\ProgramFiles\\BigData\\data\\cvs\\test.xlsx");//excel文件路径 + 文件名





        cvsdataset.printSchema();

        //执行Action
        cvsdataset.show();

        sparkSession.stop();

    }
}
