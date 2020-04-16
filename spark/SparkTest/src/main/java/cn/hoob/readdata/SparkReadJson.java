package cn.hoob.readdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Spark 读取 CVS数据
 */
public class SparkReadJson {
    public static void main(String[] args) throws Exception {

        //创建SparkSession
        SparkSession sparkSession= SparkSession.builder().appName("SparkReadCVS").master("local[*]").getOrCreate();
        Dataset<Row> jsondataset= sparkSession.read().
                format("json")
                .load("D:\\ProgramFiles\\BigData\\data\\input\\rating.json");//excel文件路径 + 文件名

        //执行Action
        jsondataset.show();

        sparkSession.stop();

    }
}
