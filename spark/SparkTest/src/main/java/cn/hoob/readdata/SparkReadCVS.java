package cn.hoob.readdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Spark 读取 CVS数据
 */
public class SparkReadCVS {
    public static void main(String[] args) throws Exception {

        //创建SparkSession
        SparkSession sparkSession= SparkSession.builder().appName("SparkReadCVS").master("local[*]").getOrCreate();

        //(指定以后从哪里)读数据，是lazy
        //Dataset分布式数据集，是对RDD的进一步封装，是更加智能的RDD
        //dataset只有一列，默认这列叫value
        Dataset<Row> cvsdataset= sparkSession.read()
                .format("com.databricks.spark.csv")
                .option("delimiter", ",") // 字段分割符
                .option("header", "true") // 是否将第一行作为表头header
                .option("inferSchema", "false") //是否自动推段内容的类型
                .option("codec", "none") // 压缩类型
                .load("D:\\ProgramFiles\\BigData\\data\\cvs\\电子发票登记---Hoob(2020-04).cvs.csv");//excel文件路径 + 文件名
        cvsdataset.printSchema();
      //或则
        cvsdataset= sparkSession.read().csv("D:\\ProgramFiles\\BigData\\data\\cvs");//必须是给个目录，不能是文件



        cvsdataset.printSchema();

        //执行Action
        cvsdataset.show();

        sparkSession.stop();

    }
}
