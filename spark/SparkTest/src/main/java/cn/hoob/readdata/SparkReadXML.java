package cn.hoob.readdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Spark 读取 CVS数据
 */
public class SparkReadXML {
    public static void main(String[] args) throws Exception {

        //创建SparkSession
        SparkSession sparkSession= SparkSession.builder().appName("SparkReadXML").master("local[*]").getOrCreate();

        //(指定以后从哪里)读数据，是lazy
        //Dataset分布式数据集，是对RDD的进一步封装，是更加智能的RDD
        //dataset只有一列，默认这列叫value
        Dataset<Row> cvsdataset= sparkSession.read()
                .format("com.databricks.spark.xml")
                .option("rowTag", "wsdl:definitions") // xml文件rowTag，分行标识,"testXml"即为上文rowTag
                .load("D:\\ProgramFiles\\BigData\\data\\XML\\c2.xml");//excel文件路径 + 文件名





        cvsdataset.printSchema();

        //执行Action
        cvsdataset.show();

        sparkSession.stop();

    }
}
