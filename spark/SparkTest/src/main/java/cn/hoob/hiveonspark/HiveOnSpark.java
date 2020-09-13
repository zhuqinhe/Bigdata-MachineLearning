package cn.hoob.hiveonspark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * spark SQL 分析访问日子
 */
public class HiveOnSpark {
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession= SparkSession.builder().appName("HiveOnSpark").master("local[*]").enableHiveSupport().getOrCreate();
        Dataset  hivedataSet= sparkSession.sql("select * from t_access");
        hivedataSet.show();
        sparkSession.sql("drop table hoob_t_a");
        sparkSession.sql("create table hoob_t_a(name string,numb int) row format delimited fields terminated by ','");
        sparkSession.sql("load data   inpath '/a.txt' into table hoob_t_a ");
        sparkSession.sql("select * from hoob_t_a").show();
        sparkSession.stop();
    }
}
