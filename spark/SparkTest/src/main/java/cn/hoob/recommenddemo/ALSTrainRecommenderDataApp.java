package cn.hoob.recommenddemo;

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.SQLException;
import java.util.Iterator;

/***
 * 根据ALS.train() 方法训练的模型 为所有用户推荐数据
 * ***/
public class ALSTrainRecommenderDataApp {
    public static void main(String[] args) throws SQLException {

        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("ALSTrainRecommenderDataApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        Dataset<Row> useridDateset = sparkSession.sql("select distinct(userId) from logs_training");
        Iterator contentIds = useridDateset.toJavaRDD().map(line -> line.get(0)).toLocalIterator();

        //可以设置成参数
        String moelpath = "hdfs://node1:9000/recommend_processed_bestModel/0.0";
        MatrixFactorizationModel model = MatrixFactorizationModel.load(sparkSession.sparkContext(), moelpath);
        while (contentIds.hasNext()) {
            int userId = Integer.parseInt(contentIds.next() + "");
            Rating[] datas = model.recommendProducts(userId, 30);
            MySQLUtlis.processRecommendData(datas);
        }

    }
}
