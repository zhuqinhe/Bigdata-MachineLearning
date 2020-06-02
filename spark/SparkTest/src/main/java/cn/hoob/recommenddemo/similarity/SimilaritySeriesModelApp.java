package cn.hoob.recommenddemo.similarity;

import lombok.val;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.spark_project.jetty.util.StringUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/*****
 * 抽取db里面的数据，构造内容相似向量，存于hivi，以备待用
 * ********/
public class SimilaritySeriesModelApp {
    public static <Dateset> void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("SimilarityModelApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();
        //加载db里面的全量合集
        Properties properties = new Properties();
        properties.put("user", "appuser");
        properties.put("password", "Mysql123+");
        String url = "jdbc:mysql://127.0.0.1:3306/bigdata";
        Dataset<Row> seriesData = sparkSession.read().jdbc(url, "series_id_contentid", properties);
        //seriesData.show();

        //合集过滤不满足条件的，转换成
        JavaRDD<TrainningData> trainningDataJavaRDD = seriesData.javaRDD().map(row -> {
            String kind = (String) row.getAs("genre");
            if (StringUtil.isBlank(kind)) {
                System.out.println("some field is null " + row.toString());
                return null;
            }
            TrainningData data = new TrainningData();
            data.setId(Long.parseLong(row.getAs("id") + ""));
            data.setContentId(row.getAs("contentid"));
            data.setKind(kind);
            return data;
        }).filter((data) -> data != null);
        //过滤包装后的数据再装成Dataset
        Dataset<Row> seriesDataFilter = sparkSession.createDataFrame(trainningDataJavaRDD, TrainningData.class);
        //合集元数据存于hdfs 和 hive
        //数据存到hdfs 和 hive  中以备待用
        seriesDataFilter.write().mode(SaveMode.Overwrite).parquet("hdfs://node1:9000/recommend_processed_/trainningSeriesDataset");
        sparkSession.sql("drop table if exists series_data");
        sparkSession.sql("create table if not exists series_data(id bigint,contentId string,kind string) stored as parquet");
        sparkSession.sql("load data inpath 'hdfs://node1:9000/recommend_processed_/trainningSeriesDataset' overwrite into table series_data");
        //sparkSession.sql("select * from series_data").show();
        //分词拆分后的Dataset
        Dataset<Row> trainningTfidfDataset = TfidfUtil.tfidf(seriesDataFilter);

        JavaRDD<TrainningDataFeature> trainningFeatureRdd = trainningTfidfDataset.javaRDD().map(row -> {
            TrainningDataFeature data = new TrainningDataFeature();
            data.setId(Long.parseLong(row.getAs("id") + ""));
            data.setContentId(row.getAs("contentId"));
            data.setKind(row.getAs("kind"));
            SparseVector vector = row.getAs("features");
            data.setVectorSize(vector.size());
            data.setVectorIndices(Arrays.toString(vector.indices()));
            data.setVectorValues(Arrays.toString(vector.values()));
            return data;
        });
        Dataset<Row> trainningFeaturedataset = sparkSession.createDataFrame(trainningFeatureRdd,TrainningDataFeature.class);
        //trainningFeaturedataset.show();

        //数据存到hdfs 和 hive  中以备待用
        trainningFeaturedataset.write().mode(SaveMode.Overwrite).parquet("hdfs://node1:9000/recommend_processed_/trainningTfidfDataset");
        sparkSession.sql("drop table if exists series_pre_data");
        sparkSession.sql("create table if not exists series_pre_data(id bigint,contentId string,vectorSize int,vectorIndices string,vectorValues string) stored as parquet");
        sparkSession.sql("load data inpath 'hdfs://node1:9000/recommend_processed_/trainningTfidfDataset' overwrite into table series_pre_data");
       // sparkSession.sql("select * from series_pre_data").show();


    }
}
