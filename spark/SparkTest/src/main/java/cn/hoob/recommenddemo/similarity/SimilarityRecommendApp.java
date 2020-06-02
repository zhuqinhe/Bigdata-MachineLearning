package cn.hoob.recommenddemo.similarity;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;

import java.io.IOException;

/***
 *  和用户相关
 * 基于物品的协同过滤又称Item-Based CF
   基于物品的协同过滤,这里的是余弦相似度
 **/
public class SimilarityRecommendApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("SimilarityModelApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();
        //模型训练数据
        Dataset<Row> traningDateSet = sparkSession.sql("select userId ,seriesId ,count from logs_all");
        Dataset<Row>[] splits = traningDateSet.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> training = splits[0];
        Dataset<Row> test = splits[1];
        RDD<MatrixEntry>sim=standardCosine(parseToMatrix(training));

        JavaRDD<SimilarityDataModel>tt=sim.toJavaRDD().map(line -> {
                    return new SimilarityDataModel(line.i(),line.j(),line.value());
                });
        //Encoder<MatrixEntry> MatrixEntryEncoder = Encoders.bean(MatrixEntry.class);
        //Dataset<MatrixEntry> simDataset=sparkSession.createDataset(sim, MatrixEntryEncoder);
        Dataset<Row> simDataset=sparkSession.createDataFrame(tt, SimilarityDataModel.class);
        simDataset.createOrReplaceTempView("itemSim");
        training.createOrReplaceTempView("training");
        test.createOrReplaceTempView("test");
        Dataset<Row> testItemSim = sparkSession.sql(
                "select test.userId ,test.seriesId ,test.count testRating,training.count rating,sim.count sim from test " +
                        "left join training on test.userId=training.userId " +
                        "left join itemSim sim on test.seriesId=sim.sidX and training.seriesId=sim.sidY");
        testItemSim.cache();
        testItemSim.createOrReplaceTempView("testAndSim");

        //预测评分
        String  sqlRank = "select userId,seriesId,testRating,rating,sim," +
                "rank() over (partition by userId,seriesId order by sim desc) rank from testAndSim";
        Dataset<Row> testAndPre = sparkSession.sql(
                "select userId,seriesId,first(testRating) rate,nvl(sum(rating*sim)/sum(abs(sim)),0) pre from( " +
                        "  select *" +
                        "  from  (" + sqlRank + ") t " +
                        " where rank <= 5 " +
                        ") w " +
                        "group by userId,seriesId");
        testAndPre.show();

    }
    //相似度矩阵,评分数据转换成矩阵
    public static CoordinateMatrix parseToMatrix(Dataset<Row> data){
        JavaRDD<MatrixEntry> parsedData = data.toJavaRDD().map(row -> new MatrixEntry(row.getInt(0),
                row.getInt(1), row.getInt(2)));
       return  new CoordinateMatrix(parsedData.rdd());
    }
    //计算相似度矩阵
    public static RDD<MatrixEntry> standardCosine(CoordinateMatrix matrix){
        CoordinateMatrix similarity = matrix.toIndexedRowMatrix().columnSimilarities();
        return similarity.entries();
    }

}
