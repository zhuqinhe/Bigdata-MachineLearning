package cn.hoob.recommenddemo.similarity;

import com.alibaba.fastjson.JSON;
import com.mysql.cj.xdevapi.JsonParser;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.linalg.BLAS;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.*;
import org.spark_project.jetty.util.StringUtil;
import scala.reflect.ClassTag$;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/*****
 * 计算每个合集的相对相似度
 *
 * ********/
public class SimilaritySeriesRecommendApp {
    public static final String TRAINNING_DATA_SQL = "select id,contentId,vectorSize, coalesce(vectorIndices,'[]') as vectorIndices, coalesce(vectorValues,'[]') as vectorValues" +
            " from series_pre_data ";

    public static final String COMPETITOR_DATA_SQL = "select id,contentId,coalesce(kind,'') as kind from series_data " +
            " where kind is not null ";

    public static <Dateset> void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("SimilarityModelApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();
        String trainningSql = String.format(TRAINNING_DATA_SQL);
        //加载预数据，处理成可计算的模式
        Dataset<Row> rowRdd = sparkSession.sql(trainningSql);
        List<MiniTrainningData> trainningDataList = rowRdd.javaRDD().map((row) -> {
            MiniTrainningData data = new MiniTrainningData();
            data.setId(row.getAs("id"));
            data.setContentId( row.getAs("contentId"));
            Integer vectorSize = row.getAs("vectorSize");
            List<Integer> vectorIndices = JSON.parseArray(row.getAs("vectorIndices"),Integer.class);
            List<Double> vectorValues = JSON.parseArray(row.getAs("vectorValues"),Double.class);
            SparseVector vector = new SparseVector(vectorSize.intValue(),integerListToArray(vectorIndices),doubleListToArray(vectorValues));
            data.setFeatures(vector);
            return data;
        }).collect();
        MiniTrainningData[] miniTrainningDataArray = new MiniTrainningData[trainningDataList.size()];
        trainningDataList.toArray(miniTrainningDataArray);
        //把加载的与数据广播出去，海量数据集时。。。
        final Broadcast<MiniTrainningData[]> trainningData = sparkSession.sparkContext().broadcast(miniTrainningDataArray, ClassTag$.MODULE$.<MiniTrainningData[]>apply(MiniTrainningData[].class));

        String  DATA_SQL= String.format(COMPETITOR_DATA_SQL);

        //数据已经格式化好了
        Dataset<Row> seriesData = sparkSession.sql(DATA_SQL);
        Dataset<Row> seriesTfidDataset = TfidfUtil.tfidf(seriesData);
        //求解合集之间的相似度
        Dataset<SimilartyData> similartyDataList = pickupTheTopSimilarShop(seriesTfidDataset, trainningData);
        similartyDataList.show();

    }
    private static Dataset<SimilartyData> pickupTheTopSimilarShop(Dataset<Row> meituanTfidDataset, Broadcast<MiniTrainningData[]> trainningData){
        return meituanTfidDataset.map(new MapFunction<Row, SimilartyData>() {
            @Override
            public SimilartyData call(Row row) throws Exception {
                SimilartyData similartyData = new SimilartyData();
                Long id = row.getAs("id");
                Vector features = row.getAs("features");
                similartyData.setId(id);
                MiniTrainningData[] trainDataArray = trainningData.value();
                if(ArrayUtils.isEmpty(trainDataArray)){
                    return similartyData;
                }
                double maxSimilarty = 0;
                long maxSimilareleShopId = 0;
                for (MiniTrainningData trainData : trainDataArray) {
                    Vector trainningFeatures = trainData.getFeatures();
                    long eleShopId = trainData.getId();
                    double dot = BLAS.dot(features.toSparse(), trainningFeatures.toSparse());
                    double v1 = Vectors.norm(features.toSparse(), 2.0);
                    double v2 = Vectors.norm(trainningFeatures.toSparse(), 2.0);
                    double similarty = dot / (v1 * v2);
                    if(similarty>maxSimilarty){
                        maxSimilarty = similarty;
                        maxSimilareleShopId = eleShopId;
                    }
                }
                similartyData.setEleId(maxSimilareleShopId);
                similartyData.setSimilarty(maxSimilarty);
                return similartyData;
            }
        }, Encoders.bean(SimilartyData.class));
    }
    private static int[] integerListToArray(List<Integer> integerList){
        int[] intArray = new int[integerList.size()];
        for (int i = 0; i < integerList.size(); i++) {
            intArray[i] = integerList.get(i).intValue();
        }
        return intArray;
    }

    private static double[] doubleListToArray(List<Double> doubleList){
        double[] doubleArray = new double[doubleList.size()];
        for (int i = 0; i < doubleList.size(); i++) {
            doubleArray[i] = doubleList.get(i).intValue();
        }
        return doubleArray;
    }
}
