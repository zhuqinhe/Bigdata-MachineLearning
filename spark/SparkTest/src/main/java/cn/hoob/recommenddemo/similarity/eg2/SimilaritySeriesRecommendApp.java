package cn.hoob.recommenddemo.similarity.eg2;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.linalg.BLAS;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.*;
import scala.reflect.ClassTag$;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/*****
 * 计算每个合集的相对相似度
 * 基于IF-IDF 分词构成的向量，计算两两合集的相似度
 * ********/
public class SimilaritySeriesRecommendApp {
    public static final String TRAINNING_DATA_SQL = "select id,contentId,vectorSize, coalesce(vectorIndices,'[]') as vectorIndices, coalesce(vectorValues,'[]') as vectorValues" +
            " from series_pre_data ";

    public static final String COMPETITOR_DATA_SQL = "select id,contentId,coalesce(kind,'') as kind from series_data " +
            " where kind is not null ";

    public static  void main(String[] args) throws IOException {
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
        Dataset<SimilartyDatas> similartyDataList = pickupTheTopSimilarShop(seriesTfidDataset, trainningData);
        //这个我不知道为啥会报row.getSimilartyDatas（）方法无效
        //Dataset<List<SimilartyData>>similartyDatas=similartyDataList.map(row->{return row.getSimilartyDatas()},Encoders.bean(List.class));
        JavaRDD<SimilartyData>similartyDataListRdd=similartyDataList.toJavaRDD().map(row->row.getSimilartyDatas()).flatMap(row->row.iterator());

       // Dataset<Row> similartys=sparkSession.createDataFrame(similartyDataListRdd,SimilartyData.class);
        Dataset<SimilartyData> similartys=sparkSession.createDataset(similartyDataListRdd.rdd(),Encoders.bean(SimilartyData.class));
        similartys.createOrReplaceTempView("similartys");
        Dataset similartysFiter=  sparkSession.sql("" +
             //   "select a.id,a.contentId,a.eleId,a.eleContentId,a.similarty " +
              //  "from similartys a " +
              //  "left join  similartys b on a.id=b.id and a.similarty>b.similarty " +
              //  "where a.id!=a.eleId and b.id!=b.eleId  group by a.id,a.contentId   having count(a.id)<100 "
        "select * from (select id,contentId,eleId,eleContentId,similarty, row_number() over(partition by id order by similarty desc) as rn from similartys where id!=eleId  ) tmp where rn<100");

        //similartysFiter.show();
        //把数据写入mysql
        similartysFiter.write().format("jdbc")
                .mode(SaveMode.Overwrite)
                .option("url", "jdbc:mysql://localhost:3306/bigdata")
                .option("dbtable", "series_similarty")
                .option("user", "appuser")
                .option("password", "Mysql123+")
                .save();



    }
    private static Dataset<SimilartyDatas> pickupTheTopSimilarShop(Dataset<Row> TfidDataset, Broadcast<MiniTrainningData[]> trainningData){
        return TfidDataset.map(new MapFunction<Row, SimilartyDatas>() {
            @Override
            public SimilartyDatas call(Row row) throws Exception {
                SimilartyDatas similartyDatass=new SimilartyDatas();
                List<SimilartyData> similartyDatas = new ArrayList<SimilartyData>();
                Long id = row.getAs("id");
                String  contentId=row.getAs("contentId");
                Vector features = row.getAs("features");

                MiniTrainningData[] trainDataArray = trainningData.value();
                if(ArrayUtils.isEmpty(trainDataArray)){
                    return similartyDatass;
                }
              //  double maxSimilarty = 0;
               // long maxSimilareleShopId = 0;
                for (MiniTrainningData trainData : trainDataArray) {
                    SimilartyData similartyData=new SimilartyData();
                    Vector trainningFeatures = trainData.getFeatures();
                    long eleShopId = trainData.getId();
                    double dot = BLAS.dot(features.toSparse(), trainningFeatures.toSparse());
                    double v1 = Vectors.norm(features.toSparse(), 2.0);
                    double v2 = Vectors.norm(trainningFeatures.toSparse(), 2.0);
                    double similarty = dot / (v1 * v2);
                    //这里这样时计算最相似的一个，不需要先保留每一个的计算只后面才过滤
                    //if(similarty>maxSimilarty){
                     //   maxSimilarty = similarty;
                     //   maxSimilareleShopId = eleShopId;
                    //}
                    similartyData.setId(id);
                    similartyData.setContentId(contentId);
                    similartyData.setEleContentId(trainData.getContentId());
                    similartyData.setEleId(eleShopId);
                    similartyData.setSimilarty(similarty);
                    if(similartyDatas.size()<200){
                        similartyDatas.add(similartyData);
                    }else{
                        if(similartyDatas.get(199).getSimilarty()<similarty){
                            similartyDatas.remove(199);
                            similartyDatas.add(similartyData);
                        }
                    }

                }
                similartyDatass.setId(id);
                similartyDatass.setSimilartyDatas(similartyDatas);
                return similartyDatass;
            }
        }, Encoders.bean(SimilartyDatas.class));
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
