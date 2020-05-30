package recommenddemo;

import lombok.val;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import shapeless.Tuple;

import java.util.Arrays;
import java.util.List;
/*******
 *
 *使用默认的ALS.train() 方法，即显性反馈（默认implicitPrefs 为false）来构建推荐模型并根据模型对评分预测的均方根误差来对模型进行评估
 * **********/
public class ALSTrainModelApp {
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("ALSTrainModelApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();
        //模型训练数据
        Dataset<Row> traningDateSet=sparkSession.sql("select * from logs_training");
        //模型测试数据
        Dataset<Row>  testDateSet=sparkSession.sql("select * from logs_test");
       // 训练集，转为Rating格式
        JavaRDD<Rating> traningRdd=traningDateSet.toJavaRDD().map(row->{
           return new Rating( row.getInt(0),row.getInt(1),row.getInt(2));
        });
        //预测数据格式封装
        JavaPairRDD<Integer, Integer> traningRdd2=traningRdd.mapToPair(line->new Tuple2<>(line.user(),line.product()));

        //测试集，转为Rating格式
        JavaRDD<Rating> testJavaRdd=testDateSet.toJavaRDD().map(row->{
            return new Rating( row.getInt(0),row.getInt(1),row.getInt(2));
        });
        //测试预测数据
        JavaPairRDD<Tuple2<Integer,Integer>,Double>testJavaRdd2=testJavaRdd.mapToPair(line->{
            return new Tuple2<Tuple2<Integer,Integer>,Double>(new Tuple2(line.user(),line.product()), line.rating());
        });
       // 特征向量的个数
        int rank = 1;
        // 正则因子
        List<Double> lambda = Arrays.asList(0.001, 0.005, 0.01, 0.015);
        // 迭代次数
        List<Integer> iteration = Arrays.asList(10, 15, 18);
        Double bestRMSE = Double.MAX_VALUE;
        int bestIteration = 0;
        double bestLambda = 0.0;
        // persist可以根据情况设置其缓存级别
        // 持久化放入内存，迭代中使用到的RDD都可以持久化
        //模型训练预测数据
        traningRdd.persist(StorageLevel.MEMORY_AND_DISK());
        traningRdd2.persist(StorageLevel.MEMORY_AND_DISK());
        //测试数据
        testJavaRdd.persist(StorageLevel.MEMORY_AND_DISK());
        testJavaRdd2.persist(StorageLevel.MEMORY_AND_DISK());

            for(Double l:lambda){
                for(Integer i:iteration){
                    // 循环收敛这个模型
                    //lambda 用于表示过拟合的这样一个参数，值越大，越不容易过拟合，但精确度就低
                    MatrixFactorizationModel model =  ALS.train(traningRdd.rdd(), rank, i, l);

                    // 对原始数据进行推荐值预测
                    JavaRDD<Rating> predict = model.predict(traningRdd2);
                    //对预测的数据格式包装
                    JavaPairRDD<Tuple2<Integer,Integer>,Double>predictRDD=predict.mapToPair(line->{
                      return new Tuple2<Tuple2<Integer,Integer>,Double>(new Tuple2(line.user(),line.product()), line.rating());
                    });
                    // 根据(userid, movieid)为key，将提供的rating与预测的rating进行比较
                    JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> predictAndFact = predictRDD.join(testJavaRdd2);

                    // 计算RMSE(均方根误差)

                    Double MSE = predictAndFact.mapToDouble(line -> {
                        Double err = line._2._1 - line._2._2;
                        return err * err;
                    }).mean();


                    Double RMSE = Math.sqrt(MSE); // 求平方根

                    // RMSE越小，代表模型越精确
                    if (RMSE < bestRMSE) {
                        // 将模型存储下来
                        model.save(sparkSession.sparkContext(),"hdfs://node1:9000/recommend_processed_bestModel/"+RMSE);
                        bestRMSE = RMSE;
                        bestIteration = i;
                        bestLambda = l;
                    }
                    System.out.println("Best model is located in hdfs://node1:9000/recommend_processed_bestModel/"+RMSE);
                    System.out.println("Best Iteration "+bestIteration);
                    System.out.println("Best bestLambda "+bestLambda);
                }
            }
        sparkSession.stop();
        }

}
