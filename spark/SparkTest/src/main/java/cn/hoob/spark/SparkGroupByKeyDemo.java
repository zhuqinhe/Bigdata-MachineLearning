package cn.hoob.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
/**
 * 感觉reduceByKey只能完成一些满足交换率，结合律的运算，如果想把某些数据聚合到一些做一些操作，得换groupbykey
 *
 * 比如下面：我想把相同key对应的value收集到一起，完成一些运算（例如拼接字符串，或者去重）
 * **/
public class SparkGroupByKeyDemo {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String args[]) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("SparkGroupByKeyDemo").setMaster("local");

        JavaSparkContext context = new JavaSparkContext(sparkConf);

        List<Integer> data = Arrays.asList(1,1,2,2,1,3,4,5,6,7,8,8,9,7);
        JavaRDD<Integer> distData= context.parallelize(data);

        JavaPairRDD<Integer, Integer> firstRDD = distData.mapToPair(tp->new Tuple2<>(tp,tp*tp));

        JavaPairRDD<Integer, Iterable<Integer>> secondRDD = firstRDD.groupByKey();
        System.out.println(secondRDD.collect());
   /*     List<Tuple2<Integer, String>> reslist = secondRDD.map(
                new Function<Tuple2<Integer, Iterable<Integer>>, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<Integer, Iterable<Integer>> integerIterableTuple2) throws Exception {
                        int key = integerIterableTuple2._1();
                        StringBuffer sb = new StringBuffer();
                        Iterable<Integer> iter = integerIterableTuple2._2();
                        for (Integer integer : iter) {
                            sb.append(integer).append(" ");
                        }
                        return new Tuple2(key, sb.toString().trim());
                    }
                }).collect();*/
        List<Tuple2<Integer,String>> reslist = secondRDD.map(tp->{
            String str="";
            for(Integer in:tp._2){
                str=str+in+",";
            }
            return  new Tuple2<>(tp._1,str);
        }).collect();


        for(Tuple2<Integer, String> str : reslist) {
            System.out.println(str._1() + "\t" + str._2() );
        }
        context.stop();
    }
}
