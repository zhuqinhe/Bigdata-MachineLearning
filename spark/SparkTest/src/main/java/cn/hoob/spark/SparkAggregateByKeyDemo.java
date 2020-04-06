package cn.hoob.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/***
 * aggregateByKey函数对PairRDD中相同Key的值进行聚合操作，在聚合过程中同样使用了一个中立的初始值。
 * 和aggregate函数类似，aggregateByKey返回值的类型不需要和RDD中value的类型一致。
 * 因为aggregateByKey是对相同Key中的值进行聚合操作，所以aggregateByKey函数最终返回的类型还是Pair RDD，
 * 对应的结果是Key和聚合好的值；而aggregate函数直接是返回非RDD的结果，这点需要注意。在实现过程中，定义了三个aggregateByKey函数原型，
 * 但最终调用的aggregateByKey函数都一致。其中，参数zeroValue代表做比较的初始值；
 * 参数partitioner代表分区函数；参数seq代表与初始值比较的函数；参数comb是进行合并的方法
 * **/
public class SparkAggregateByKeyDemo {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //将这个测试程序拿文字做一下描述就是：在data数据集中，按key将value进行分组合并，
        //合并时在seq函数与指定的初始值进行比较，保留大的值；然后在comb中来处理合并的方式。
        List<Integer> data = Arrays.asList(5,5,4,3,2,1,1,1,1);
        int numPartitions = 4;
        JavaRDD<Integer> javaRDD = jsc.parallelize(data,2);
        final Random random = new Random(50);
        //构建一个二位rdd
        //JavaPairRDD<Integer,Integer> javaPairRDD = javaRDD.mapToPair(tp->new Tuple2<>(tp,random.nextInt(50)));
        JavaPairRDD<Integer,Integer> javaPairRDD = javaRDD.mapToPair(tp->new Tuple2<>(tp,random.nextInt(10)));
        //打印看下
        System.out.println(javaPairRDD.collect());

        JavaPairRDD<Integer, Integer> aggregateByKeyRDD = javaPairRDD.aggregateByKey(2,numPartitions,(v1,v2)->{
           return Math.max(v1, v2);//函数功能可以自定义
           //return v1+v2;//局部聚合？
            },(v1,v2)->v1+v2);//全局聚合
        System.out.println(aggregateByKeyRDD.partitions().size());
        System.out.println(aggregateByKeyRDD.collect());
    }
}
