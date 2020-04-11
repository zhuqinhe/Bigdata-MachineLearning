package cn.hoob.sparkoperator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 该函数是用于将RDD[k,v]转化为RDD[k,c]，其中类型v和类型c可以相同也可以不同。
 * 其中的参数如下：
 * - createCombiner：该函数用于将输入参数RDD[k,v]的类型V转化为输出参数RDD[k,c]中类型C;
 * - mergeValue：合并函数，用于将输入中的类型C的值和类型V的值进行合并，得到类型C，输入参数是（C，V），输出是C；
 * - mergeCombiners：合并函数，用于将两个类型C的值合并成一个类型C，输入参数是（C，C），输出是C；
 * - numPartitions：默认HashPartitioner中partition的个数；
 * - partitioner：分区函数，默认是HashPartitionner；
 * - mapSideCombine：该函数用于判断是否需要在map进行combine操作，类似于MapReduce中的combine，默认是 true。
 *
 * 从源码中可以看出，combineByKey()的实现是一边进行aggregate，一边进行compute() 的基础操作。
 * 假设一组具有相同 K 的 <K, V> records 正在一个个流向 combineByKey()，createCombiner 将第一个 record 的 value 初始化为 c （比如，c = value），
 * 然后从第二个 record 开始，来一个 record 就使用 mergeValue(c, record.value) 来更新 c，
 * 比如想要对这些 records 的所有 values 做 sum，那么使用c = c + record.value。等到 records 全部被 mergeValue()，得到结果 c。
 * 假设还有一组 records（key 与前面那组的 key 均相同）一个个到来，combineByKey() 使用前面的方法不断计算得到 c’。
 * 现在如果要求这两组 records 总的 combineByKey() 后的结果，
 * 那么可以使用 final c = mergeCombiners(c, c') 来计算；然后依据partitioner进行不同分区合并
 * */
public class SparkCombineByKeyDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkCountByKeyDemo").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(5,4,3,2,1,5,4);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);
        //转化为pairRDD
        JavaPairRDD<Integer,Integer> javaPairRDD = javaRDD.mapToPair(tp->new Tuple2<Integer, Integer>(tp,1));

        JavaPairRDD<Integer,String> combineByKeyRDD = javaPairRDD.combineByKey(tp->tp+":createCombiner",
                (C,V)-> C + " :mergeValue: " + V,(V1,V2)->V1 + " :mergeCombiners: " + V2);
        System.out.println(combineByKeyRDD.collect());
    }
}
