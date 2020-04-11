package cn.hoob.sparkoperator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * 与treeAggregate类似，只不过是seqOp和combOp相同的treeAggregate。
 *
 * 从源码中可以看出，treeReduce函数先是针对每个分区利用scala的reduceLeft函数进行计算；
 * 最后，在将局部合并的RDD进行treeAggregate计算，这里的seqOp和combOp一样，初值为空。在实际应用中，可以用treeReduce来代替reduce，
 * 主要是用于单个reduce操作开销比较大，而treeReduce可以通过调整深度来控制每次reduce的规模
 * **/
public class SparkTreeReduceDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkTakeOrderedDemo").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data,5);
        JavaRDD<String> javaRDD1 = javaRDD.map(tp->Integer.toString(tp));
        String result = javaRDD1.treeReduce((v1,v2)->{
                return v1 + "=" + v2;
        });
        System.out.println(result);
    }
}
