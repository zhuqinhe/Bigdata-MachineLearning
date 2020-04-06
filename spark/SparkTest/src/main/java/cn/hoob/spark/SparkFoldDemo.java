package cn.hoob.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
/**
 * fold是aggregate的简化，将aggregate中的seqOp和combOp使用同一个函数op。
 * 从源码中可以看出，先是将zeroValue赋值给jobResult，
 * 然后针对每个分区利用op函数与zeroValue进行计算，再利用op函数将taskResult和jobResult合并计算，
 * 同时更新jobResult，最后，将jobResult的结果返回
 * **/
public class SparkFoldDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkFoldDemo").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<String> data = Arrays.asList("7","5", "1","4","3", "1", "3", "6", "2", "2","8");
        JavaRDD<String> javaRDD = jsc.parallelize(data,5);
        JavaRDD<String> partitionRDD = javaRDD.mapPartitionsWithIndex(
                (index,tp)->{
                    LinkedList<String> linkedList = new LinkedList<String>();
                    while(tp.hasNext()){
                        linkedList.add(index+ "=" + tp.next());
                    }
                    return linkedList.iterator();
                },false);

        System.out.println(partitionRDD.collect());

        String foldRDD = javaRDD.fold("0",(f1,f2)->{
                  return f1+"-"+f2;
                }
            );
        System.out.println(foldRDD);
    }
}
