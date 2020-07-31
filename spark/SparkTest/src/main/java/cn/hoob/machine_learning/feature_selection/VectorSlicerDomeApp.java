package cn.hoob.machine_learning.feature_selection;


import avro.shaded.com.google.common.collect.Lists;
import org.apache.spark.ml.attribute.Attribute;
import org.apache.spark.ml.attribute.AttributeGroup;
import org.apache.spark.ml.attribute.NumericAttribute;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.VectorSlicer;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.List;

/**
 VectorSlicer
 算法介绍：
 VectorSlicer是一个转换器输入特征向量，输出原始特征向量子集。
 VectorSlicer接收带有特定索引的向量列，通过对这些索引的值进行筛选得到新的向量集。可接受如下两种索引
 1.整数索引，setIndices()。
 2.字符串索引代表向量中特征的名字，此类要求向量列有AttributeGroup，因为该工具根据Attribute来匹配名字字段。
 指定整数或者字符串类型都是可以的。另外，同时使用整数索引和字符串名字也是可以的。
 不允许使用重复的特征，所以所选的索引或者名字必须是没有独一的。注意如果使用名字特征，当遇到空值的时候将会报错。
 输出将会首先按照所选的数字索引排序（按输入顺序），其次按名字排序（按输入顺序）
 **/
public class VectorSlicerDomeApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("VectorSlicerDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        Attribute[] attrs = new Attribute[]{
                NumericAttribute.defaultAttr().withName("f1"),
                NumericAttribute.defaultAttr().withName("f2"),
                NumericAttribute.defaultAttr().withName("f3")
        };
        AttributeGroup group = new AttributeGroup("userFeatures", attrs);

        List<Row> data = Lists.newArrayList(
                RowFactory.create(Vectors.sparse(3, new int[]{0, 1}, new double[]{-2.0, 2.3})),
                RowFactory.create(Vectors.dense(-2.0, 2.3, 0.0))
        );

        Dataset<Row> dataset = sparkSession.createDataFrame(data, (new StructType()).add(group.toStructField()));

        VectorSlicer vectorSlicer = new VectorSlicer()
                .setInputCol("userFeatures").setOutputCol("features");

        vectorSlicer.setIndices(new int[]{1}).setNames(new String[]{"f3"});
       // or slicer.setIndices(new int[]{1, 2}), or slicer.setNames(new String[]{"f2", "f3"})

        Dataset<Row> output = vectorSlicer.transform(dataset);
        output.show();
        System.out.println(output.select("userFeatures", "features").first());
    }
}
