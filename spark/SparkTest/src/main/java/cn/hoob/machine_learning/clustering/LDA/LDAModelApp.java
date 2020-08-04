package cn.hoob.machine_learning.clustering.LDA;

import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 文档主题生成模型(LDA)
 算法介绍：
 LDA（Latent Dirichlet Allocation）是一种文档主题生成模型，也称为一个三层贝叶斯概率模型，包含词、主题和文档三层结构。
 所谓生成模型，就是说，我们认为一篇文章的每个词都是通过“以一定概率选择了某个主题，并从这个主题中以一定概率选择某个词语”这样一个过程得到。
 文档到主题服从多项式分布，主题到词服从多项式分布。
 LDA是一种非监督机器学习技术，可以用来识别大规模文档集（document collection）或语料库（corpus）中潜藏的主题信息。
 它采用了词袋（bag of words）的方法，这种方法将每一篇文档视为一个词频向量，从而将文本信息转化为了易于建模的数字信息。
 但是词袋方法没有考虑词与词之间的顺序，这简化了问题的复杂性，同时也为模型的改进提供了契机。
 每一篇文档代表了一些主题所构成的一个概率分布，而每一个主题又代表了很多单词所构成的一个概率分布。
 参数：
 checkpointInterval:类型：整数型。含义：设置检查点间隔（>=1），或不设置检查点（-1）。
 docConcentration:类型：双精度数组型。含义：文档关于主题（"theta"）的先验分布集中参数（通常名为“alpha"）。
 featuresCol:类型：字符串型。含义：特征列名。
 k:类型：整数型。含义：需推断的主题（簇）的数目。
 maxIter:类型：整数型。含义：迭代次数（>=0）。
 optimizer:类型：字符串型。含义：估计LDA模型时使用的优化器。含义：类别条件概率预测结果列名。
 seed:类型：长整型。含义：随机种子。
 subsamplingRate:类型：双精度型。含义：仅对在线优化器（即optimizer=”online”）。
 topicConcentration:类型：双精度型。含义：主题关于文字的先验分布集中参数（通常名为“beta"或"eta"）。
 topicDistributionCol:类型：字符串型。含义：每个文档的混合主题分布估计的输出列（文献中通常名为"theta"）
 **/
public class LDAModelApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("RandomForestRegressionModelApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "1").enableHiveSupport().getOrCreate();

        // Load training data   libsvm
        Dataset<Row> dataset = sparkSession.read().format("libsvm").
                load("D:\\ProgramFiles\\GitData\\hadoop\\spark\\SparkTest\\src\\main" +
                       "\\data\\mllib\\input\\mllibFromSpark\\sample_lda_libsvm_data.txt");
        // Trains a LDA model.
        LDA lda = new LDA().setK(10).setMaxIter(10);
        LDAModel model = lda.fit(dataset);

        double ll = model.logLikelihood(dataset);
        double lp = model.logPerplexity(dataset);
        System.out.println("The lower bound on the log likelihood of the entire corpus: " + ll);
        System.out.println("The upper bound bound on perplexity: " + lp);

        // Describe topics.
        Dataset<Row> topics = model.describeTopics(3);
        System.out.println("The topics described by their top-weighted terms:");
        topics.show(false);

        // Shows the result.
        Dataset<Row> transformed = model.transform(dataset);
        transformed.show(false);
    }
}
