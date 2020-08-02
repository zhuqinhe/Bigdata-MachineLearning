package cn.hoob.machine_learning.classify.LogisticRegression;

import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 逻辑回归

 算法原理：

 逻辑回归是一个流行的二分类问题预测方法。它是Generalized Linear models 的一个特殊应用以预测结果概率。
和线性SVMs不同，逻辑回归的原始输出有概率解释（x为正的概率）。
 二分类逻辑回归可以扩展为多分类逻辑回归来训练和预测多类别分类问题。
 如一个分类问题有K种可能结果，我们可以选取其中一种结果作为“中心点“，其他K－1个结果分别视为中心点结果的对立点。在spark.mllib中，
 取第一个类别为中心点类别。
 ＊目前spark.ml逻辑回归工具仅支持二分类问题，多分类回归将在未来完善。
 ＊当使用无拦截的连续非零列训练LogisticRegressionModel时，Spark MLlib为连续非零列输出零系数。这种处理不同于libsvm与R glmnet相似。
 参数：
 elasticNetParam：类型：双精度型。含义：弹性网络混合参数，范围[0,1]。
 featuresCol:类型：字符串型。含义：特征列名。
 fitIntercept:类型：布尔型。含义：是否训练拦截对象。
 labelCol:类型：字符串型。含义：标签列名。
 maxIter:类型：整数型。含义：最多迭代次数（>=0）。
 predictionCol:类型：字符串型。含义：预测结果列名。
 probabilityCol:类型：字符串型。含义：用以预测类别条件概率的列名。
 regParam:类型：双精度型。含义：正则化参数（>=0）。
 standardization:类型：布尔型。含义：训练模型前是否需要对训练特征进行标准化处理。
 threshold:类型：双精度型。含义：二分类预测的阀值，范围[0,1]。
 thresholds:类型：双精度数组型。含义：多分类预测的阀值，以调整预测结果在各个类别的概率。
 tol:类型：双精度型。含义：迭代算法的收敛性。
 weightCol:类型：字符串型。含义：列权重
 **/
public class LogisticRegressionModelApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("LogisticRegressionModelApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        // Load training data
        Dataset<Row> dataFrame = sparkSession.read().format("libsvm").
                load("D:\\ProgramFiles\\GitData\\hadoop\\spark\\SparkTest\\src\\main\\data\\mllib\\sample_libsvm_data.txt");

        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8);

        // Fit the model
        LogisticRegressionModel lrModel = lr.fit(dataFrame);

        // Print the coefficients and intercept for logistic regression
        System.out.println("Coefficients: "
                + lrModel.coefficients() + " Intercept: " + lrModel.intercept());
    }
}
