import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

// -Dio.netty.tryReflectionSetAccessible=true is required additionally for Apache Arrow library
public class SimpleSparkApplication {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Simple Application")
                .getOrCreate();
        spark.sparkContext().setLogLevel("DEBUG");

        List<String> data = Arrays.asList("dev, engg, 10000", "karthik, engg, 20000");
        Dataset<Row> dataframe = spark.sqlContext().createDataset(data, Encoders.STRING()).toDF();
        dataframe.show();
        spark.stop();
    }
}