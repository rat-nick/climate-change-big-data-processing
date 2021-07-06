import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.functions._

object TrainTestSplit{
  
  def main(args: Array[String]) {
    
    val spark = SparkSession.builder.appName("Train-Test Split").getOrCreate()    
    
    val dir = "data/prepped/" + args(0)

    val df = spark
      .read 
      .option("header", true)
      .option("inferSchema", true)
      //.schema(schema)
      .csv(dir + "/*.csv")
      //.filter(x => x.getAs[Float]("lat") > 0)

    val Array(train, test) = df.randomSplit(Array(0.9, 0.1), 42)
    
    train
      .write
      .format("csv")
      .option("header", true)
      .save("data/ML/" + args(0) +"/train")
    
    test
      .write
      .format("csv")
      .option("header", true)
      .save("data/ML/" + args(0) +"/test")
  }
  
}