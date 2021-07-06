import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.DataFrameStatFunctions


object TrainSpatioTemporalModel {
    def main(args: Array[String]) {
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
        
        val testDir = "data/ML/SpatioTemporalTemperatureData/test"
        val trainDir = "data/ML/SpatioTemporalTemperatureData/train"
        
        val spark = SparkSession.builder.appName("ML train").getOrCreate()

        val trainData = spark
            .read
            .format("csv")
            .option("header", true)
            .option("inferSchema", true)
            .load(trainDir + "/*.csv")
            .where("lat > 0")

        val testData = spark
            .read
            .format("csv")
            .option("header", true)
            .option("inferSchema", true)
            .load(testDir + "/*.csv")
            .where("lat > 0")

        trainData.describe().show()

        val assembler = new VectorAssembler()
            .setInputCols(Array("lat", "lng", "year", "month"))
            .setOutputCol("features")
    
        val trainSet = assembler.transform(trainData).select("features", "temperature")
        val testSet = assembler.transform(testData).select("features", "temperature")
        
        trainSet.show()
        testSet.show()

        val lr = new LinearRegression()
            .setFeaturesCol("features")
            .setLabelCol("temperature")
        
        val model = lr.fit(trainSet)
        val trainingSummary = model.summary
        
        val testSummary = model.evaluate(testSet)
        
        testSummary.predictions.show()
        println(testSummary.rootMeanSquaredError)  
    }
}
    
object TrainTemporalTemperatureEmissionModel {
    def main(args: Array[String]){
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
        
        val testDir = "data/ML/yearlyEmissionsTemperature/test"
        val trainDir = "data/ML/yearlyEmissionsTemperature/train"
        
        val spark = SparkSession.builder.appName("ML train").getOrCreate()

        val trainData = spark
            .read
            .format("csv")
            .option("header", true)
            .option("inferSchema", true)
            .load(trainDir + "/*.csv")
            

        val testData = spark
            .read
            .format("csv")
            .option("header", true)
            .option("inferSchema", true)
            .load(testDir + "/*.csv")

        val assembler = new VectorAssembler()
            .setInputCols(Array("year", "emission"))
            .setOutputCol("features")
        
        val trainSet = assembler.transform(trainData).select("features", "temperature")
        val testSet = assembler.transform(testData).select("features", "temperature")    
        
        val lr = new LinearRegression()
            .setFeaturesCol("features")
            .setLabelCol("temperature")
        
        val model = lr.fit(trainSet)
        val trainingSummary = model.summary
        
        val testSummary = model.evaluate(testSet)        
        testSummary.predictions.show()  

        println(testSummary.rootMeanSquaredError)

    }
}