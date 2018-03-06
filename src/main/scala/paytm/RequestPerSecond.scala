package paytm
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.expressions.Window
import org.apache.spark.mllib.stat.Statistics


object RequestPerSecond {
  
    def main(args: Array[String]){
      if (args.length != 2) {
        println("Args:<input file name> <output file name>");
        System.exit(1);
      }
      val input_path = args(0).trim
      val output_path = args(1).trim
      val sparkConf = new SparkConf().setAppName("Request Per Second Prediction").setMaster("local");
      val sc = new SparkContext(sparkConf);
      val sqlContext = new SQLContext(sc);
      import sqlContext.implicits._
      
      // Load data
      val input_data = sqlContext.read.parquet(input_path);
      
      // Feature Extraction
      val featureExtractor = new ML_Feature_Extractor(input_data)
      val all_features = featureExtractor.request_load
      
      // Transform and Analysis
      
      // Change day of the week to dummy values. For this model I am using Label Encoder. Using One-hot encoding for each dummy variable as a column should also be checked.
      val indexer = new StringIndexer().setInputCol("day_week").setOutputCol("day_week_idx").fit(all_features)
      val data = indexer.transform(all_features)
      
      // After Correlation Analysis on response variable next_num_url features mentioned below were taken. The correlation range from 0.86 to 0.32 and -0.48 to -0.20
      val all_data = data.select($"date_sec".cast(TimestampType),$"avg_rqpt",$"avg_bckpt",$"avg_respt",$"avg_recB",$"avg_sentB",$"GET",$"POST",$"ct_url",$"next_num_url").na.fill(0)
      val max_time = all_data.select(max($"date_sec")).collect().head(0)
      
      // Taking last 5 minutes for prediction (Unknown Data). This helps when deploying. Output of one time-step is fed into the other. Error in this can help in actually understanding the deviation
      // Applying test and train on remaining data
      val last_5_mins = all_data.where($"date_sec" >= "2015-07-23 12:55:23")
      
      val known_data = all_data.where($"date_sec" < "2015-07-23 12:55:23")

      // converting into features //
      val featureCols = Array("avg_rqpt","avg_bckpt","avg_respt","avg_recB","avg_sentB","ct_url","GET","POST")
      val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
      val df_tmp = assembler.transform(known_data)
      val df = df_tmp.withColumn("label",$"next_num_url".cast(DoubleType))

      val Array(training, test) = df.select("label","features").randomSplit(Array(0.8, 0.2))
      
      // Model Building and Predictions
      val model_class = new ML_Model(training, test, output_path+"/model_" )
      
      // Linear Regression with Cross Validation and  predicting using the best model
      val model_lr_cv_test_result = model_class.lr_with_cv()
            
      // Some result when the model was run on the test set locally
      """
        MSE = 1866.2957319988477
        RMSE = 43.20064504146723
        R-squared = 0.5765565398894987
        MAE = 26.249633239437316
        Explained variance = 6724.321530669252
      """
      
      // LR without CV
      val model_lr_result = model_class.lr()
      
      // Some result when the model was run on the test set locally
      """
        MSE = 1865.077745115534
        RMSE = 43.18654588081262
        R-squared = 0.577391207934732
        MAE = 26.246404724129825
        Explained variance = 6724.3090820190255
      """

      // Decision Tree Regressor
      val model_dt_results = model_class.DecisionTree()
      
      // Some result when the model was run on the test set locally
      """
        MSE = 1954.0485168642133
        RMSE = 44.204620989939656
        R-squared = 0.5823001894761284
        MAE = 26.203013180182293
        Explained variance = 6722.315521000414
      """
      
      // Since model any of the model's results doesn't seem to be satisfactory in the first pass (Need to experiment more with model params), I decided to check for simple Autocorrelation
      // Since spark.ml doesn't have TimeSeries related funtions, I decided to write a simple autoregressor using Window function.
      // There is library "Sparkts" by Cloudera which was developed to handle TS data in spark. But it is not merged in Spark Main repo. and is not maintained by author now. 
      // So I haven't used but it is to-do list because - If AR works, I need to explore more Time series concepts like MR and ARMA and ARIMA and ACF, PACF to fins the appropriate number of lags.
      
      // Get Lag values. Lag = 1 to 6
      val w = Window.partitionBy($"day_week").orderBy($"date_sec")
      val defaultValue = 0
      val lag_values= data.select($"date_sec",$"ct_url".cast(DoubleType),lag($"ct_url", 1, defaultValue).over(w).as('lag_1).cast(DoubleType),lag($"ct_url", 2, defaultValue).over(w).as('lag_2).cast(DoubleType),
                      lag($"ct_url", 3, defaultValue).over(w).as('lag_3).cast(DoubleType),lag($"ct_url", 4, defaultValue).over(w).as('lag_4).cast(DoubleType),
                      lag($"ct_url", 5, defaultValue).over(w).as('lag_5).cast(DoubleType),lag($"ct_url", 6, defaultValue).over(w).as('lag_6).cast(DoubleType))
                      
      // corelation between these
      val lag_rdd = lag_values.rdd.map(x => org.apache.spark.mllib.linalg.Vectors.dense(x.getDouble(1),x.getDouble(2), x.getDouble(3),x.getDouble(4),x.getDouble(5),x.getDouble(6),x.getDouble(7)))
      
      // Vectorizer
      val correlMatrix = Statistics.corr(lag_rdd, "pearson")
      
      println(correlMatrix.toString(10,Int.MaxValue))
      """ Some Result :
      1.0                 0.8488539796321346  0.791027857373547   0.7591143414971272  0.7508978004092767  0.7413519141262985  0.7254728168505064
      0.8488539796321346  1.0                 0.8490797509601098  0.7912477884603276  0.7593024205606121  0.7510958763704945  0.7415501874912089
      0.791027857373547   0.8490797509601098  1.0                 0.8492111130081272  0.7914355561690448  0.7595147273010507  0.751315841892783
      0.7591143414971272  0.7912477884603276  0.8492111130081272  1.0                 0.8493363570010883  0.7916059408720163  0.759710450651749
      0.7508978004092767  0.7593024205606121  0.7914355561690448  0.8493363570010883  1.0                 0.8494499816717163  0.791761883393174
      0.7413519141262985  0.7510958763704945  0.7595147273010507  0.7916059408720163  0.8494499816717163  1.0                 0.8495634068008607
      0.7254728168505064  0.7415501874912089  0.751315841892783   0.759710450651749   0.791761883393174   0.8495634068008607  1.0
      """
      
      // Since there is high correlation, just taking an average of the last 6 lags should be good. Also, this is just using last 7 secs. We should also use ACF to get the right lag values.
      // 5 Rows in the starting will have missing value which is filled by 0. I haven't changed it because it accounts for missing values in the data //
      val lag_values_pred = lag_values.withColumn("autoregressive_value",($"lag_1"+$"lag_2"+$"lag_3"+$"lag_4"+$"lag_5"+$"lag_6")/6)
      val lag_values_pred_rdd = lag_values_pred.rdd.map(x=> (x.getDouble(1),x.getDouble(8)))
      model_class.reg_metrices(lag_values_pred_rdd)
      
      """
        MSE = 1735.122879133538
        RMSE = 41.6548061948863
        R-squared = 0.650103575682677
        MAE = 25.164935882306658
        Explained variance = 5962.082293356887
      """
      
      // Since there is a reduction in MSE and improvement in R-sq value, one should experiment more with different values of ARIMA.
  }
}