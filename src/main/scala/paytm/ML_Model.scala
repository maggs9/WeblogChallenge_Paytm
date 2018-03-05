package paytm
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.feature.VectorAssembler

import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.ml.clustering.KMeans



class ML_Model(training:DataFrame,test:DataFrame,save_path:String) extends Serializable {
  
  def reg_metrices(inputRDD: RDD[(Double, Double)]){
      val metrics = new RegressionMetrics(inputRDD)
      // Squared error
      println(s"MSE = ${metrics.meanSquaredError}")
      println(s"RMSE = ${metrics.rootMeanSquaredError}")
      
      // R-squared
      println(s"R-squared = ${metrics.r2}")
      
      // Mean absolute error
      println(s"MAE = ${metrics.meanAbsoluteError}")
      
      // Explained variance
      println(s"Explained variance = ${metrics.explainedVariance}")
  }
  
  def lr_with_cv() : DataFrame = {
      // Model
      val lr = new LinearRegression().setMaxIter(10).setFeaturesCol("features").setLabelCol("label")
      
      // ParamGridBuilder to construct a grid of parameters to search over.
      val paramGrid = new ParamGridBuilder().addGrid(lr.regParam, Array(0.1, 0.01)).addGrid(lr.fitIntercept).build()
      
      // A  CrossValidator requires an Estimator (linear regression), a set of Estimator ParamMaps, and an Evaluator
      val cv = new CrossValidator().setEstimator(lr).setEvaluator(new RegressionEvaluator).setEstimatorParamMaps(paramGrid).setNumFolds(5)
      
      // Run cross-validation, and choose the best set of parameters.
      val cvModel = cv.fit(training)
      
      // Make predictions on test data. model is the model with combination of parameters
      // that performed best.
      val test_results = cvModel.transform(test).select("features", "label", "prediction")
      val result_rdd = test_results.rdd.map(x=> (x.getDouble(1),x.getDouble(2)))
      
      // Print Regression Metrics
      reg_metrices(result_rdd)
      
      // Save Model
      cvModel.save(save_path+"lr_with_cv")

      // return Test Results
      test_results
  }
  
  def lr() : DataFrame = {
    
     val lr = new LinearRegression().setMaxIter(10).setFeaturesCol("features").setLabelCol("label")
     val lrModel = lr.fit(training)
    
    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    
    // Summarize the model over the training set and print out some metrics // Check overfitting with test and train values i.e are not acceptably similar.
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}") 
    
    // Test summary //
    val test_results = lrModel.transform(test)
    val result_rdd = test_results.rdd.map(x=> (x.getDouble(1),x.getDouble(2)))
    
    // Print Regression Metrics
    reg_metrices(result_rdd)
    
    // Save Model
    lrModel.save(save_path+"lr")
      
    // return Test Results
    test_results
  }
  
  def DecisionTree() : DataFrame = {
    // Train a DecisionTree model.
    val dt = new DecisionTreeRegressor().setLabelCol("label").setFeaturesCol("features")
    
    // Train model. This also runs the indexer.
    val DTmodel = dt.fit(training)
    
    // Make predictions.
    val test_results = DTmodel.transform(test)
    
    // Test summary //
    val test_results_tmp = test_results.select( "label","prediction","features")
    val result_rdd = test_results_tmp.rdd.map(x=> (x.getDouble(0),x.getDouble(1)))
    
    // Print Regression Metrics
    reg_metrices(result_rdd)
    
    // Save Model
    DTmodel.save(save_path+"dtv")
      
    // return Test Results
    test_results  
  }
}