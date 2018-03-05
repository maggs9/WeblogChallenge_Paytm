package paytm
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.Column



object SessionLengthUser {
  def main(args: Array[String]){
        if (args.length != 2) {
          println("Args:<input file name> <output file name>");
          System.exit(1);
        }
      val input_path = args(0).trim // Here the input is Logs with session Ids i.e Data saved in "/logWithSessionIds" while doing the first analysis task.
      val output_path = args(1).trim
      
      val sparkConf = new SparkConf().setAppName("Predict Session Length and #Unique IP of a User").setMaster("local");
      val sc = new SparkContext(sparkConf);
      val sqlContext = new SQLContext(sc);
      import sqlContext.implicits._
      
      val logWithSessionIds = sqlContext.read.parquet(input_path)
      
      // User Agent Analysis :This is to understand the data, can be commented
      print(logWithSessionIds.filter($"user_agent" contains "Mobi").count()) //153505
      print(logWithSessionIds.count) // 1158500
      
      val tmp_mobile = logWithSessionIds.filter($"user_agent" contains "Mobi").groupBy($"cl_ip",$"sessionId").agg((unix_timestamp(max($"date_time"))- unix_timestamp(min($"date_time"))).as("ses_time"))
      val tmp_web = logWithSessionIds.filter(not ($"user_agent" contains "Mobi")).groupBy($"cl_ip",$"sessionId").agg((unix_timestamp(max($"date_time"))- unix_timestamp(min($"date_time"))).as("ses_time"))
      
      val user_mobile_sess = tmp_mobile.groupBy($"cl_ip").agg(avg($"ses_time").as("sess_time_mob"))
      val user_web_sess = tmp_web.groupBy($"cl_ip").agg(avg($"ses_time").as("sess_time_web"))
      val diff = user_mobile_sess.join(user_web_sess,"cl_ip")
    
      // I analysed difference in avg. session length values for different IPs on web and mobile. I found that there were some users who were more active on web, some on app and some on both mediums almost equally.
      //Thus as a first filter I propose to check the user agent. Based on that, I tried to cluster user using their characteristics. 
      // Thus there will be two clustering models - Model-1 for mobile and Model-2 for web.
      // The intersection of list of IPs of each model will be non-empty set, But the first request of the session starting will have user-agent. Using that, we can decide which model to user for predicting session time of the user.
      
      // Feature Extraction
      val featureExtractor = new ML_Feature_Extractor(logWithSessionIds)
      val (user_mobile, user_web) = featureExtractor.session_length_user
      
      // Making features ready for model//
      val featureCols_mobile = Array("avg_rqpt","avg_recB","ct_url","sess_time_mob")
      val assembler_mobile = new VectorAssembler().setInputCols(featureCols_mobile).setOutputCol("features")
      val df_mobile = assembler_mobile.transform(user_mobile)
      df_mobile.persist()
      
      val featureCols_web = Array("avg_rqpt","avg_recB","ct_url","sess_time_web")
      val assembler_web = new VectorAssembler().setInputCols(featureCols_web).setOutputCol("features")
      val df_web = assembler_web.transform(user_web)
      df_web.persist()
      
        
  def KMeans_Cal(data: DataFrame, k: Int, save_path: String): DataFrame = {
    val kmeans = new KMeans().setK(k).setSeed(1L)
    val model = kmeans.fit(data)
    
    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(data)
    println(s"Within Set Sum of Squared Errors = $WSSSE")
    
    model.save(save_path)
    
    // return cluster predictions
    model.summary.predictions
  }
      // CLuster User using K-means
      val pred_user_mobile = KMeans_Cal(df_mobile,6, output_path+"/kmeans_modile")
      val pred_user_web = KMeans_Cal(df_web,7, output_path+"/kmeans_web")
      
      """ // Best K value using Knee - point
      Mobile (best k = 6)
      
      4 	2.32
      5 	2.03
      6 	1.47
      7 	1.02
      8 	0.98
      9 	0.85
      10 	0.77
      11 	0.69

      WEB (best k = 7)
      k 	WWSE
      4 	1.72
      5 	1.37
      6 	1.97
      7 	1.03
      8 	0.76
      9 	0.63
      10 	0.53"""
      
          
    // Now get Mean session length and Standard Deviation of each of the cluster.
    // The predicted session time for each user will lie between (mean - std, mean+ std)
    
    def mySd(col: Column): Column = {
        sqrt(avg(col * col) - avg(col) * avg(col))
    }
    pred_user_mobile.groupBy("prediction").agg(avg($"sess_time_mob"),mySd($"sess_time_mob")).orderBy($"prediction").show()
    pred_user_web.groupBy("prediction").agg(avg($"sess_time_web"),mySd($"sess_time_web")).orderBy($"prediction").show()
    
    
    // SIMILAR ANALYSIS FOR - Predict the number of unique URL visits by a given IP
    // We saw in correlation matrix that more the session time more the number of URL hits by the user.
    // Since count of unique url is also one of the features used for clustering, I used the same clusters to approximate the number of unique url hits.
    pred_user_mobile.groupBy("prediction").agg(avg($"ct_url"),mySd($"ct_url")).orderBy($"prediction").show()
    pred_user_web.groupBy("prediction").agg(avg($"ct_url"),mySd($"ct_url")).orderBy($"prediction").show()
    
    
    

  

  }
}