package main.scala.paytm
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object analysis {
  def main(args: Array[String]){
    if (args.length != 2) {
      println("Args:<input file name> <output file name>");
      System.exit(1);
    }
    val input_path = args(0).trim
    val output_path = args(1).trim
    
    val sparkConf = new SparkConf().setAppName("WebLog Analysis").setMaster("local");
    val sc = new SparkContext(sparkConf);
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._
    val input_data = sqlContext.read.parquet(input_path)
    
    // Transform Data //
    val input_data_tmp_1 = input_data.withColumn("_tmp_1", split($"client_port", ":")).withColumn("cl_ip", $"_tmp_1".getItem(0)).withColumn("cl_port",$"_tmp_1".getItem(1)).
                           withColumn("_tmp_2", split($"backend_port", ":")).withColumn("bck_ip",$"_tmp_2".getItem(0)).withColumn("bck_port",$"_tmp_2".getItem(1)).
                           drop($"_tmp_1").drop($"client_port").drop($"_tmp_2").drop($"backend_port")

    val input_data_tmp_2 = input_data_tmp_1.withColumn("date_time",$"timestamp".cast(TimestampType)).withColumn("_tmp_1", split($"request", " ")).
    										    withColumn("request_type", $"_tmp_1".getItem(0)).withColumn("request_url", $"_tmp_1".getItem(1)).
    										    drop($"_tmp_1").drop($"timestamp").drop($"request")
    
    val input_data_trans = input_data_tmp_2.withColumn("_tmp_1", split($"request_url", "\\?")).withColumn("url", $"_tmp_1".getItem(0)).withColumn("get_params", $"_tmp_1".getItem(1)).drop($"_tmp_1")
    input_data_trans.repartition($"cl_ip")
    input_data_trans.write.parquet(output_path+"/Transformed_Data")
    
    // Q1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a fixed time window. 
    
    val logWithSessionIds_1 = input_data_trans.select('*,lag('date_time, 1).over(Window.partitionBy('cl_ip).orderBy('date_time)).as('prevTimestamp))

    val logWithSessionIds_2 =logWithSessionIds_1.select('*,when((unix_timestamp($"date_time") - unix_timestamp($"prevTimestamp"))< lit(15*60), lit(0)).otherwise(lit(1)).as('isNewSession))

    val logWithSessionIds = logWithSessionIds_2.select('*,sum('isNewSession).over(Window.partitionBy('cl_ip).orderBy('cl_ip, 'date_time)).as('sessionId))
    logWithSessionIds.write.parquet(output_path+"/logWithSessionIds")
    
    // Q2. Determine the average session time
    val avg_sess_time = logWithSessionIds.groupBy($"cl_ip",$"sessionId").agg((unix_timestamp(max($"date_time"))- unix_timestamp(min($"date_time"))).as("ses_time")).
                                          agg(avg($"ses_time")).collect.head(0)    

    // Q3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
    val unique_url_per_session_with_params = logWithSessionIds.groupBy($"cl_ip",$"sessionId").agg(countDistinct($"request_url").as("dist_url"))
    //unique_url_per_session_with_params.agg(avg($"dist_url")).show
    unique_url_per_session_with_params.write.parquet(output_path+"/unique_url_per_session_with_params")
    
    val unique_url_per_session_without_params =  logWithSessionIds.groupBy($"cl_ip",$"sessionId").agg(countDistinct($"url").as("dist_url"))
    //unique_url_per_session_without_params.agg(avg($"dist_url")) = 8.12
    unique_url_per_session_without_params.write.parquet(output_path+"/unique_url_per_session_without_params")
    

    // Q4. Find the most engaged users, ie the IPs with the longest session times
    val sess_interval = logWithSessionIds.groupBy($"cl_ip",$"sessionId").agg((unix_timestamp(max($"date_time"))- unix_timestamp(min($"date_time"))).as("ses_time"))
    val engaged_users_desc = sess_interval.groupBy($"cl_ip").agg(avg($"ses_time").as("avg_sess")).orderBy($"avg_sess".desc)
    val most_engaged_users = engaged_users_desc.limit(100)
    
    engaged_users_desc.write.parquet(output_path+"/engaged_users_desc")
  }
}