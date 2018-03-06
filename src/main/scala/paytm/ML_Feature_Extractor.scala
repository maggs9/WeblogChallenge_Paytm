package paytm
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class ML_Feature_Extractor(input_data:DataFrame) extends Serializable {
  import input_data.sparkSession.implicits._
  
  // Features for Request Per Second //
  def request_load() : DataFrame = {
      
      // Convert Time stamps into : second and minute format, extract hour of the day, quater of the day, day of the week from timestamp
      val request_min = input_data.filter("date_time is not null").select($"date_time",date_format($"date_time", "y-M-d hh:mm:ss").as("date_sec"),
                                        date_format($"date_time", "y-M-d hh:mm").as("date_min"),quarter($"date_time").as("quater_day"),hour($"date_time").as("hour_day"),date_format($"date_time", "EEEE").as("day_week"),
                                        $"request_processing_time",$"backend_processing_time",$"response_processing_time",$"received_bytes",$"sent_bytes",$"request_type",$"url").
                                        repartition($"date_sec")
      
      // Get average request and backed processing time,  #unique url, avg recieve and sent bytes
      val agg_num = request_min.groupBy($"date_sec").agg(avg($"request_processing_time").as("avg_rqpt"),avg($"backend_processing_time").as("avg_bckpt"),avg($"response_processing_time").as("avg_respt"),avg($"received_bytes").as("avg_recB"),avg($"sent_bytes").as("avg_sentB"),countDistinct($"url").as("ct_url"))
      
      // distinct values of timestamp feature
      val req_day_min = request_min.select($"date_sec",$"quater_day",$"hour_day",$"day_week").distinct
       
      // Get total count of each request type of each second.
      val req_type_cnts = request_min.groupBy($"date_sec").pivot("request_type").count().na.fill(0)
      
      // Combine all features
      val expected_load_per_sec_data = agg_num.join(req_day_min,"date_sec").join(req_type_cnts,"date_sec")
      
      // Lag over 1 to get the prediction column i.e ct_url. Thus next_num_url is the responce variable
      val expected_load_per_sec_data_next_load = expected_load_per_sec_data.select('*,lag('ct_url, -1).over(Window.partitionBy('day_week).orderBy('date_sec)).as('next_num_url))
      expected_load_per_sec_data_next_load
    }
  
  // Features for Session Length and Unique URLs
  def session_length_user : (DataFrame,DataFrame) = {
    
      // Get web and mobile IPs filtered by user agent
      val tmp_mobile = input_data.filter($"user_agent" contains "Mobi").groupBy($"cl_ip",$"sessionId").agg((unix_timestamp(max($"date_time"))- unix_timestamp(min($"date_time"))).as("ses_time"))
      val tmp_web = input_data.filter(not ($"user_agent" contains "Mobi")).groupBy($"cl_ip",$"sessionId").agg((unix_timestamp(max($"date_time"))- unix_timestamp(min($"date_time"))).as("ses_time"))
      
      // Calculate average session time
      val user_mobile_sess = tmp_mobile.groupBy($"cl_ip").agg(avg($"ses_time").as("sess_time_mob"))
      val user_web_sess = tmp_web.groupBy($"cl_ip").agg(avg($"ses_time").as("sess_time_web"))      
      
      // So you can see the some users and more active on web , some on app, some on both equally, Thus we need to first check the user agent.
      //Thus first pass on that filter is user agent -> Model1 or Model2
      //Now cluster based on user charateristics.
      
      // Calculate avg. request_processing_time, received_bytes, #url
      val user_mobile_data = input_data.filter($"user_agent" contains "Mobi").select($"cl_ip",$"request_processing_time",$"received_bytes",$"url")
      val user_web_data = input_data.filter(not ($"user_agent" contains "Mobi")).select($"cl_ip",$"request_processing_time",$"received_bytes",$"url")
      
      val user_mobile_charac = user_mobile_data.groupBy($"cl_ip").agg(avg($"request_processing_time").as("avg_rqpt"),avg($"received_bytes").as("avg_recB"),countDistinct($"url").as("ct_url"))
      val user_web_charac = user_web_data.groupBy($"cl_ip").agg(avg($"request_processing_time").as("avg_rqpt"),avg($"received_bytes").as("avg_recB"),countDistinct($"url").as("ct_url"))
      
      // Join to get ALl features
      val user_mobile = user_mobile_sess.join(user_mobile_charac, "cl_ip")
      
      val user_web = user_web_sess.join(user_web_charac, "cl_ip")
    
      (user_mobile,user_web )
 
  }
}
