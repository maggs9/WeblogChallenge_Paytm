package main.scala.paytm
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd
import org.apache.spark.sql.SQLContext

object loadData { 
  def main(args: Array[String]){
    if (args.length != 2) {
      println("Args:<input file name> <output file name>");
      System.exit(1);
    }
    val input_loc = args(0).trim
    val output_loc = args(1).trim
    
    // Initialize spark
    val sparkConf = new SparkConf().setAppName("WebLog Parser").setMaster("local");
    val sc = new SparkContext(sparkConf);
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._
    
    // Load log data in text format
    val log = sc.textFile(input_loc);//"/Users/IronMan/Documents/prep/WeblogChallenge_Paytm/project/data/2015_07_22_mktplace_shop_web_log_sample.log"
    
    // Regex Parser
    val elbLogRegex = """(\d{4}-\d{2}-\d{2}\w\d{2}:+\d.+\d\w) (\w\S+) (\S+) (\S+) (\S+) ([\d.]+[:\d]) ([\d.]+[:\d]) (\d{3}) (\d{3}) (\d+) (\d+) "(.+?)" "(.+?)" ([\w-]+) ([\w+\d.]+)""".r
    def extractKey(line: String): (String, String, String,String, String, String,String, String, String,String, String, String,String, String, String) = {
  elbLogRegex.findFirstIn(line) match {
    case Some(elbLogRegex(timestamp, elb, client_port, backend_port, request_processing_time, backend_processing_time, response_processing_time, elb_status_code, backend_status_code, received_bytes, sent_byte, request, user_agent, ssl_cipher,ssl_protocol)) =>
    (timestamp, elb, client_port, backend_port, request_processing_time, backend_processing_time, response_processing_time, elb_status_code, backend_status_code, received_bytes, sent_byte, request, user_agent, ssl_cipher,ssl_protocol)
    case _ => (null, null, null,null, null, null,null, null, null,null, null, null,null, null, null)
  }
}
    // Convert to Dataframe
    val all_logs = log.map(x=> extractKey(x));
    val all_logs_df = all_logs.toDF("timestamp","elb","client_port","backend_port","request_processing_time","backend_processing_time","response_processing_time","elb_status_code","backend_status_code","received_bytes","sent_bytes","request","user_agent","ssl_cipher","ssl_protocol");
    
    // Save as parquet
    all_logs_df.write.parquet(output_loc) //("/Users/IronMan/Documents/prep/WeblogChallenge_Paytm/project/data/logs_parquet")
  }
}