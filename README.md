
 ### Weblog Challenge

 #### Data : 
 Elastic Load Balancer Logs of two days. To get sense of the fields in the log file I referenced ELB documents at Amazon ELB documentation. The fields are:
 "timestamp elb client:port backend:port request_processing_time backend_processing_time response_processing_time elb_status_code backend_status_code received_bytes sent_bytes "request" "user_agent" ssl_cipher ssl_protocol"

 #### Code Structure : 
 1. LoadData.scala object : 
  * Input: path to log file (f1), save path for dataframe (f2)
  * Loads log data in text format
  * Runs regex through each line to match the ELB fields
  * Converts RDD with field values intpo Spark - Dataframe with appropiate columns names.
  * Saves dataframe in parquet format

 2. analysis.scala object : 
 * Input: f2 file which stores dataframe, folder path for saving various file that will be generated after queries
 * Transforms data into more descriptive feilds:
   ** client IP : IP and port
   ** backend IP : IP and port
   ** request_url : url and query params
   ** request to GET/POST/DELETE etc. (#6)
 * Sessionize using lag and window function over timestamp for each IP and store results in parquet file (f3)
 * Determine the average session time
 * Determine unique URL visits per session for both cases: url with query params and without
 * Calculate average engagement time of each user and get the most engaged users

 3. ML_Feature_Extractor.scala Class :
 Calculates features for various prediction tasks.
 * request_load : Extracts features for load (request per second) predcition.
 * session_length_user : Extract features for session length and number of unique url prediction model

 4. ML_Model.scala Class:
 Functions for different regression models
 * lr_with_cv : Linear Regression with cross validation
 * lr : Linear Regression without cross validation
 * DecisionTree : Decision Tree Regressor
 * reg_metrices : Calculated metrics for evaluating regression results

 5. RequestPerSecond.scala Object:
 Runs analysis and training process for predicting expected load
 * Input :  f3 file which stores dataframe, folder path for saving various models
 * Calls ML_Feature_Extractor class's request_load function for extracting features from input dataframe
 * Preform operations to transform these features into sparkML types
 * Calls ML_Model class's different functions for training, and analysing results and storing models.
 * Runs a Autoregressive model for predction of the same.

 6. SessionLengthUser.scala Object:
 Ananlysis, clustering and prediction of different IP's expected session length and unique URL hits
   * Input : f3 file which stores dataframe, folder path for saving models
   * Performs user agent analysis : Mobile/Web
   * Calls ML_Feature_Extractor class's session_length_user function for extracting features from input dataframe
   * Cluster users based on client IP features using k-means
   * Calculates the mean and standard deviation of avg session length and unique url hits for each cluster.

#### Ananlysis and Model for Prediction Problems:

1. Predict the expected load (requests/second) in the next minute 

* Feature Extraction :
  * Using data from the last second:
    * Extracted features like : hour of the day, quater of the day, day of the week from timestamp, average request and backend processing time, total count of each request type (GET/POST/DELETE etc) of each second, unique urls, avg recieve and sent bytes.
    * Correlation of these features with the load in the next second.
    * After Correlation Analysis on response variable next_num_url features used for model building were avg_rqpt, avg_bckpt, avg_respt, avg_recB, avg_sentB, GET, POST, ct_url.
    * The correlation values range from 0.86 to 0.32 and -0.48 to -0.20 of the features used.
  * Using ARIMA
    * Lags values of last 6 second using window function

* Models Tried :
  * I started with using Linear regression with/without cross validaion using the above features.
  * Then I tried Decision Tree regressor to check if results improve.
  * Since there was not much improvement in the results, I pivoted towards time-series based approach. I decided to check for simple sutocorrelation. Since spark.ml doesn't have TimeSeries related funtions, I decided to write a simple autoregressor using Window function.
   *  There was a high correlation among lags, thus I thought taking an average of the last 6 lags should be good. 
   *  There was a reduction in MSE and improvement in R-sq value, thus one should experiment more with different values of ARIMA.
  
  Note : There is library "Sparkts" by Cloudera that was developed to handle Time Series data in spark. But it is not merged in Spark Main repo. and is not maintained by author now. So I haven't used but it is to-do list because - If AR works, I need to explore more Time series concepts like MR and ARMA and ARIMA and ACF, PACF to find the appropriate number of lags.
  
 * To-Do:
   * ARIMA models
   * Non linearity
  
2. Predict the session length and number unique URLsfor a given IP

* Feature Extraction:
  * I checked number of IPs having user agent as web or mobile and what are the different values of seesion length and number of URL hits of these IP on different platform.
  * I analysed difference in avg. session length values for different IPs on web and mobile. I found that there were some users who were more active on web, some on app and some on both mediums almost equally.Thus as a first filter I propose to check the user agent. 
  * Based on that, I tried to cluster user using their characteristics. Thus there will be two clustering models - Model-1 for mobile and Model-2 for web. The intersection of list of IPs of each model will be non-empty set, But the first request of the session starting will have user-agent. Using that, we can decide which model to user for predicting session time of the user.
  * Features used for clustring were based on IPs historical data of attributes that are a function of client interaction.
  * Features used were for each IP were avg. request_processing_time, avg. received_bytes, avg. number of unique url, avg. session time 

* Model:
  * I tried k-means for clustering the users.
  * Exprimented with different k values to get knee point of WSSE values.
  * Best k value for mobile was 6 and web was 7.
  * Caluculted mean and standard deviation of session length and # unique urls of each cluster.
  * The predicted session time for each user will lie between (mean - std, mean+ std). Similar case with # unique urls.
  
* To-Do
  * Link graph of urls to understand if there is a pattern (Like only recharge urls were hit in that session)
  * More methods for clustering users
  

