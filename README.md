
### Weblog Challenge

#### Data : 
Elastic Load Balancer Logs of two days. To get sense of the fields in the log file and checked ELB documents at Amazon ELB documentation. THe fields are:
timestamp elb client:port backend:port request_processing_time backend_processing_time response_processing_time elb_status_code backend_status_code received_bytes sent_bytes "request" "user_agent" ssl_cipher ssl_protocol

#### Code Structure : 
1. LoadData.scala object : 
* Input: path to log file (f1), save path for dataframe (f2)
* Loads log data in text format
* Runs regex through each line to match the ELB fields
* Converts RDD with field values intpo Spark - Dataframe with appropiate columns names.
* Saves dataframe in parquet format.

2. analysis.scala object : 
* Input: f2 file which stores dataframe, folder path for saving various file that will be generated after queries
* Transforms data into more descriptive feilds:
  ** client IP : IP and port
  ** backend IP : IP and port
  ** request_url : url and query params
  ** request to GET/POST/DELETE etc. (#6)
* Sessionize using lag and window function over timestamp for each IP and store results in parquet file. (f3)
* Determine the average session time
* Determine unique URL visits per session for both cases: url with query params and without
* Calculate average engagement time of each user and get get the most engaged users.

3. ML_Feature_Extractor.scala Class :
Calculates features for various prediction tasks.
* request_load : Extracts features for request/second load predcition.
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
Ananlysis, cluster and predict different IP's expected session length and unique URL hits
* Input : f3 file which stores dataframe, folder path for saving models
* Performs user agent analysis : Mobile/Web
* Calls ML_Feature_Extractor class's session_length_user function for extracting features from input dataframe
* Cluster users based on client IP features using k-means
* Calculates the mean and standard deviation of avg session length and unique url hits for each cluster.

