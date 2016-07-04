### Summary of Analysis

* The min time window between two session was estimated to about 35 min
* All logs were sessionized using this time window.
* The average session time is about 169.55 sec ~ 2min 50 sec
* Distribution of the session time yields a knee point at around 350sec 
* All records having session time >= 350 sec were filtered out as HighSessionTime records.
* All records having session time < 350 sec were filtered out as LowSessionTime records.
* Features were generated for both sets of records and were compared.


### Findings

* Number of unique urls visited by high session time records is about 4 times that of low session time records.
* Average time spent on each unique url is also abbout 20 times higher for a high session time sessions.
as compared to sessions with lesser time
* Average backend processing is higher in case of sessions with higher time.
* The avg number of bytes sent is higher (almost 2 times) in case of sessions with high time
* This could possiblly indicate more number of POST requests.
* Most low session time session have visits to paytm.com/shop/xxx...
* Most high session time sessions have visits to paytm.com/shop/xxx  and paytm.com/papi/xxx .
* Thus one can conclude that the high session users are essentially people who are shopping online and making a lot of papi requests (I guess it could mean post api). They not only visit more number of unique urls but also spend more time on each of them.



### Platform and Tools Used

* Entire analysis was done using spark and python libraries 
* Currently working on the scala code for the same. Will update the repo as soon as I finish.

 
