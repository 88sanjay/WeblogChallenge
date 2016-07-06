'''
This file contains methods and class definitions used for  data analysis 
sessions of IPs. 
* An attempt is made  to understand what the min 
inactivity window between two reqs should be to consider them as part of different 
sessions. 
* Once the window is arrived upon, the records are sessionized and the 
sessions with high session time are filtered from the entire dataset.
* Features are the derived for each session to compare the high session time
records from the low session time records.
* Results of the analysis are presented in the comments at the end of the section
'''

from pyspark import SparkContext
from pyspark.sql import SQLContext,Row
import re
import numpy as np 
from datetime import datetime
import matplotlib.pyplot as plt


## PARSER METHODS

'''
parser - Method to parse the raw logs and split into required fields 

'''
def parser(x):
	# remove the data in double quotes first by spliting on double quotes
	tempVar = (re.split(' \"|\" ',x))
	retValues = tempVar[0].split(" ") + tempVar[1:3] + tempVar[3].split(" ")
	ret = dict(zip(headerbc.value,retValues))
	# convert timestamp to date time object
	ret['timestamp'] = datetime.strptime(ret['timestamp'],'%Y-%m-%dT%H:%M:%S.%fZ')
	# convert all time duration captured to floating point numbers
	ret['request_processing_time'] =  float(ret['request_processing_time'])
	ret['backend_processing_time'] =  float(ret['backend_processing_time'])
	ret['response_processing_time'] = float(ret['response_processing_time'])
	return ret

'''
getDeltas - method to get time difference between consecutive request events 
            Takes array of rawRDD records as input
'''
def getDeltas(x):
	out = []
	for i in range(len(x) - 1) :
		elapsedTime = x[i+1]['timestamp'] - x[i]['timestamp']
		out = out + [elapsedTime.total_seconds()/60.0]
	return out

'''
splitOnSessionDelta - method to split a group of consecutive request events into sessions
					  based on assumption that delta in timestamp of consecutive events 
					  cannot exceed thrshold.
'''
def splitOnSessionDelta(x,threshold):
	ip = x[0]
	deltas = getDeltas(x[1])
	idToSplit=[idx for idx,value in enumerate(deltas) if value >= threshold]
	out = []
	if len(idToSplit) > 0:
		start = 0
		for i in idToSplit:
			end = i+1
			out = out+[(x[0],x[1][start:end])]
			start = i+1
		out = out + [(x[0],x[1][idToSplit[-1]+1:])]
	else:
		out = [x]
	return out

'''
pruneSessions -  method to prune consecutive duplicate records in a session 

'''

def pruneSessions(x):
	fullurlset = [i['request'] for i in x[1]]
	urls = set(fullurlset) # changes sorting
	ret = []
	for i in urls:
		ret = ret + [x[1][fullurlset.index(i)]]
	return (x[0],sorted(ret,key=lambda k : k['timestamp']))


## FEATURE GENERATION METHODS
'''
getPath  - method to get the first directory in the path of the request url
'''

def getPath(url):
	elems = re.findall(r'\.[a-zA-Z0-9]+:?[0-9]*(\/[a-zA-Z0-9]+)\/',url)
	if len(elems) == 0 :
		elems = re.findall(r'\.[a-zA-Z0-9]+:?[0-9]*(\/[a-zA-Z0-9]*)',url)
	return (elems[0] if len(elems) > 0 else "/")

'''
getSessionTime -  method to get time duration of a particular session
'''
def getSessionTime(x):
	ret = 0
	if len(x[1]) > 1:
		delta = x[1][-1]["timestamp"] - x[1][0]["timestamp"]
		ret = delta.total_seconds()
	return (ret + max(0.0,x[1][-1]["request_processing_time"]) + \
	max(0.0,x[1][-1]["backend_processing_time"]) + \
	max(0.0,x[1][-1]["response_processing_time"]))

'''
countKPIs  - method to count number of occurances of each element of y 
			 in x
'''

def countKPIs(x,y,name):
	ret = []
	for i in y:
		ret = ret+[(name+"_"+i.replace('.','*'),x.count(i))]
	return ret

'''
createKPIs  - method to create all features
'''

def createKPIs(x,formapper=True):
	# Session time
	sessiontime = [("sessiontime",getSessionTime(x))]
	# Unique URLs
	numurls = [("numurls",len(set([i['request'] for i in x[1]])))]
	# Time per url
	timeperurl = [("timeperurl",sessiontime[0][1]/numurls[0][1])]
	avgdeltatimes = [("avgdeltatimes",sum(getDeltas(x[1]))/len(x[1]))]
	# backend ip
	backendips = countKPIs([i["backend:port"].split(":")[0] for i in x[1]],
		bipbc.value,"backendip")
	avg_request_processing_time = [("avg_request_processing_time_per_url",
		sum([max(0,i['request_processing_time']) for i in x[1]])/numurls[0][1])]
	avg_backend_processing_time	= [("avg_backend_processing_time_per_url",
		sum([max(0,i['backend_processing_time']) for i in x[1]])/numurls[0][1])]
	avg_response_processing_time = [("avg_response_processing_time_per_url",
		sum([max(0,i['response_processing_time']) for i in x[1]])/numurls[0][1])]
	elbstatuscode = countKPIs([i["elb_status_code"][0] for i in x[1]],
		elbstatusbc.value,"elb_status_code")	
	total_received_bytes = [
	("total_received_bytes_per_url",
		sum([float(i['received_bytes']) for i in x[1]])/numurls[0][1])
	]
	total_sent_bytes = [
	("total_sent_bytes_per_url",
		sum([float(i['sent_bytes']) for i in x[1]])/numurls[0][1])
	]
	request = countKPIs([getPath(i["request"]) for i in x[1]],
		hostbc.value,"request")
	# user_agent
	ssl_protocol = countKPIs([i["ssl_protocol"] for i in x[1]],sslprotbc.value,"sslport")
	ret = dict(sessiontime+
		numurls+
		request+
		backendips+
		elbstatuscode+
		avg_response_processing_time+
		avg_backend_processing_time+
		avg_request_processing_time+
		timeperurl+
		avgdeltatimes+
		total_received_bytes+
		total_sent_bytes+
		ssl_protocol)
	if formapper:
		return ret.values()
	return ret.keys() 

if __name__ =="__main__":
	dataHome = "/home/sanjay/Kidagiri/algorithm_ds/paytm/WeblogChallenge/data/"
	dataPath = dataHome + "*.log"
	sc = SparkContext('local', 'pyspark')
	sqlContext = SQLContext(sc)
	header = [
		"timestamp","elb","client:port","backend:port","request_processing_time",
		"backend_processing_time","response_processing_time","elb_status_code",
		"backend_status_code","received_bytes","sent_bytes","request","user_agent",
		"ssl_cipher","ssl_protocol"
	]

	headerbc=sc.broadcast(header)
	rawRDD = sc.textFile(dataPath).distinct().map(parser).cache()
	#MIN date  : datetime.datetime(2015, 7, 22, 2, 40, 6, 499174)
	#MAX date  : datetime.datetime(2015, 7, 22, 21, 10, 27, 993803)

	ipGroupedAnalysisRDD = rawRDD.map(
		lambda x: (x['client:port'].split(':')[0] , [x])
		).reduceByKey(
		lambda a,b : a + b
		).map(
		lambda x: (x[0],sorted(x[1],key=lambda k: k['timestamp']))
		).cache()

	'''
	The ipGroupedAnalysisRDD is essentially rawRDD (data provided) grouped 
	at an IP level, with the objects in individual groups arranged in order
	of increasing timestamp.
	Time deltas between all immediate requests made by each IP are calculated and
	using the getDeltas method shown below.
	'''

	# calculate time duration between individual time stamps
	# number of records are small hence can be collected without sampling.
	interEventTime = ipGroupedAnalysisRDD.flatMap(
		lambda x: getDeltas(x[1])
		).collect()
	#print np.percentile(interEventTime,95)

	'''
	95%  of inter event deltas occur within 1.001 min
	beyond that the distribution reduces about 0 around 35min beyond which it 
	increases and decreases in cycles. The histogram plotted below gives a 
	better idea about the same. 
	We plot the histogram of all values greater that 1  and less that 536 
	(99.9 percentile)to get a closer view of the pattern. This could be attributed 
	to users coming back to websites when they get free at lunch dinner afyer work hours etc.
	which might exhibit some seasonality. 
	'''

	# Plot the histogram of individual deltas
	filteredTimes = np.array(interEventTime)
	filteredTimes = filteredTimes[(filteredTimes > 1) & (filteredTimes < 536)]
	hist, bins = np.histogram(filteredTimes, bins=100)
	width = 0.7 * (bins[1] - bins[0])
	center = (bins[:-1] + bins[1:]) / 2
	plt.figure(0)
	plt.bar(center, hist, align='center', width=width)
	plt.title("Inter Request Time")
	plt.xlabel("Time in min between consec reqs")
	plt.ylabel("Frequency")
	#plt.show()
	plt.savefig(dataHome+"inter_even_time.png")

	'''
	We see that the distribution dips to 0 first at around window = 35min 
	We can take this time as the min time window between sessions 
	Next sessionize the grouped records based on time window threshold we have derived.

	**** ANSWER 1

	'''

	sessionAnalysisRDD = ipGroupedAnalysisRDD.flatMap(
		lambda x: splitOnSessionDelta(x,threshold=35)
		).cache()

	avgSessionTime = sessionAnalysisRDD.map(getSessionTime).reduce(lambda a,b : a+b)
	avgSessionTime = avgSessionTime/(sessionAnalysisRDD.count())
	# Average Session Time is 169.55 secs ~ 2.82 min
	sessionTimes = np.array(sessionAnalysisRDD.map(getSessionTime).collect())
	sessionTimes = sessionTimes[(sessionTimes < 2000)]
	hist, bins = np.histogram(sessionTimes, bins=100)
	width = 0.7 * (bins[1] - bins[0])
	center = (bins[:-1] + bins[1:]) / 2
	plt.figure(1)
	plt.bar(center, hist, align='center', width=width)
	plt.title("Session Time Distribution")
	plt.xlabel("Session Time")
	plt.ylabel("Frequency")
	#plt.show()
	plt.savefig(dataHome+"session_time.png")

	'''
	**** ANSWER 2

	The average session time is about 3 min . The max session time however goes upto 54 min.
	The distribution clearly shows a knee at around 350-400 sec. That roughly is around 
	the 90 percentile mark. The top 10%  shows different behavior as compared to the bottom 90%

	We compare these two groups over the distribution on the following features to 
	get better insight.

	"elb","client_port","backend_port","avg_request_processing_time",
	"avg_backend_processing_time","avg_response_processing_time","elb_status_code",
	"backend_status_code","received_bytes","sent_bytes","request","user_agent",
	"ssl_protocol"
	'''


	user_agent=rawRDD.map(lambda x: (x['user_agent'],1)).reduceByKey(lambda a,b:a+b).collect()
	user_agent = sorted(user_agent,key=lambda x: x[1])
	# 64.11% of user agents in top 40 user agents - Can do analysis on aggregate distribution of both
	host=rawRDD.map(lambda x: (getPath(x['request']),1)).reduceByKey(lambda a,b:a+b).collect()
	host = sorted(host,key=lambda x: -x[1])[0:6] 
	# 93.6% in top 6 paths [u'/favicon', u'/api', u'/', u'/papi', u'/offer', u'/shop']
	sslprot=rawRDD.map(lambda x: (x['ssl_protocol'],1)).reduceByKey(lambda a,b:a+b).collect()
	sslprot = sorted(sslprot,key=lambda x : x[1])
	# [u'TLSv1.1', u'TLSv1', u'TLSv1.2', u'-']
	#backstatus=rawRDD.map(lambda x: (x['backend_status_code'][0],1)).reduceByKey(lambda a,b:a+b).collect()
	#backstatus=sorted(backstatus,key=lambda x:x[1])
	# [0,2,3,4,5] for [0,2xx,3xx,4xx,5xx]
	elbstatus=rawRDD.map(lambda x: (x['elb_status_code'][0],1)).reduceByKey(lambda a,b:a+b).collect()
	elbstatus=sorted(elbstatus,key=lambda x: x[1])
	# [2,3,4,5] for [2xx,3xx,4xx,5xx]
	bip=rawRDD.filter(lambda x: x['backend:port'] != '-').map(lambda x: (x['backend:port'].split(":")[0],1)).reduceByKey(lambda a,b:a+b).collect()
	# ['-',u'10.0.4.176', u'10.0.4.217', u'10.0.6.99', u'10.0.6.158', u'10.0.4.227', u'10.0.4.150', u'10.0.6.195', u'10.0.4.225', u'10.0.6.199', u'10.0.6.108', u'10.0.6.178', u'10.0.4.244']
	#cport = rawRDD.map(lambda x: (x['client:port'].split(":")[1],1)).reduceByKey(lambda a,b:a+b).collect()
	# large number of ports and hence unimportant
	# elb has just one distinct value hence not considering 


	'''
	Create KPIs for each session and split into High and Low session time users
	Num unique urls per session stored as numurls

	**** ANSWER 3

	**** ANSWER 4

	'''

	bipbc = sc.broadcast([i[0] for i in bip])
	elbstatusbc = sc.broadcast([i[0] for i in elbstatus])
	#backstatusbc = sc.broadcast([i[0] for i in backstatus])
	sslprotbc = sc.broadcast([i[0] for i in sslprot])
	hostbc = sc.broadcast([i[0] for i in host])


	KPIheader = createKPIs(sessionAnalysisRDD.take(1)[0],formapper=False)
	KPIs = sessionAnalysisRDD.map(createKPIs).toDF(KPIheader).cache()
	LowSessionTimeUsers = KPIs.filter(KPIs['sessiontime']  < 350)
	HighSessionTimeUsers = KPIs.filter(KPIs['sessiontime']  >= 350)
	HighSessionTimeUsers.describe(['numurls','sessiontime','timeperurl','avgdeltatimes']).show()

	'''
	High
	+-------+------------------+------------------+-------------------+--------------------+
	|summary|           numurls|       sessiontime|         timeperurl|       avgdeltatimes|
	+-------+------------------+------------------+-------------------+--------------------+
	|  count|             11056|             11056|              11056|               11056|
	|   mean|24.689308972503618| 1193.273642873914| 178.76191181748038|  1.9200477214512648|
	| stddev|183.97465308680384|495.03339121517826| 237.25843082648925|  2.1419546667072855|
	|    min|                 1|        350.297417|0.21707600020981957|0.002556360484451383|
	|    max|              9532|3248.3628379999996| 2054.3041430000003|  16.886909966666668|
	+-------+------------------+------------------+-------------------+--------------------+

	Low
	+-------+-----------------+------------------+------------------+--------------------+
	|summary|          numurls|       sessiontime|        timeperurl|       avgdeltatimes|
	+-------+-----------------+------------------+------------------+--------------------+
	|  count|            94411|             94411|             94411|               73903|
	|   mean|6.795945387719652| 49.67333098615628| 9.903288623269466| 0.15531788225352902|
	| stddev|32.43724549864875|  70.2555088065015|19.313007502439344|  0.2080157401103004|
	|    min|                1|               0.0|               0.0|3.611111111111110...|
	|    max|             4656|349.41524999999996|        341.397474|  2.9111481416666667|
	+-------+-----------------+------------------+------------------+--------------------+

	This shows that number of unique urls visited by high session time sessions is much higher as compared to 
	low session time sessions. The overall session time is also much higher. The high session time users spend 
	upto 3 min on an average per URL as compared to 9.9 sec of low session time users.

	High
	+-------+-----------------------------------+-----------------------------------+------------------------------------+
	|summary|avg_backend_processing_time_per_url|avg_request_processing_time_per_url|avg_response_processing_time_per_url|
	+-------+-----------------------------------+-----------------------------------+------------------------------------+
	|  count|                              11056|                              11056|                               11056|
	|   mean|                0.07007709749211238|                4.15779955269676E-5|                3.754644402347150...|
	| stddev|                0.41736295167487103|               1.111270480550150...|                1.083421961768697...|
	|    min|                            2.12E-4|                            1.15E-5|                             1.05E-5|
	|    max|                 20.156927999999997|               0.004716666666666666|                0.005106666666666665|
	+-------+-----------------------------------+-----------------------------------+------------------------------------+

	Low
	+-------+-----------------------------------+-----------------------------------+------------------------------------+
	|summary|avg_backend_processing_time_per_url|avg_request_processing_time_per_url|avg_response_processing_time_per_url|
	+-------+-----------------------------------+-----------------------------------+------------------------------------+
	|  count|                              94401|                              94401|                               94401|
	|   mean|                0.03889739415694287|                2.98272162518232E-5|                2.692415274110822E-5|
	| stddev|                0.32590716940442155|               2.225273014698909...|                2.077891436998339...|
	|    min|                             1.8E-4|                             1.1E-5|                             1.05E-5|
	|    max|                  34.82180686666666|               0.043637000000000474|                 0.04032500000000015|
	+-------+-----------------------------------+-----------------------------------+------------------------------------+

	The backend processing time of the high session rime uses is also higher as compared to low session time users.



	High 
	+-------+----------------------------+------------------------+
	|summary|total_received_bytes_per_url|total_sent_bytes_per_url|
	+-------+----------------------------+------------------------+
	|  count|                       11056|                   11056|
	|   mean|           78.36780335547522|      11444.656996503454|
	| stddev|           360.7390142263224|      168071.63473732624|
	|    min|                         0.0|                     0.0|
	|    max|          22799.454545454544|     1.200568742857143E7|
	+-------+----------------------------+------------------------+


	Low
	+-------+----------------------------+------------------------+
	|summary|total_received_bytes_per_url|total_sent_bytes_per_url|
	+-------+----------------------------+------------------------+
	|  count|                       94411|                   94411|
	|   mean|           51.85252839758609|      6922.6385375643285|
	| stddev|          392.05358099147634|      137960.48876532092|
	|    min|                         0.0|                     0.0|
	|    max|                     32413.8|              2.689372E7|
	+-------+----------------------------+------------------------+

	The total data sent by low session time users and high session time users 
	per url is also very skewed. This indicates that High session time session may be POST request heavy.


	High
	+-------+------------------+------------------+------------------+------------------+-----------------+------------------+
	|summary|         request_/|      request_/api|  request_/favicon|    request_/offer|    request_/papi|     request_/shop|
	+-------+------------------+------------------+------------------+------------------+-----------------+------------------+
	|  count|             11056|             11056|             11056|             11056|            11056|             11056|
	|   mean|1.5596056439942112|0.4107272069464544|0.2828328509406657|6.2950434153400865|9.865683791606367|14.140828509406656|
	| stddev|  3.84673256334449|1.0656381186984494|0.9887005975325149|23.616626636232027|172.0388083080011|112.82251834167688|
	|    min|                 0|                 0|                 0|                 0|                0|                 0|
	|    max|               172|                17|                36|               660|            13431|              5568|
	+-------+------------------+------------------+------------------+------------------+-----------------+------------------+

	Low
	+-------+------------------+------------------+-------------------+------------------+------------------+------------------+
	|summary|         request_/|      request_/api|   request_/favicon|    request_/offer|     request_/papi|     request_/shop|
	+-------+------------------+------------------+-------------------+------------------+------------------+------------------+
	|  count|             94411|             94411|              94411|             94411|             94411|             94411|
	|   mean|0.4631981442840347|0.1533084068593702|0.15645422673205453|2.2319221277181684|1.5186683755070913|3.1580959845780683|
	| stddev|1.6211414455262967|0.5326438795047537| 0.4706176050315895| 12.71606852864698|26.145092668387658|15.904051847109157|
	|    min|                 0|                 0|                  0|                 0|                 0|                 0|
	|    max|               393|                21|                 46|               567|              5114|              2083|
	+-------+------------------+------------------+-------------------+------------------+------------------+------------------+

	The proportion of papi calls seem significantly higher in higher sessiontime sessions as compared 
	to lower session time sessions. This is further made evident by the frequecy count described below

	LowSessionUsersSiteDetails =LowSessionTimeUsers.map(lambda x: (
		1 if x['request_/'] > 0 else 0 ,
		1 if x['request_/api'] > 0 else 0 ,
		1 if x['request_/favicon'] > 0 else 0 ,
		1 if x['request_/offer'] > 0 else 0 ,
		1 if x['request_/papi'] > 0 else 0 ,
		1 if x['request_/shop'] > 0 else 0 
		)).toDF(
		['request_/','request_/api','request_/favicon','request_/offer','request_/papi','request_/shop']
		).groupBy(['request_/offer','request_/papi','request_/shop']).count().orderBy("count",ascending=0)


	HighSessionUsersSiteDetails =HighSessionTimeUsers.map(lambda x: (
		1 if x['request_/'] > 0 else 0 ,
		1 if x['request_/api'] > 0 else 0 ,
		1 if x['request_/favicon'] > 0 else 0 ,
		1 if x['request_/offer'] > 0 else 0 ,
		1 if x['request_/papi'] > 0 else 0 ,
		1 if x['request_/shop'] > 0 else 0 
		)).toDF(
		['request_/','request_/api','request_/favicon','request_/offer','request_/papi','request_/shop']
		).groupBy(['request_/offer','request_/papi','request_/shop']).count().orderBy("count",ascending=0)


	High
		+--------------+-------------+-------------+-----+
		|request_/offer|request_/papi|request_/shop|count|
		+--------------+-------------+-------------+-----+
		|             0|            1|            1| 5908|
		|             0|            0|            1| 2346|
		|             0|            0|            0| 1121|
		|             1|            1|            1|  743|
		|             0|            1|            0|  357|
		|             1|            0|            1|  353|
		|             1|            0|            0|  213|
		|             1|            1|            0|   15|
		+--------------+-------------+-------------+-----+


	Low 

		+--------------+-------------+-------------+-----+
		|request_/offer|request_/papi|request_/shop|count|
		+--------------+-------------+-------------+-----+
		|             0|            0|            1|46505|
		|             0|            1|            1|22692|
		|             0|            0|            0|13396|
		|             0|            1|            0| 6445|
		|             1|            0|            0| 2676|
		|             1|            0|            1| 1768|
		|             1|            1|            1|  860|
		|             1|            1|            0|   69|
		+--------------+-------------+-------------+-----+



	High
	+-------+------------------+------------------+------------------+--------------------+
	|summary| elb_status_code_2| elb_status_code_3| elb_status_code_4|   elb_status_code_5|
	+-------+------------------+------------------+------------------+--------------------+
	|  count|             11056|             11056|             11056|               11056|
	|   mean|29.544138929088277| 4.217076700434154|0.5813133140376266|0.019627351664254705|
	| stddev|231.14216756062183|12.886531263576801| 9.856431637520654|  0.3637511070841117|
	|    min|                 0|                 0|                 0|                   0|
	|    max|             13431|               653|               548|                  23|
	+-------+------------------+------------------+------------------+--------------------+


	Low
	+-------+-----------------+------------------+-------------------+--------------------+
	|summary|elb_status_code_2| elb_status_code_3|  elb_status_code_4|   elb_status_code_5|
	+-------+-----------------+------------------+-------------------+--------------------+
	|  count|            94411|             94411|              94411|               94411|
	|   mean|  7.0057514484541|1.1241274851447396|0.11351431506921864|0.003442395483577...|
	| stddev|35.78793946769989|  5.47454881969022|  5.717455407437703| 0.17282493765972426|
	|    min|                0|                 0|                  0|                   0|
	|    max|             4657|               462|               1564|                  36|
	+-------+-----------------+------------------+-------------------+--------------------+


	# There seem to be slightly higher 4xx / 5xx errors amongst high sessiontime users as 
	compared to low session time users

	'''
