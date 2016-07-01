'''
This file contains methods and class definitions used for  data analysis 
sessions of IPs. An attempt is made  to understand what the min 
inactivity window between two reqs should be to consider them as part of different 
sessions
'''
def parser(x):
	# header definition 
	header = [
		"timestamp","elb","client:port","backend:port","request_processing_time",
		"backend_processing_time","response_processing_time","elb_status_code",
		"backend_status_code","received_bytes","sent_bytes","request","user_agent",
		"ssl_cipher","ssl_protocol"
	]
	# remove the data in double quotes first by spliting on double quotes
	tempVar = (re.split(' \"|\" ',x))
	retValues = tempVar[0].split(" ") + tempVar[1:3] + tempVar[3].split(" ")
	ret = dict(zip(header,retValues))
	# convert timestamp to 
	ret['timestamp'] = datetime.strptime(ret['timestamp'],'%Y-%m-%dT%H:%M:%S.%fZ')
	# convert all time duration captured to floating point numbers
	ret['request_processing_time'] = float(ret['request_processing_time'])
	ret['backend_processing_time'] = float(ret['backend_processing_time'])
	ret['response_processing_time'] = float(ret['request_processing_time'])
	return ret

def getDeltas(x):
	out = []
	for i in range(len(x) - 1) :
		elapsedTime = x[i+1]['timestamp'] - x[i]['timestamp']
		out = out + [elapsedTime.total_seconds()/60.0]
	return out

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

def getSessionTime(x):
	ret = 0
	if len(x[1]) > 1:
		delta = x[1][-1]["timestamp"] - x[1][0]["timestamp"]
		ret = delta.total_seconds()
	return (ret + x[1][-1]["request_processing_time"] + \
	x[1][-1]["backend_processing_time"] + \
	x[1][-1]["response_processing_time"])

dataHome = "/home/sanjay/Kidagiri/algorithm_ds/paytm/WeblogChallenge/data/"
dataPath = dataHome + "*.log"
sc = SparkContext('local', 'pyspark')
rawRDD = sc.textFile(dataPath).distinct().map(parser)
#MIN date  : datetime.datetime(2015, 7, 22, 2, 40, 6, 499174)
#MAX date  : datetime.datetime(2015, 7, 22, 21, 10, 27, 993803)
sessionAnalysisRDD = rawRDD.map(
	lambda x: (x['client:port'].split(':')[0] , [x])
	).reduceByKey(
	lambda a,b : a + b
	).map(
	lambda x: (x[0],sorted(x[1],key=lambda k: k['timestamp']))
	).flatMap(
	lambda x: splitOnSessionDelta(x,threshold=35)
	).cache()


#calculate session specific KPIs

sessionAnalysisRDD.map(getSessionTime)

