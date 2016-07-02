'''
This file contains methods and class definitions used for  data analysis 
inter request time deltas of IPs. An attempt is made  to understand what the min 
inactivity window between two reqs should be to consider them as part of different 
sessions
'''

from pyspark import SparkContext
from pyspark.sql import SQLContext
import re
import numpy as np 
import matplotlib.pyplot as plt

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
	ret['request_processing_time'] =  max(0.0,float(ret['request_processing_time']))
	ret['backend_processing_time'] =  max(0.0,float(ret['backend_processing_time']))
	ret['response_processing_time'] = max(0.0,float(ret['response_processing_time']))
	return ret

def getDeltas(x):
	out = []
	for i in range(len(x) - 1) :
		elapsedTime = x[i+1]['timestamp'] - x[i]['timestamp']
		out = out + [elapsedTime.total_seconds()/60.0]
	return out


if __name__=="__main__":
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
		)
	'''
	The sessionAnalysisRDD is essentially rawRDD (data provided) grouped 
	at an IP level, with the objects in individual groups arranged in order
	of increasing timestamp.
	Time deltas between all immediate requests made by each IP are calculated and
	using the getDeltas method shown below.
	'''
	# calculate time duration between individual time stamps
	# number of records are small hence can be collected without sampling.
	interEventTime = sessionAnalysisRDD.flatMap(
		lambda x: getDeltas(x[1])
		).collect()
	#print np.percentile(interEventTime,95)
	'''
	95 % of inter event deltas occur within 1.001 min
	beyond that the distribution reduces about 0 around 35min beyond which it 
	increases and decreases in cycles. The histogram plotted below gives a 
	better idea about the same. 
	We plot the histogram of all values greater that 1  and less that 536 
	(99.9 percentile)to get a closer view of the pattern. This could be attributed 
	to users coming back to websites when they get free at lunch dinner afyer work hours etc.
	which exhibits some seasonality. 
	'''
	# Plot the histogram of individual deltas
	filteredTimes = np.array(interEventTime)
	filteredTimes = filteredTimes[(filteredTimes > 1) & (filteredTimes < 536)]
	hist, bins = np.histogram(filteredTimes, bins=100)
	width = 0.7 * (bins[1] - bins[0])
	center = (bins[:-1] + bins[1:]) / 2
	plt.bar(center, hist, align='center', width=width)
	plt.title("Inter Request Time")
	plt.xlabel("Time in min between consec reqs")
	plt.ylabel("Frequency")
	#plt.show()
	plt.savefig(dataHome+"inter_even_time.png")
	'''
	***
	We see that the distribution dips to 0 first at around window = 35min 
	We can take this time as the min time window between sessions 
	'''
