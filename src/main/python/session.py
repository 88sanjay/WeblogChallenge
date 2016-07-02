'''
This file contains methods and class definitions used for  data analysis 
sessions of IPs. An attempt is made  to understand what the min 
inactivity window between two reqs should be to consider them as part of different 
sessions
'''
from pyspark import SparkContext
from pyspark.sql import SQLContext
import re
import numpy as np 


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
	ret['request_processing_time'] =  float(ret['request_processing_time'])
	ret['backend_processing_time'] =  float(ret['backend_processing_time'])
	ret['response_processing_time'] = float(ret['response_processing_time'])
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

def getPath(url):
	elems = re.findall(r'\.[a-zA-Z0-9]+:?[0-9]*(\/[a-zA-Z0-9]+)\/',url)
	if len(elems) == 0 :
		elems = re.findall(r'\.[a-zA-Z0-9]+:?[0-9]*(\/[a-zA-Z0-9]*)',url)
	return (elems[0] if len(elems) > 0 else "/")

def getSessionTime(x):
	ret = 0
	if len(x[1]) > 1:
		delta = x[1][-1]["timestamp"] - x[1][0]["timestamp"]
		ret = delta.total_seconds()
	return (ret + max(0.0,x[1][-1]["request_processing_time"]) + \
	max(0.0,x[1][-1]["backend_processing_time"]) + \
	max(0.0,x[1][-1]["response_processing_time"]))


def createKPIs(x):
	# Session time
	sessiontime = [("sessiontime",getSessionTime(x))]
	# Unique URLs
	numurls = ["numurls":len(set([x[1]['request'][i] for i in range(len(x[1]))]))]
	# backend ip
	backendips = countKPIs(
		[[i]["backend:port"].split(":")[0] for i in x[1],
		bipbc.values,
		"backendip"
		)

	# avg_request_processing_time
	# avg_backend_processing_time
	# avg_response_processing_time
	elbstatuscode = countKPIs(
		[[i]["elb_status_code"][0] for i in x[1],
		elbstatusbc.values,
		"elb_status_code"
		)
	# total received_bytes
	# total sent_bytes
    # request
    # user_agent
    # ssl_protocol
    return sessiontime+numurls+backendips+elbstatuscode


dataHome = "/home/sanjay/Kidagiri/algorithm_ds/paytm/WeblogChallenge/data/"
dataPath = dataHome + "*.log"
sc = SparkContext('local', 'pyspark')
rawRDD = sc.textFile(dataPath).distinct().map(parser).cache()
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

avgSessionTime = sessionAnalysisRDD.map(getSessionTime).reduce(lambda a,b : a+b)
avgSessionTime = avgSessionTime/(sessionAnalysisRDD.count())
# Average Session Time is 169.55 secs ~ 2.82 min
sessionTimes = np.array(sessionAnalysisRDD.map(getSessionTime).collect())
sessionTimes = sessionTimes[(sessionTimes < 2000)]
hist, bins = np.histogram(sessionTimes, bins=100)
width = 0.7 * (bins[1] - bins[0])
center = (bins[:-1] + bins[1:]) / 2
plt.bar(center, hist, align='center', width=width)
plt.title("Session Time Distribution")
plt.xlabel("Session Time")
plt.ylabel("Frequency")
#plt.show()
plt.savefig(dataHome+"session_time.png")


'''
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
host = sorted(host,key=lambda x: -x[1])[0:6] + [('other',0)]
# 93.6% in top 6 paths [u'/favicon', u'/api', u'/', u'/papi', u'/offer', u'/shop', 'other']
sslprot=rawRDD.map(lambda x: (x['ssl_protocol'],1)).reduceByKey(lambda a,b:a+b).collect()
sslprot = sorted(sslprot,key x : x[1])
# [u'TLSv1.1', u'TLSv1', u'TLSv1.2', u'-']

backstatus=rawRDD.map(lambda x: (x['backend_status_code'][0],1)).reduceByKey(lambda a,b:a+b).collect()
backstatus=sorted(backstatus,key=lambda x:x[1])
# [0,2,3,4,5] for [0,2xx,3xx,4xx,5xx]

elbstatus=rawRDD.map(lambda x: (x['elb_status_code'][0],1)).reduceByKey(lambda a,b:a+b).collect()
elbstatus=sorted(elbstatus,key=lambda x: x[1])
# [2,3,4,5] for [2xx,3xx,4xx,5xx]

bip=rawRDD.filter(lambda x: x['backend:port'] != '-').map(lambda x: (x['backend:port'].split(":")[0],1)).reduceByKey(lambda a,b:a+b).collect()
# ['-',u'10.0.4.176', u'10.0.4.217', u'10.0.6.99', u'10.0.6.158', u'10.0.4.227', u'10.0.4.150', u'10.0.6.195', u'10.0.4.225', u'10.0.6.199', u'10.0.6.108', u'10.0.6.178', u'10.0.4.244']

cport = rawRDD.map(lambda x: (x['client:port'].split(":")[1],1)).reduceByKey(lambda a,b:a+b).collect()
# large number of ports and hence unimportant

# elb has just one distinct value hence not considering 


'''
Create KPIs for each session.  

'''

bipbc = sc.broadcast([i[0] for i in bip])
elbstatusbc = sc.broadcast([i[0] for i in elbstatus])
backstatusbc = sc.broadcast([i[0] for i in backstatus])
sslprotbc = sc.broadcast([i[0] for i in sslprot])
hostbc = sc.broadcast([i[0] for i in host])

