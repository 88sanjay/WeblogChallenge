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

