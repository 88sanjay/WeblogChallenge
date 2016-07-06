import org.apache.spark.{SparkContext,SparkConf}
import scala.collection.mutable.{Map => MutableMap}

object Spark {
  val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local[*]"))
}

object LogParser extends java.io.Serializable {
    
    val header =List[String](
    "timestamp","elb","client:port","backend:port","request_processing_time",
    "backend_processing_time","response_processing_time","elb_status_code",
    "backend_status_code","received_bytes","sent_bytes","request","user_agent",
    "ssl_cipher","ssl_protocol"
    )
    
    val timeFormat = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
    val threshold = 35*60    
    def parser(x:String) : Map[String,String] = {
        val tempVar = x.split(" \"|\" ")
        // Split the String on "\"" to get 
        val retValues = tempVar(0).split(" ") ++ tempVar.slice(1,3) ++ tempVar(3).split(" ")
        val ret = (this.header zip retValues).toMap
        return ret
    }
    
    def getDeltas(x:Array[Map[String,String]]) : Array[Double] = {
        var out = Array[Double]()
        for (id <- 0 to x.length-2) {
            out = out :+ (this.timeFormat.parse(x(id + 1)("timestamp")).getTime() 
                - this.timeFormat.parse(x(id)("timestamp")).getTime())/1000.0
        }
        return out
    }
    
    def splitOnSessionDelta(ip:String,groupedMaps:Array[Map[String,String]]) : 
    List[(String,Array[Map[String,String]])]= {
        val deltas = this.getDeltas(groupedMaps)
        var idToSplit= Array[Int]()
        for((d,i) <- deltas.zipWithIndex if d >= this.threshold)
            idToSplit = idToSplit :+ i
        var out = List[(String,Array[Map[String,String]])]()
        if (idToSplit.length > 0) {
            var start = 0
            var end = 0
            for (i <- idToSplit){
                end = i+1
                out = out :+ (ip,groupedMaps.slice(start,end))
                start = i+1
            }
            out = out :+ (ip,groupedMaps.slice(idToSplit.last+1,groupedMaps.length))
        } else{ 
            out = List((ip,groupedMaps))
        }
        return out
    }
}

object FeatureExtraction {

    def getPath(url:String): String = {
        var pattern = """\.[a-zA-Z0-9]+:?[0-9]*(\/[a-zA-Z0-9]+)\/""".r
        var elems = pattern.findFirstMatchIn(url)
        if (elems == None ) {
            pattern = """\.[a-zA-Z0-9]+:?[0-9]*(\/[a-zA-Z0-9]*)""".r
            elems = pattern.findFirstInm.(url)
        }
        return (if (elems !=  None) elems.get.group(1) else "/")
    }

// '''
// getSessionTime -  method to get time duration of a particular session
// '''
    // def getSessionTime(x):
    //     ret = 0
    //     if len(x[1]) > 1:
    //         delta = x[1][-1]["timestamp"] - x[1][0]["timestamp"]
    //         ret = delta.total_seconds()
    //     return (ret + max(0.0,x[1][-1]["request_processing_time"]) + \
    //     max(0.0,x[1][-1]["backend_processing_time"]) + \
    //     max(0.0,x[1][-1]["response_processing_time"]))

// '''
// countKPIs  - method to count number of occurances of each element of y 
//              in x
// '''

// def countKPIs(x,y,name):
//     ret = []
//     for i in y:
//         ret = ret+[(name+"_"+i.replace('.','*'),x.count(i))]
//     return ret

// '''
// createKPIs  - method to create all features
// '''

// def createKPIs(x,formapper=True):
//     # Session time
//     sessiontime = [("sessiontime",getSessionTime(x))]
//     # Unique URLs
//     numurls = [("numurls",len(set([i['request'] for i in x[1]])))]
//     # Time per url
//     timeperurl = [("timeperurl",sessiontime[0][1]/numurls[0][1])]
//     avgdeltatimes = [("avgdeltatimes",sum(getDeltas(x[1]))/len(x[1]))]
//     # backend ip
//     backendips = countKPIs([i["backend:port"].split(":")[0] for i in x[1]],
//         bipbc.value,"backendip")
//     avg_request_processing_time = [("avg_request_processing_time_per_url",
//         sum([max(0,i['request_processing_time']) for i in x[1]])/numurls[0][1])]
//     avg_backend_processing_time = [("avg_backend_processing_time_per_url",
//         sum([max(0,i['backend_processing_time']) for i in x[1]])/numurls[0][1])]
//     avg_response_processing_time = [("avg_response_processing_time_per_url",
//         sum([max(0,i['response_processing_time']) for i in x[1]])/numurls[0][1])]
//     elbstatuscode = countKPIs([i["elb_status_code"][0] for i in x[1]],
//         elbstatusbc.value,"elb_status_code")    
//     total_received_bytes = [
//     ("total_received_bytes_per_url",
//         sum([float(i['received_bytes']) for i in x[1]])/numurls[0][1])
//     ]
//     total_sent_bytes = [
//     ("total_sent_bytes_per_url",
//         sum([float(i['sent_bytes']) for i in x[1]])/numurls[0][1])
//     ]
//     request = countKPIs([getPath(i["request"]) for i in x[1]],
//         hostbc.value,"request")
//     # user_agent
//     ssl_protocol = countKPIs([i["ssl_protocol"] for i in x[1]],sslprotbc.value,"sslport")
//     ret = dict(sessiontime+
//         numurls+
//         request+
//         backendips+
//         elbstatuscode+
//         avg_response_processing_time+
//         avg_backend_processing_time+
//         avg_request_processing_time+
//         timeperurl+
//         avgdeltatimes+
//         total_received_bytes+
//         total_sent_bytes+
//         ssl_protocol)
//     if formapper:
//         return ret.values()
//     return ret.keys() 

}

object analysis {
    def main(args: Array[String]) {
        val dataHome = "/home/sanjay/Kidagiri/algorithm_ds/paytm/WeblogChallenge/data/"
        val dataPath = dataHome + "*.log"
        val rawRDD = sc.textFile(dataPath).map(LogParser.parser).cache()
        val ipGroupedAnalysisRDD = rawRDD.map(
            x => {(x("client:port").split(':')(0) , List(x))}
            ).reduceByKey(
             _ ::: _
            ).map(
                x => {
                    (
                        x._1,
                        scala.util.Sorting.stableSort(x._2,
                        (e1: Map[String,String], e2: Map[String,String]) => 
                        e1("timestamp") < e2("timestamp"))
                    )
                }
            )
        val sessionAnalysisRDD = ipGroupedAnalysisRDD.flatMap( x =>
            LogParser.splitOnSessionDelta(x._1,x._2)
            )

    }
}